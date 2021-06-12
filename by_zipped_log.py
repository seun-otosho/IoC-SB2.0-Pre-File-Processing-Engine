import pandas as pd
import progressbar
import re
import zipfile

from sqlalchemy import create_engine
from datetime import datetime
from multiprocessing.pool import ThreadPool
from os import listdir, path, sep, sys
from os.path import isfile, join
from pymongo import MongoClient
from shutil import move

inc_pth = path.dirname(path.dirname(path.abspath(__file__)))
# inc_pth = path.dirname(path.abspath(__file__))
print(inc_pth)
sys.path.append(inc_pth)
# sys.path.append(path.dirname(inc_pth))

# print(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))

from IoCEngine.commons import count_down, mk_dir, std_out
from IoCEngine.logger import get_logger
from IoCEngine.utils.file import SB2Exceptions
from the_process import IoCstr, nlv

dir2watch = drop_zone = inc_pth + sep + 'SB2Logs' + sep
mdjlog = get_logger('analyzeZipp3DLogs')

client = MongoClient()

db = client.IoC
collecxn = db.IoC_SB2Exception_logs
sb_file_view = db.IoC_SB2FileInfo
sb2file_errorlog = db.IoC_SB2ErrorSummary
sb2field_errorlog = db.IoC_SB2FileFieldErrors

ioc_tns = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.1.16)(PORT = 1521)) '
ioc_tns += '(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = crcpdb01) ) )'
ioc_con = create_engine("oracle+cx_oracle://ioc:IoC@" + ioc_tns)

probar = progressbar.ProgressBar()

'''
select min(file_id) min_file_id from sb_file_view where date_check >= 20110610120000
'''


def pull_in_sb_file_info():
    df = pd.read_sql('''select * from sb_file_view where date_check >= 20110610120000''', ioc_con)
    sb_file_view.insert_many(df.to_dict('records'))


def getSB2file_info(dpid, y4m2):
    dp_file_info_q = '''
    SELECT
        submxn, to_char(date_reported, 'Mon-YYYY') date_processed,
        SUM(nvl(total, 0))      sum_total,
        SUM(nvl(invalids, 0))   sum_invalids,
        SUM(nvl(rejected, 0))   sum_rejected,
        SUM(nvl(moved, 0))  sum_moved
    FROM     sb_file_view
    WHERE    dpid = '{}'
    and      yyyymm = {}
    GROUP BY submxn, to_char(date_reported, 'Mon-YYYY')
    '''.format(dpid, y4m2)
    df = pd.read_sql(dp_file_info_q, ioc_con)
    mdjlog.info(dp_file_info_q)
    return df  # .to_dict('records')[0]


def parse_zip_log(zip_log):
    try:
        zip_ref = zipfile.ZipFile(zip_log, 'r')
        file = zip_ref.namelist()[0]
        file_id, stage = file.split('_')[0], file.split('_')[1]
        ziprefls = zip_ref.open(file).readlines()
        category, dp_name, fid, fn, process_date = hdr_dtls(ziprefls)
        dtls_df = pd.read_csv(zip_ref.open(file), dtype=str, sep='|', header=6, skipinitialspace=True, skiprows=[7])
        dtls_df.fillna('', inplace=True)
        # , encoding="ISO-8859-1"
        if not dtls_df.empty:
            mdjlog.info(dtls_df.shape)
            dtls_df.columns = dtls_df.columns.str.strip()

            mdjlog.info('Will now write data from {} to DB. Thank you!'.format(file))
            dtls_df['S.NO'] = dtls_df['S.NO'].apply(lambda x: int(x) if str(x).strip().isdigit() else None)
            dtls_df.rename(columns={
                'S.NO': 'S_NO', 'CREDIT FACILITY ACCOUNT NUMBER': 'account_no', 'DATA PROVIDER BRANCH ID': 'branch_code'
            }, inplace=True)
            del dtls_df['S_NO']
            dtls_df.columns = [col.strip().replace(' ', '_').lower() for col in dtls_df.columns]
            dtls_df_obj = dtls_df.select_dtypes(['object'])
            dtls_df[dtls_df_obj.columns] = dtls_df_obj.apply(lambda x: x.str.strip())
            dtls_df.dropna(inplace=True)
            try:
                if not dtls_df.empty:
                    # imp_pool = ThreadPool()
                    dtls_df['file_name'], dtls_df['dp_name'], dtls_df['category'] = fn, dp_name.strip(), category
                    dpid, dtls_df['process_date'], dtls_df['stage'] = fn.strip().split('-')[0], process_date, stage
                    dtls_df['dpid'], dtls_df['yyyymm'] = dpid, int(process_date.strftime('%Y%m'))
                    collecxn.insert_many(dtls_df.to_dict('records'))
                    # for d in collect_error(dtls_df):
                    #     try:
                    #         SB2Exceptions(d).save()
                    #     except Exception as e:
                    #         pass  # mdjlog.error(e)

                    mdjlog.info("Data written to DB!. Thank you again.")
                    del dtls_df
            except Exception as e:
                mdjlog.error(e)
        else:
            mdjlog.info("no error data logged")
    except Exception as e:
        mdjlog.error(e)
    # next_step(asdf, dp_name, fn)
    # dpname, dpid, yyyymm, file_id, fn, date_process
    return dp_name.strip(), dpid, file_id, fn, process_date  # dtls_df,


# def collect_error(dl):
#     for i, d in dl.iterrows():
#         yield d.to_dict()


def hdr_dtls(ziprefls):
    try:
        process_date = None
        for i, line in enumerate(ziprefls):
            line = line.decode()
            if i == 3:
                fn = line.split('/')[2].split(' ')[0]
                fid = line.split('/')[1].split(':')[1]
                dp_name = line[line.find('Institution') + 17:].strip()
                if process_date is None:
                    process_date = line[line.find('-') + 1:].strip()
                    process_date = process_date[process_date.find('-') + 1:].strip()
                    process_date = process_date[:11]
                mdjlog.info('\n\n{}\t{}\t\t{}'.format(dp_name.strip(), fid, fn))
            if i == 4:
                category = ' '.join(line.split()[:2])
                category = category[:-4] if 'Run' in category else category
                try:
                    # process_date = datetime.strptime(line[line.find('Date') + 5:].strip(), '%m/%d/%Y %I:%M:%S %p')
                    process_date = datetime.strptime(process_date, '%d-%b-%Y')

                    mdjlog.info('{} {}'.format(process_date, category))
                except Exception as e:
                    mdjlog.error(e)
                return category, dp_name, fid, fn, process_date
    except Exception as e:
        mdjlog.error(e)


def watch_dir(dir2watch, zipp3DD):
    i = 1
    try:
        rtpl = zip_log_pro(zipp3DD, dir2watch, i)
        dpname, zipp3DD, file_id = rtpl[0], rtpl[1], rtpl[2]
        for dpid_d in zipp3DD:
            for y4m2 in zipp3DD[dpid_d]['y4m2s']:
                y4m2 = int(y4m2)
                report_selected_fields(zipp3DD[dpid_d]['dpname'], zipp3DD[dpid_d]['dpid'], y4m2, file_id)
    except Exception as e:
        mdjlog.error(e)
    count_down(dir2watch)


def zip2insttn_dir(insttn_name, yyyymm, zip_log_path):
    dir, file = path.split(zip_log_path)
    dir_dst = join(dir, insttn_name, yyyymm)
    if not path.exists(dir_dst):
        mk_dir(dir_dst)
    new_dst = join(dir_dst, file)
    move(zip_log_path, new_dst)
    mdjlog.info('{}\nmoved to \n{}'.format(zip_log_path, new_dst))


def zip_log_pro(zipp3DD, dir2watch, i):
    for zip_log, c in probar(watch(dir2watch)):
        zip_log = join(dir2watch, zip_log)
        if isfile(zip_log):
            try:
                dpname, dpid, file_id, fn, date_process = parse_zip_log(zip_log)
                yyyymm = date_process.strftime("%Y%m")
                zip2insttn_dir(dpname.strip(), yyyymm, zip_log)

                if dpid not in zipp3DD:
                    zipp3DD[dpid] = {}
                    zipp3DD[dpid]['dpid'] = dpid
                    zipp3DD[dpid]['dpname'] = dpname
                    zipp3DD[dpid]['y4m2s'] = [yyyymm]
                else:
                    zipp3DD[dpid]['y4m2s'] = list(set(zipp3DD[dpid]['y4m2s'] + [yyyymm]))

            except Exception as e:
                mdjlog.error(e)
        mdjlog.info('i is {} of {}'.format(i, c))
        if i == c:
            return dpname, zipp3DD, file_id

        i += 1


def report_all_fields(all_stages_df, dpname, fn):
    print(all_stages_df.shape)
    insttxn_smmry = getSB2file_info(fn)
    mdjlog.info(insttxn_smmry)
    report_writer = pd.ExcelWriter(
        join(''.join((dir2watch, '{} {} Submission Report.xlsx'.format(dpname.strip(), insttxn_smmry['yyyymm'][0])))))
    # field_names =
    insttxn_smmry.to_excel(report_writer, sheet_name='Institution Summary', index=False)
    #
    fields_errors = all_stages_df[all_stages_df['field_name'] != '']
    fields_errors = fields_errors[['account_no', 'field_name', 'error_description']]
    fields_errors.drop_duplicates(inplace=True)
    fieldname_group = fields_errors.groupby(['field_name', 'error_description'])['account_no'].count()
    fields_errors_group = fieldname_group.to_frame().reset_index()
    fields_errors_group.rename(columns={
        'error_description': 'Types of Errors', 'account_no': 'Affected Accounts', 'field_name': 'Affected Fields'
    }, inplace=True)
    if not fields_errors_group.empty:
        fields_errors_group.to_excel(report_writer, sheet_name='All Errors', index=False)
    #
    for fieldname in all_stages_df.field_name.unique():
        if fieldname != '':
            fieldDF = all_stages_df[all_stages_df['field_name'] == fieldname]
            if not fieldDF.empty:
                fieldDF[
                    ['category', 'branch_code', 'account_no', 'field_value', 'error_description', 'severity', ]
                ].to_excel(report_writer, sheet_name=fieldname, index=False)
    #
    uncat = all_stages_df.loc[(all_stages_df['field_name'] == '') & (all_stages_df['error_description'] != '')]
    if not uncat.empty:
        uncat[
            ['category', 'branch_code', 'account_no', 'error_description', 'severity', ]
        ].to_excel(report_writer, sheet_name='Uncategorized', index=False)
    #
    report_writer.save()
    mdjlog.info('analyzes complete. Thank you! Again!')


def report_selected_fields(dpname, dpid, yyyymm, file_id):
    all_stages_csr = collecxn.find({"dpid": dpid, "yyyymm": int(yyyymm)})
    all_stages_df = pd.DataFrame(list(all_stages_csr))
    monyyyy, y4m2s = None, list(all_stages_df.yyyymm.unique())
    if len(y4m2s) > 1:
        for y4m2 in y4m2s:
            insttxn_smmry = getSB2file_info(dpid, int(y4m2))
            mdjlog.info(insttxn_smmry)
            if insttxn_smmry is not None and not insttxn_smmry.empty:
                summarize_logs(all_stages_df, dpid, dpname, file_id, insttxn_smmry)
    else:
        y4m2 = int(y4m2s[0])
        insttxn_smmry = getSB2file_info(dpid, y4m2)
    mdjlog.info('\n{}'.format(insttxn_smmry))
    if insttxn_smmry is not None and not insttxn_smmry.empty:
        summarize_logs(all_stages_df, dpid, dpname, file_id, insttxn_smmry)
    else:
        mdjlog.warn('*** NO DATA ERROR FOUND IN THE FILE ***')


def summarize_logs(all_stages_df, dpid, dpname, file_id, insttxn_smmry):
    try:
        insttxn_smmry['dpid'], insttxn_smmry['dpname'], insttxn_smmry['file_id'] = dpid, dpname, file_id
        monyyyy = insttxn_smmry['date_processed'][0]
        for insummry in insttxn_smmry.to_dict('records'):
            try:
                sb2file_errorlog.insert_many(insummry)
            except:
                pass
        report_writer = pd.ExcelWriter(
            join(''.join((dir2watch,
                          '{} {} Data Process Report (Summarized).xlsx'.format(dpname.strip(), monyyyy)))))
        # field_names =
        insttxn_smmry.to_excel(report_writer, sheet_name='Institution Summary', index=False)
        #
        all_stages_df['severity'] = all_stages_df.severity.apply(lambda x: str(x).upper())
        all_stages_df = all_stages_df[all_stages_df['severity'] != 'ALERT']
        # all_stages_df = all_stages_df[all_stages_df['severity'] != 'STRUCTURE']
        #
        uncat = all_stages_df.loc[(all_stages_df['field_name'] == '') & (all_stages_df['error_description'] != '')]
        #
        fields_errors = all_stages_df[all_stages_df['field_name'] != '']
        fields_errors = fields_errors[['account_no', 'field_name', 'severity', 'error_description']]
        fields_errors.drop_duplicates(inplace=True)
        fieldname_group = fields_errors.groupby(['field_name', 'severity', 'error_description'])['account_no'].count()
        #
        if not fieldname_group.empty:
            fields_errors_group = fieldname_group.to_frame().reset_index()
            fields_errors_group['dpid'], fields_errors_group['dpname'], fields_errors_group['date_processed'], \
            fields_errors_group['file_id'] = dpid, dpname, monyyyy, file_id
            sb2field_errorlog.insert_many(fields_errors_group.to_dict('records'))
            fields_errors_group.rename(columns={
                'error_description': 'Types of Errors', 'account_no': 'Affected Accounts',
                'field_name': 'Affected Fields'
            }, inplace=True)
            del fields_errors_group['dpid'], fields_errors_group['dpname'], fields_errors_group['file_id'], \
                fields_errors_group['date_processed']
            fields_errors_group.to_excel(report_writer, sheet_name='All Errors', index=False)
            del fields_errors_group
        #
        for fieldname in all_stages_df.field_name.unique():
            if fieldname != '':
                fieldDF = all_stages_df[all_stages_df['field_name'] == fieldname]
                if not fieldDF.empty:
                    fieldDF[
                        ['category', 'branch_code', 'account_no', 'field_value', 'error_description', 'severity', ]
                    ].to_excel(report_writer, sheet_name=re.sub(' +', ' ', fieldname.replace('/', ''))[:30],
                               index=False)
        #
        if not uncat.empty:
            uncat[
                ['category', 'branch_code', 'account_no', 'error_description', 'severity', ]
            ].to_excel(report_writer, sheet_name='Uncategorized', index=False)
        #
        report_writer.save()
        del all_stages_df, fields_errors, fieldname_group, uncat
        mdjlog.info('analyses completed for {} {} submission. Thank you! Again!'.format(dpname, monyyyy))
    except Exception as e:
        mdjlog.error(e)


def watch(drop_zone):
    before = dict([(f, None) for f in listdir(drop_zone)])
    mdjlog.debug("before: " + ", ".join(before) + nlv)
    while 1:
        after = dict([(f, None) for f in listdir(drop_zone)])
        added = [f for f in after if not f in before]
        removed = [f for f in before if not f in after]
        mdjlog.debug("Removed: " + ", ".join(removed) + nlv)
        if removed:
            std_out("Removed: " + ", ".join(removed), nlv)
        mdjlog.debug("Added: " + ", ".join(removed) + nlv)
        if added:
            std_out("Added: {} files ".format(len(added)) + ", ".join(added), nlv)
            for file in added:
                # count_down()
                yield file, len(added)
                # count_down()
        before = after

        count_down(drop_zone)


if __name__ == '__main__':
    mdjlog.info(IoCstr)
    mk_dir(dir2watch)
    while True:
        zipp3DD = {}
        watch_dir(dir2watch, zipp3DD)
