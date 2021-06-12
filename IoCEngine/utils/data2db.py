# import motor
import csv
import json
import os
import subprocess

from bson import json_util
from concurrent import futures
from numbers import Real
from os import remove
from os.path import abspath

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
# from tornado import gen

from IoCEngine.celeryio import app
from IoCEngine.commons import count_down, getID
from IoCEngine.cores import ppls, ppns
from IoCEngine.logger import get_logger
from IoCEngine.utils.data_modes import *
from IoCEngine.utils.file import (dbhs, pym_db, CorporateSubmissions, FacilitySubmissions, IndividualSubmissions,
                                  DataBatchProcess, DataFiles, CombinedSubmissions)

# dbhs = '172.16.2.23'
dbn = 'IoC'


@app.task(name='route_df')
def route_df(data_tupl):
    # global data_tpl, cols, data_store
    data_batch_no, mdjlogger = None, get_logger(data_tupl[0]['dp_name'])
    data_type, in_mod, out_mod = data_tupl[1], data_tupl[0]['in_mod'], data_tupl[0]['out_mod']
    data_tpl = list(data_tupl)
    df = data_tpl.pop(2)
    # todo gather_stats(data_tpl)
    try:
        if in_mod in ('cmb', 'iff', 'mfi', 'pmi', 'sb2',):
            if data_type in ('corp',):
                if in_mod == 'cmb':
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        data_type], 'corporate_submissions'  # CorporateSubmissions
                    return data2col([data_tpl, cols, data_store], df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
                else:
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        'comm'], 'corporate_submissions'  # CorporateSubmissions
                    return data2col((data_tpl, cols, data_store), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ('ndvdl',):
                if in_mod == 'cmb':
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        data_type], 'individual_submissions'  # IndividualSubmissions
                    return data2col((data_tpl, cols, data_store), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
                else:
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        'cons'], 'individual_submissions'  # IndividualSubmissions
                    return data2col((data_tpl, cols, data_store), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ('corpfac', 'ndvdlfac', 'fac',):
                if in_mod == 'cmb':
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        data_type], 'facility_submissions'  # FacilitySubmissions
                    return data2col((data_tpl, cols, data_store), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
                else:
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        'fac'], 'facility_submissions'  # FacilitySubmissions
                    return data2col((data_tpl, cols, data_store), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])

        elif in_mod in ('cdt', 'udf'):
            if data_type in ('corp',):
                data_tpl, cols, data_store = data_tpl, cdt()[data_type], 'corporate_submissions'  # CorporateSubmissions
                return data2col((data_tpl, cols, data_store), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ('ndvdl',):
                data_tpl, cols, data_store = data_tpl, cdt()[
                    data_type], 'individual_submissions'  # IndividualSubmissions
                return data2col((data_tpl, cols, data_store), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ('corpfac', 'ndvdlfac', 'fac',):
                data_tpl, cols, data_store = data_tpl, cdt()[data_type], 'facility_submissions'  # FacilitySubmissions
                return data2col((data_tpl, cols, data_store), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])

        elif in_mod in ('fandl',):
            if data_type in ('corp',):
                data_tpl, cols, data_store = data_tpl, fandl()[
                    data_type], 'corporate_submissions'  # CorporateSubmissions
                return data2col((data_tpl, cols, data_store), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ('ndvdl',):
                data_tpl, cols, data_store = data_tpl, fandl()[
                    data_type], 'individual_submissions'  # IndividualSubmissions
                return data2col((data_tpl, cols, data_store), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ('corpfac', 'ndvdlfac', 'fac',):
                data_tpl, cols, data_store = data_tpl, fandl()[data_type], 'facility_submissions'  # FacilitySubmissions
                return data2col((data_tpl, cols, data_store), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])

        elif in_mod in ('c0mf', 'visa',):
            if data_type in ('combo',):
                data_tpl, cols, data_store = data_tpl, c0mf(), 'combined_submissions'  # CombinedSubmissions
                return data2col((data_tpl, cols, data_store), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            else:
                mdjlogger.error("Specified route for data does not exist!")

        elif in_mod in ('phed',):
            if data_type in ('corp',):
                data_tpl, cols, data_store = data_tpl, phed()[
                    data_type], 'corporate_submissions'  # CorporateSubmissions
                return data2col((data_tpl, cols, data_store), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ('corpfac', 'fac',):
                data_tpl, cols, data_store = data_tpl, phed()[data_type], 'facility_submissions'  # FacilitySubmissions
                return data2col((data_tpl, cols, data_store), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])

        else:
            mdjlogger.info('Sorry dear, route has not been configured for {} input mode'.format(in_mod.upper()))

    except Exception as e:
        mdjlogger.error(e)
        return False


"""
# keep for transformation routing
def route_iff(data_batch_no, data_tpl):
    in_mod, out_mod = data_tpl[0]['in_mod'], data_tpl[0]['out_mod']

    if in_mod == out_mod and out_mod in ('iff', 'sb2',):
        # bank1 no transformation required
        data_batch_no = route_iff(data_batch_no, data_tpl)
        return data_batch_no

    if (in_mod == 'iff' and out_mod == 'sb2') or (in_mod == 'sb2' and out_mod == 'iff'):
        # !bank1 transformation required
        data_batch_no = route_iff(data_batch_no, data_tpl)

    return data_batch_no
"""


def data2col(args, df):
    data_tpl, cols, data_store = args[0], args[1], args[2]
    file_name, mdjlogger = data_tpl[0]['file_name'], get_logger(data_tpl[0]['dp_name'])
    data_batch_info, df_dtls = {}, DataFiles.objects(file_name=data_tpl[0]['file_name']).first()
    # data_batch_info = {}
    mdjlogger.info("checking counts in d2c {} | data type is {}".format(df.shape[0], data_tpl[1]))
    # count_down(None, 5)
    try:
        batchID = getID() if not data_tpl[0]['batch_no'] else data_tpl[0]['batch_no']
        # data_batch_info['batch_no'], dp_name, df = batchID, data_tpl[0]['dp_name'], data_tpl[2]
        data_batch_info['batch_no'], dp_name = batchID, data_tpl[0]['dp_name']
        # if None in df:
        #     del df[None]
        df.replace(to_replace='NULL', value='', inplace=True)
        df.replace(to_replace='N/A', value='', inplace=True)
        df.columns, dpid, cycle_ver = cols, data_tpl[0]['dpid'], data_tpl[0]['cycle_ver']
        data_batch_info['dp_name'], df['batch_no'], df['dpid'], df['cycle_ver'] = dp_name, batchID, dpid, cycle_ver
        data_batch_info['dpid'], data_batch_info['cycle_ver'], data_batch_info['file_name'] = dpid, cycle_ver, file_name
        data_batch_info['in_mod'], data_batch_info['out_mod'] = data_tpl[0]['in_mod'], data_tpl[0]['out_mod']
        in_mod = data_tpl[0]['in_mod']
        # bulk upsert
        # bulk = data_store._get_collection().initialize_ordered_bulk_op()
        # bulk = db.data_store.initialize_ordered_bulk_op()
        # bulk_upd8s = []
        # idx = 0
        df.fillna('', inplace=True)
        df['status'], df['dp_name'], df['data_file'], data_type = 'Loaded', dp_name, data_tpl[0]['file_name'], data_tpl[
            1]
        # df_dict = df.to_dict('records')
        # mdjlogger.info(df.columns)
        dfbn = '_'.join((dp_name, str(cycle_ver), data_type, str(os.getpid())))
        dfbfnext = 'json' #'csv'  #
        dfbfn = '.'.join((dfbn, dfbfnext))
        # df.to_csv(dfbfn, index=False, quoting=csv.QUOTE_ALL)
        df.to_json(dfbfn, orient='records')
        # cmd_list = ['mongoimport', '-h', dbhs, '-d', dbn, '-c', data_store, '--upsert', '--upsertFields', ',
        # '.join(df.columns), abspath(dfbfn), '--type', dfbfnext, '--headerline', '-j32', '--columnsHaveTypes']

        cmd_list = ['mongoimport', '-h', dbhs, '-d', dbn, '-c', data_store, '--upsert', '--upsertFields',
                    ','.join(df.columns), abspath(dfbfn), ' --jsonArray', '-j32']


        mdjlogger.info(" ".join(cmd_list))
        rez_mprt = subprocess.Popen(" ".join(cmd_list), shell=True, stdout=subprocess.PIPE).communicate()
        mdjlogger.info(rez_mprt)
        remove(abspath(dfbfn))
        # dfjsonh = open(dfbfn, 'w')
        # dfjsonh.write(json.dumps({"mydata": df.to_dict('records')}, default=json_util.default))
        # dfjsonh.close()
        # with futures.ProcessPoolExecutor() as executor:
        #     syndi_rez = executor.map(syndi_pairs, df_dict)
        # for idx, row in df.iterrows():
        #     try:
        #         row_dict = row.to_dict()
        #         # row_dict['status'], row_dict['dp_name'] = 'Loaded', dp_name
        #         # works bns collect_dict.apply_async(args=[dp_name, data_type, in_mod, data_store, row_dict])
        #         collect_dict(dp_name, data_type, in_mod, data_store, row_dict)
        #     except Exception as e:
        #         mdjlogger.critical(e)
        # bulk_rez = None
        # try:
        #     bulk_rez = pym_db.data_store.bulk_write([UpdateOne(
        #         {"dpid": row['dpid'], "account_no": row['account_no'], "cycle_ver": row['cycle_ver']},
        #         {'$inc': row}, upsert=True
        #     ) for row in df.to_dict('records')]) #.iterrows()
        # except Exception as e:
        #     mdjlogger.error(e)

        # result = pym_db.data_store.bulk_write([bulk_ops for bulk_ops in bulk_upd8s])
        # count_down(None, 50)

        # mdjlogger.info("bulk_upd8s: {}".format(len(bulk_upd8s)))
        # mdjlogger.info("last idx: {}".format(idx))
        # bulk_rez = bulk.execute()
        # mdjlogger.info(bulk_rez)
        # data_cursor = pym_db[data_store].find({"dpid": dpid, "cycle_ver": data_tpl[0]['cycle_ver']})
        # mdjlogger.info(len(list(data_cursor)))
        # fac_df = pd.DataFrame(list(fac_cursor))
        pro_stat = 'Loaded'
        data_batch_info['status'], data_batch_info['data_type'] = pro_stat, data_tpl[0]['data_type']
        df_dtls.update(status=pro_stat)  # batch_no=batchID,

        try:
            data_batch_data = DataBatchProcess(**data_batch_info)
            data_batch_data.save()
        except Exception as e:
            # mdjlogger.error(e)
            data_batch_data = DataBatchProcess.objects(cycle_ver=data_tpl[0]['cycle_ver'],
                                                       dp_name=data_tpl[0]['dp_name'],
                                                       data_type=data_tpl[0]['data_type']).first()
            data_batch_data.update(**data_batch_info)
        #
        data_batch_data.reload()
        data_batch_data.update(segments=list(set(data_batch_data['segments'] + [data_tpl[1]])))
        return batchID
    except Exception as e:
        mdjlogger.error(e)


@app.task(name='collect_dict')
def collect_dict(dp_name, data_type, in_mod, data_store, row_dict):
    mdjlogger = get_logger(dp_name)
    try:
        rec_col = pym_db[data_store]
        if in_mod in ('c0mf', 'cmb', 'fandl', 'iff', 'mfi', 'phed', 'pmi'):
            # bulk.find({"dpid": row['dpid'], "account_no": row['account_no'], "cycle_ver": row['cycle_ver']}
            #           ).upsert().update({'$inc': row_dict})
            rec_col.update(
                {"dpid": row_dict['dpid'], "account_no": row_dict['account_no'], "cycle_ver": row_dict['cycle_ver']},
                {'$set': row_dict}, upsert=True
            )
        else:
            if data_type in ('corp', 'ndvdl',):
                # bulk.find({"dpid": row['dpid'], "cust_id": row['cust_id'], "cycle_ver": row['cycle_ver']}
                #           ).upsert().update({'$inc': row_dict})
                rec_col.update(
                    {"dpid": row_dict['dpid'], "cust_id": row_dict['cust_id'], "cycle_ver": row_dict['cycle_ver']},
                    {'$set': row_dict}, upsert=True
                )
            elif data_type in ('corpfac', 'ndvdlfac', 'fac',):
                # bulk.find(
                #           ).upsert().update({'$inc': row_dict})
                rec_col.update(
                    {"dpid": row_dict['dpid'], "account_no": row_dict['account_no'],
                     "cycle_ver": row_dict['cycle_ver']},
                    {'$set': row_dict}, upsert=True
                )
                #
                # row_dict['idx'] = str(idx + 1)
                # if data_tpl[1] in ('corp', 'ndvdl',):
                #     bulk.find(
                #         {"dpid": row['dpid'], "cust_id": row['cust_id'], "cycle_ver": row['cycle_ver']}
                #     ).upsert().update({'$inc': row_dict})
                # elif data_tpl[1] in ():
                #     bulk.find({"dpid": row['dpid'], "cust_id": row['cust_id'],
                #            "cycle_ver": row['cycle_ver']}).upsert().update(row.to_dict())
                # elif data_tpl[1] in ('corpfac', 'ndvdlfac', 'fac',):
                #     bulk.find(
                #         {"dpid": row['dpid'], "account_no": row['account_no'], "cycle_ver": row['cycle_ver']}
                #     ).upsert().update({'$inc': row_dict})

                # bulk_upd8s.append(row_dict)
                # bulk_upd8s.append(UpdateOne(
                #
                #
                # ))

                # except BulkWriteError as bwe:
                #     mdjlogger.critical(bwe.details)
    except Exception as e:
        mdjlogger.info(e)


def gather_stats(data_tpl):
    df, file_report_stats = data_tpl[2], {}
    if df.shape[0] > 1:
        # file name
        # total
        if data_tpl[1] in ('commfac', 'consfac', 'fac',):
            # account no
            acno_stats = df.account_no.describe()
            file_report_stats['t_ac_nos'], file_report_stats['u_ac_nos'] = acno_stats['count'], acno_stats['unique']
            # account with no customer
            # balance
            df.outstanding_bal = df.outstanding_bal.apply(lambda x: x if isinstance(x, Real) else 0)
            # currency
        if data_tpl[1] in ('comm', 'commsub',):
            # customer id
            cust_id_stats = df.cust_id.describe()
            file_report_stats['t_corp_custs'], file_report_stats['u_corp_custs'] = cust_id_stats['count'], \
                                                                                   cust_id_stats['unique']
            # customer with no account
            # this will be done during syndication
            # rc number
            df.columns.values[5] = 'biz_reg_no'
            rcno_stats = df.biz_reg_no.describe()
            file_report_stats['t_rc_nos'], file_report_stats['u_rc_nos'] = rcno_stats['count'], rcno_stats['unique']
        if data_tpl[1] in ('cons', 'conssub',):
            # bvn
            bvn_stats = df.bvn.describe()
            file_report_stats['t_bvns'], file_report_stats['u_bvns'] = bvn_stats['count'], bvn_stats['unique']
            # need to do valid bvn
            # customer id
            cust_id_stats = df.cust_id.describe()
            file_report_stats['t_indvdl_custs'], file_report_stats['u_indvdl_custs'] = cust_id_stats['count'], \
                                                                                       cust_id_stats['unique']
            # customer with no account
            # this will be done during syndication

        print(file_report_stats)
