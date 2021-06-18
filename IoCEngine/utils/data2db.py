# import motor
import json
from datetime import datetime
from numbers import Real

import pandas as pd
import progressbar
from elasticsearch import helpers

from IoCEngine.SHU.amounts import float_numbers
from IoCEngine.SHU.trans4mas import fields2date, normalize_amts
from IoCEngine.celeryio import app
from IoCEngine.commons import (
    cdt_udf_modes, cf, count_down, cs, data_type_dict, fs, getID, ns, submission_type_dict, gs, ps
)
from IoCEngine.config.pilot import es, es_i
from IoCEngine.logger import get_logger, module_logger
from IoCEngine.utils.data_modes import *
from IoCEngine.utils.file import (pym_db, DataBatchProcess, DataFiles)
from utilities.models import ColumnMapping, InMode, IoCField

# from pymongo import UpdateOne
# from pymongo.errors import BulkWriteError
# from tornado import gen

# dbhs = '172.16.2.23'
dbn = 'IoC'

log_txt = module_logger()
logger = get_logger(log_txt, funcname=True)
logger.info('test')


@app.task(name='route_df')
def route_df(data_tupl):
    # global data_tpl, cols, data_store
    data_batch_no, mdjlogger = None, get_logger(data_tupl[0]['dp_name'])
    data_type, in_mod, out_mod = data_tupl[1], data_tupl[0]['in_mod'], data_tupl[0]['out_mod']
    data_tpl = list(data_tupl)
    df = data_tpl.pop(2)
    df = df[[col for col in df.columns if col is not None]]
    # todo gather_stats(data_tpl)
    try:
        if in_mod in ('cmb', 'iff', 'mfi', 'pmi', 'sb2',):
            if data_type in cs:
                if in_mod == 'cmb':
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        data_type], 'corporate_submissions'  # CorporateSubmissions
                    return data2col((data_tpl, cols, data_store, data_type), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
                else:
                    try:
                        ifff = iff()[in_mod if in_mod != 'iff' else out_mod]['comm']
                    except:
                        ifff = iff()[in_mod if in_mod != 'iff' else out_mod][data_type]
                    data_tpl, cols, data_store = data_tpl, ifff, 'corporate_submissions'  # CorporateSubmissions
                    return data2col((data_tpl, cols, data_store, data_type), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ns:
                if in_mod == 'cmb':
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        data_type], 'individual_submissions'  # IndividualSubmissions
                    return data2col((data_tpl, cols, data_store, data_type), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
                else:
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        data_type], 'individual_submissions'  # IndividualSubmissions
                    return data2col((data_tpl, cols, data_store, data_type), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in fs:
                if in_mod == 'cmb':
                    data_tpl, cols, data_store = data_tpl, iff()[in_mod if in_mod != 'iff' else out_mod][
                        data_type], 'facility_submissions'  # FacilitySubmissions
                    return data2col((data_tpl, cols, data_store, data_type), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])
                else:
                    try:
                        ifff = iff()[in_mod if in_mod != 'iff' else out_mod]['fac']
                    except:
                        ifff = iff()[in_mod if in_mod != 'iff' else out_mod][data_type]
                    data_tpl, cols, data_store = data_tpl, ifff, 'facility_submissions'  # FacilitySubmissions
                    return data2col((data_tpl, cols, data_store, data_type), df)
                    # return ppns(data2col, df, [data_tpl, cols, data_store])

        elif in_mod in cdt_udf_modes:
            if data_type in cs:
                data_tpl, cols, data_store = data_tpl, cdt()[data_type], 'corporate_submissions'  # CorporateSubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ns:
                data_tpl, cols, data_store = data_tpl, cdt()[
                    data_type], 'individual_submissions'  # IndividualSubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in fs:
                data_tpl, cols, data_store = data_tpl, cdt()[data_type], 'facility_submissions'  # FacilitySubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in gs:
                data_tpl, cols, data_store = data_tpl, cdt()[data_type], 'guarantors'  # FacilitySubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
            if data_type in ps:
                data_tpl, cols, data_store = data_tpl, cdt()[data_type], 'principal_officers'  # FacilitySubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)

        elif in_mod in ('fandl',):
            if data_type in cs:
                data_tpl, cols, data_store = data_tpl, fandl()[
                    data_type], 'corporate_submissions'  # CorporateSubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in ('cons', 'ndvdl',):
                data_tpl, cols, data_store = data_tpl, fandl()[
                    data_type], 'individual_submissions'  # IndividualSubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in fs:
                data_tpl, cols, data_store = data_tpl, fandl()[data_type], 'facility_submissions'  # FacilitySubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])

        elif in_mod in ('c0mf', 'visa',):
            if data_type in ('combo',):
                data_tpl, cols, data_store = data_tpl, c0mf(), 'combined_submissions'  # CombinedSubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            else:
                mdjlogger.error("Specified route for data does not exist!")

        elif in_mod in ('phed',):
            if data_type in cs:
                data_tpl, cols, data_store = data_tpl, phed()[
                    data_type], 'corporate_submissions'  # CorporateSubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])
            if data_type in cf:
                data_tpl, cols, data_store = data_tpl, phed()[data_type], 'facility_submissions'  # FacilitySubmissions
                return data2col((data_tpl, cols, data_store, data_type), df)
                # return ppns(data2col, df, [data_tpl, cols, data_store])

        else:
            mdjlogger.info('Sorry dear, route has not been configured for {} input mode'.format(in_mod.upper()))

    except Exception as e:
        mdjlogger.error(e)
        return False


def upd8col_mappings(mdjlogger, incols, ioc_cols, in_mod):
    if ioc_cols:
        for incol, ioc_col in ((in_col.upper(), col) for in_col, col in zip(incols, ioc_cols)):
            # mdjlogger.info('{} | {} | {}'.format(incol, ioc_col, in_mod))
            try:
                ioc_field = IoCField.objects(name=ioc_col).first()
                inmode = InMode(in_mod)
                if not ioc_field:
                    ioc_field = IoCField(name=ioc_col)
                    ioc_field.in_mode.append(inmode)
                else:
                    if not inmode in ioc_field.in_mode:
                        # ioc_field.update(in_mode=list(set(ioc_field.in_mode + [in_mod])))
                        ioc_field.in_mode.append(inmode)
                ioc_field.save()
            except Exception as e:
                mdjlogger.warning(e)
            try:
                colmap = ColumnMapping(*(incol, ioc_field))
                colmap.save()
            except Exception as e:
                # mdjlogger.warning(e)
                pass
    else:
        for incol in incols:
            try:
                colmap = ColumnMapping(*(incol.upper()))
                colmap.save()
            except Exception as e:
                mdjlogger.warning(e)


def prep_grntr_id(args):
    if all(arg.strip() in ('', None,) for arg in args[4:]):
        return None
    cycle_ver, dpid, account_no, grntr_type = args[:4]
    biz_name, biz_reg_no = (i.strip() for i in args[4:6])
    last_name, first_name, national_id_no, drivin_license_no, bvn, i_pass_no = (i.strip() for i in args[6:])
    logger.info(
        f'''{cycle_ver=}, {account_no=}, {grntr_type=},
        {biz_name=}, {biz_reg_no=},
        {last_name=}, {first_name=}, {national_id_no=}, {drivin_license_no=}, {bvn=}, {i_pass_no=}'''
    )
    id = f"{dpid}-{account_no}"
    if grntr_type_check(grntr_type):
        if biz_reg_no not in ('', None):
            _id = f"{id}-R-{biz_reg_no}"
            logger.info(f"{_id=}")
            return f"{_id}-{cycle_ver}"
        if biz_name not in ('', None):
            _id = f"{id}-C-{biz_name}"
            logger.info(f"{_id=}")
            return f"{_id}-{cycle_ver}"
    else:
        if bvn not in ('', None):
            _id = f"{id}-B-{bvn}"
            logger.info(f"{_id=}")
            return f"{_id}-{cycle_ver}"
        if i_pass_no not in ('', None):
            _id = f"{id}-I-{i_pass_no}"
            logger.info(f"{_id=}")
            return f"{_id}-{cycle_ver}"
        if drivin_license_no not in ('', None):
            _id = f"{id}-D-{drivin_license_no}"
            logger.info(f"{_id=}")
            return f"{_id}-{cycle_ver}"
        if national_id_no not in ('', None):
            _id = f"{id}-N-{national_id_no}"
            logger.info(f"{_id=}")
            return f"{_id}-{cycle_ver}"
        name = last_name if last_name not in ('', None) else first_name
        logger.info(f"{name=}")
        name2u = f"{last_name} {first_name}" if last_name not in ('', None) and first_name not in ('', None) else name
        if name2u not in ('', None):
            _id = f"{id}-F-{name2u}"
            logger.info(f"{_id=}")
            return f"{_id}-{cycle_ver}"


def grntr_type_check(grntr_type: str) -> str:
    return 'corporate' if {"crp": "corporate", "corporate": "corporate",
            }.get(grntr_type.lower()) == 'corporate' else 'individual'


def prep_prnc_id(args):
    if all(arg in ('', None,) for arg in args[2:]):
        return None
    cycle_ver, dpid, cust_id, last_name, first_name, national_id_no, drivin_license_no, bvn, i_pass_no = args
    logger.info(f"""{cycle_ver=}, {dpid=}, {cust_id=},
        {last_name=}, {first_name=}, {national_id_no=}, {drivin_license_no=}, {bvn=}, {i_pass_no=}"""
    )
    id = f"{dpid}-{cust_id}"
    if bvn not in ('', None):
        _id = f"{id}-B-{bvn}"
        logger.info(f"{_id=}")
        return f"{_id}-{cycle_ver}"
    if i_pass_no not in ('', None):
        _id = f"{id}-I-{i_pass_no}"
        logger.info(f"{_id=}")
        return f"{_id}-{cycle_ver}"
    if drivin_license_no not in ('', None):
        _id = f"{id}-D-{drivin_license_no}"
        logger.info(f"{_id=}")
        return f"{_id}-{cycle_ver}"
    if national_id_no not in ('', None):
        _id = f"{id}-N-{national_id_no}"
        logger.info(f"{_id=}")
        return f"{_id}-{cycle_ver}"
    name = last_name if last_name not in ('', None) else first_name
    logger.info(f"{name=}")
    name2u = f"{last_name} {first_name}" if last_name not in ('', None) and first_name not in ('', None) else name
    if name2u not in ('', None):
        _id = f"{id}-F-{name2u}"
        logger.info(f"{_id=}")
        return f"{_id}-{cycle_ver}"


def stream_grntr(_type: str, index_col: str, df: pd.DataFrame()):
    df.loc[:, '_id'] = df[[
        'cycle_ver', 'dpid', 'account_no', 'grntr_type',
        'biz_name', 'biz_reg_no',
        'last_name', 'first_name', 'national_id_no', 'drivin_license_no', 'bvn', 'i_pass_no',
    ]].apply(lambda x: prep_grntr_id(x), axis=1)
    df = df[~df['_id'].isna()]
    df.fillna('', inplace=True)
    df.set_index('_id', inplace=True)
    sub, typ = None, 'guarantor'
    for ii in df.itertuples():
        i = 0
        try:
            d, i = ii._asdict(), i + 1
            sub = grntr_type_check(d['grntr_type'])
            yield from stream_from(d, i, sub, typ)
        except Exception as e:
            logger.error(e)


def stream_prnc(_type: str, index_col: str, df: pd.DataFrame()):
    ps0, ps1, psa, xtr_cols = prnc_cols()
    pdf0 = df[ps0 + xtr_cols]
    # for fld in psa:
    for fld in [f for f in psa if f not in df.columns]:
        df.loc[:, fld] = ''
    pdf1 = df[ps1 + psa + xtr_cols]
    pdf1.columns = ps0 + psa + xtr_cols
    pdf = pd.concat([pdf0, pdf1])
    pdf.loc[:, '_id'] = pdf[['cycle_ver', 'dpid', 'cust_id', 'last_name', 'first_name', 'national_id_no',
                             'drivin_license_no', 'bvn', 'i_pass_no', ]].apply(lambda x: prep_prnc_id(x), axis=1)
    pdf = pdf[~pdf['_id'].isna()]
    pdf.fillna('', inplace=True)
    pdf.set_index('_id', inplace=True)
    sub, typ = 'corporate', 'officer'
    for ii in pdf.itertuples():
        i = 0
        try:
            d, i = ii._asdict(), i + 1
            yield from stream_from(d, i, sub, typ)
        except Exception as e:
            logger.error(e)


def stream_from(d, i, sub, typ):
    if not i % 35710: count_down(None, 5)
    d["submission"], d["type"] = sub if sub else d["submission"], typ if typ else d["type"]
    d["cycle_ver"] = int(d["cycle_ver"])
    d["_id"], d["cy_dp"] = d['Index'], f'{d["cycle_ver"]}-{d["dpid"]}'
    del d['Index']
    d = {i: d[i] for i in d if d[i] not in ('', 'none', None,) and i != 'ndx'}
    d["_index"], d["_type"], d['sub_cy'] = es_i, 'submissions', datetime.now()
    yield d


def prnc_cols():
    ps0 = ['cust_id', 'last_name', 'first_name', 'middle_name', 'birth_date', 'gender',
           'pri_addr_line1', 'pri_addr_line2', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_country',
           'national_id_no', 'drivin_license_no', 'bvn', 'i_pass_no', 'phone_no', 'e_mail_addr', 'psxn_in_biz', ]
    ps1 = ['cust_id', 'last_name1', 'first_name1', 'middle_name1', 'birth_date1', 'gender1',
           'pri_addr_line11', 'pri_addr_line21', 'pri_addr_city_lga1', 'pri_addr_state1', 'pri_addr_country1',
           'national_id_no1', 'drivin_license_no1', 'bvn1', 'i_pass_no1', 'phone_no1', 'e_mail_addr1', 'psxn_in_biz1', ]
    psa = ['phone_no2', 'sec_addr_line', 'sec_addr_city_lga', 'sec_addr_state', 'tax_id', 'pic_file_path', ]
    xtr_cols = ['batch_no', 'cycle_ver', 'dpid', 'status', 'dp_name', 'data_file', ]
    return ps0, ps1, psa, xtr_cols


def prep_sngl_col_id(args):
    dpid, ndx, cust_id, account_no, cycle_ver = args
    if str(ndx).replace('\\', '').replace('\n', '').replace(' ', '').replace('-', '').replace('_', '').isalpha():
        return None
    if account_no and cust_id:
        return "-".join((dpid, str(cust_id).strip(), str(account_no).strip(), str(int(cycle_ver))))
    else:
        return "-".join((dpid, str(ndx).strip(), str(int(cycle_ver))))


def stream_df(_type, index_col, df):
    i = 0
    for fld in [f for f in ('cust_id', 'account_no', index_col, ) if f not in df]:
        df.loc[:, fld] = None
    df.loc[:, '_id'] = df[['dpid', index_col, 'cust_id', 'account_no', 'cycle_ver', ]].apply(
        lambda x: prep_sngl_col_id(x), axis=1)
    df = df[~df[index_col].isin(['', None, ])]
    df.set_index('_id', inplace=True)
    for ii in df.itertuples():
        try:
            d, i = ii._asdict(), i + 1
            yield from stream_from(d, i, None, None)
        except Exception as e:
            logger.error(e)


def data2col(args, df):
    data_tpl, cols, data_store, datatype = args[0], args[1], args[2], args[3]
    file_name, mdjlogger = data_tpl[0]['file_name'], get_logger(data_tpl[0]['dp_name'])
    data_batch_info, df_dtls = {}, DataFiles.objects(file_name=data_tpl[0]['file_name']).first()
    df_dtls[datatype] = df.shape[0]
    df_dtls.save()
    # todo
    # upd8col_mappings(mdjlogger, df.columns, cols, data_tpl[0]['in_mod'])
    try:
        data_batch_data = DataBatchProcess.objects(
            cycle_ver=data_tpl[0]['cycle_ver'], dp_name=data_tpl[0]['dp_name'], data_type=data_tpl[0]['data_type']
        ).first()
        batchID = getID() if not data_tpl[0]['batch_no'] else data_tpl[0]['batch_no']

        data_batch_info['batch_no'], dp_name = batchID, data_tpl[0]['dp_name']

        df.replace(to_replace='NULL', value='', inplace=True)
        df.replace(to_replace='N/A', value='', inplace=True)
        #
        if None in df.columns:
            df = df[[c for c in df.columns if c != None]]
        df = df[[c for c in df.columns if 'unnamed' not in c.lower()]]
        df.columns, dpid, cycle_ver = cols, data_tpl[0]['dpid'], data_tpl[0]['cycle_ver']
        #
        data_batch_info['dp_name'] = dp_name
        data_batch_info['dpid'], data_batch_info['cycle_ver'], data_batch_info['file_name'] = dpid, cycle_ver, file_name
        data_batch_info['in_mod'], data_batch_info['out_mod'] = data_tpl[0]['in_mod'], data_tpl[0]['out_mod']
        #
        df.loc[:, 'batch_no'], df.loc[:, 'dpid'], df.loc[:, 'cycle_ver'] = batchID, dpid, str(cycle_ver)
        df.loc[:, 'submission'], df.loc[:, 'type'] = submission_type_dict[datatype], data_type_dict[datatype]

        if data_tpl[0]['in_mod'] in ('cdt',):
            sf = cs + ns + ps
            ndx = 'cust_id' if datatype in sf else 'account_no'
            ndx = ['cust_id', 'last_name', 'first_name'] if datatype in ps else ndx
            dups_df = df[df.duplicated(subset=ndx, keep='first')]
            mdjlogger.info("{} {}".format(datatype, ndx))
            ndx_nunique = df[ndx].nunique()
            dups = dups_df.shape[0]
            if dups > 0:
                mdjlogger.warn(f'dropping {dups} duplicates from {df.shape[0]} to have {ndx_nunique} unique records')
                # todo log duplicates before dropping 'em. ..
                if datatype not in gs + ps:
                    df.drop_duplicates(subset=ndx if isinstance(ndx, list) else [ndx], keep='last', inplace=True)
            else:
                mdjlogger.info("no duplicates")
        mdjlogger.info("checking counts in d2c {} | data type is {}".format(df.shape[0], data_tpl[1]))

        df.fillna('', inplace=True)

        data_type = indexDF(data_store, data_tpl, df, dp_name, mdjlogger)

        pro_stat = 'Loaded'
        data_batch_info['status'], data_batch_info['data_type'] = pro_stat, data_tpl[0]['data_type']
        try:
            data_batch_info[data_store.split('_')[0]] = df.shape[0]
            if 'facility' in data_batch_data and '_all_' in file_name:
                data_batch_info[data_type] = df.shape[0]
                data_batch_info['facility'] = sum(
                    [data_batch_data[k] for k in data_batch_data._data.keys() if k.endswith('fac')] + [
                        data_batch_info[k] for k in data_batch_info.keys() if k.endswith('fac')])
            else:
                pass
        except Exception as e:
            data_batch_info[data_store.split('_')[0]] = df.shape[0]
        df_dtls.update(status=pro_stat)  # batch_no=batchID,

        # try:
        if not data_batch_data:
            data_batch_data = DataBatchProcess(**data_batch_info)
            data_batch_data.save()
        else:
            data_batch_data.update(**data_batch_info)
        # except Exception as e:
        #     data_batch_data.update(**data_batch_info)

        data_batch_data.reload()
        data_batch_data.update(segments=list(set(data_batch_data['segments'] + [data_tpl[1]])))

        return batchID
    except Exception as e:
        mdjlogger.error(e)


def indexDF(data_store, data_tpl, df, dp_name, mdjlogger):
    if 'int_rate' in df.columns:
        df['int_rate'] = df.int_rate.apply(float_numbers)

    for col in df.columns:
        if col in dates_and_number_fields():
            pass
        else:
            try:
                df.loc[:, col] = df[col].apply(lambda x: str(x) if x else '')
            except Exception as e:
                mdjlogger.error(f"Field Error: {e} in {col} field")
    if data_store.split('_')[0] in ('facility'):
        # df = trnsfm_amts(data_tpl[0], df)
        df = normalize_amts(data_tpl[0], df)
    df = fields2date(data_tpl[0], df)

    df.loc[:, 'status'], df.loc[:, 'dp_name'], df.loc[:, 'data_file'] = 'Loaded', dp_name, data_tpl[0]['file_name']
    bulk_data, data_type, i, _type = [], data_tpl[1], 0, data_store

    sf = cs + ns  # + ps
    ndx = 'cust_id' if data_tpl[1] in sf else 'account_no'
    # ndx = ['cust_id', 'last_name', 'first_name'] if data_tpl[1] in ps else ndx

    if 'bvn' in df.columns:
        df.loc[:, 'bvn'] = df.bvn.apply(lambda x: str(x).replace('.0', '') if str(x).endswith('.0') else str(x))
    if 'cust_id' in df.columns:
        df.loc[:, 'cust_id'] = df.cust_id.apply(lambda x: str(x).replace('.0', '') if str(x).endswith('.0') else str(x))
        df.loc[:, 'cust_id'] = df.cust_id.apply(lambda x: str(x).replace("'", '').strip())
    if 'account_no' in df.columns:
        df.loc[:, 'account_no'] = df.account_no.apply(lambda x: str(x).replace("'", '').strip())
    # df.fillna('', inplace=True)

    if not isinstance(ndx, list):
        df.set_index(ndx, inplace=True)
        df.loc[:, ndx] = df.index

    df.loc[:, 'ndx'] = df.index
    # df['sub_cy'] = datetime.strptime(data_tpl[0].date_reported, "%d-%b-%Y").date()
    df.loc[:, 'sub_cy'] = datetime.now()
    # df.to_excel(f"{dp_name} {data_type}'s segment for {data_tpl[0]['cycle_ver']} data cycle.xlsx")  # todo remove

    if _type == 'guarantors':
        df_stream = stream_grntr(_type, 'ndx', df)
    elif _type == 'principal_officers':
        df_stream = stream_prnc(_type, 'ndx', df)
    else:
        df_stream = stream_df(_type, ndx, df)

    try:
        mdjlogger.info("#IndexinG!")
        bulk_resp = helpers.bulk(es, df_stream, refresh=True)
        mdjlogger.info(f"{bulk_resp=}")
        # es.indices.refresh()
        mdjlogger.info(". .. about {} {} data indexed".format(df.shape[0], data_type))
    except Exception as e:
        mdjlogger.error(e)
    #
    return data_type  # , df


def bulkI(INDEX_NAME, bulk_data):
    count_down(None, 5)
    res = es.bulk(index=INDEX_NAME, body=bulk_data, refresh=True)
    return res


def rezNDXerrors(data_d, data_store, data_tpl, ndx_d, res):
    bulk_data, mdjlogger = [], get_logger(data_tpl[0]['dp_name'])
    i, pbmv = 0, len(res['items'])
    with progressbar.ProgressBar(max_value=pbmv) as progrbar:
        for i_res in res['items']:
            i += 1
            if 'error' in i_res['index']:
                error = i_res['index']['error']
                field, xcpxn_cat, xcpxn_desc = error['reason'], 'Field Value Error', error['caused_by']['reason']
                field = field[field.find('[') + 1:field.find(']')]
                iid, sbjt = i_res['index']['_id'], False if data_store.split('_')[0] in ('facility',) else True
                # todo R3Visit
                # todo db_log_xcpxns(data_tpl[0], data_d[iid], rec_d[iid], (xcpxn_cat, xcpxn_desc,), sbjt, (field,))
                del data_d[iid][field]
                bulk_data.append(ndx_d[iid])
                bulk_data.append(data_d[iid])
                progrbar.update(i)

    res = es.bulk(index=es_i, body=bulk_data, refresh=True)
    if res['errors']:
        mdjlogger.critical("Indexing Results:\n{}".format(json.dumps(res, indent=4)))
    mdjlogger.info("Indexing Results:\n{}".format(json.dumps(res, indent=4)))


@app.task(name='collect_dict')
def collect_dict(dp_name, data_type, in_mod, data_store, row_dict):
    mdjlogger = get_logger(dp_name)
    try:
        rec_col = pym_db[data_store]
        if in_mod in ('c0mf', 'cmb', 'fandl', 'iff', 'mfi', 'phed', 'pmi'):
            rec_col.update(
                {"dpid": row_dict['dpid'], "account_no": row_dict['account_no'], "cycle_ver": row_dict['cycle_ver']},
                {'$set': row_dict}, upsert=True
            )
        else:
            if data_type in ('corp', 'ndvdl',):
                rec_col.update(
                    {"dpid": row_dict['dpid'], "cust_id": row_dict['cust_id'], "cycle_ver": row_dict['cycle_ver']},
                    {'$set': row_dict}, upsert=True
                )
            elif data_type in ('corpfac', 'ndvdlfac', 'fac',):
                rec_col.update(
                    {"dpid": row_dict['dpid'], "account_no": row_dict['account_no'],
                     "cycle_ver": row_dict['cycle_ver']},
                    {'$set': row_dict}, upsert=True
                )
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

        logger.info(f"{file_report_stats=}")
