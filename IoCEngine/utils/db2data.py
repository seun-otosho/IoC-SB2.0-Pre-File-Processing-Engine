import pandas as pd
from elasticsearch import helpers
from pandasticsearch import Select

from IoCEngine.commons import (
    cs, data_type_dict, fs, gs, ns, submission_type_dict, re_ndx_flds, profile
)
from IoCEngine.config.pilot import chunk_size as split_var, es, es_i
from IoCEngine.logger import get_logger
from IoCEngine.utils.data2db import prep_grntr_id
from IoCEngine.utils.file import DataBatchProcess

mdjlog = get_logger('jarvis')


# split_var = 45788


def combo_data(dpid, loaded_batch, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type = 'combined_submissions'
        data_size = int(loaded_batch[data_doc_type.split('_')[0]])
        # confirm_settings()
        combo_rez = es.search(index=es_i, doc_type=data_doc_type,
                              body={"query": {"bool": {"must": [{"match": {'dpid': dpid}},
                                                                {"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                                {"match": {'status': 'Loaded'}}
                                                                ]
                                                       }
                                              }
                                    }
                              , size=data_size, from_=0)
        
        combo_df = Select.from_dict(combo_rez).to_pandas()
        combo_df.fillna('', inplace=True)
        if not combo_df.empty:
            del combo_df['batch_no'], combo_df['cycle_ver'], combo_df['dpid'], combo_df['status']
            # del combo_df['_score'], combo_df['_index'], combo_df['_type']
            mdjlog.info(combo_df.memory_usage(deep=True).sum())
            # combo, combo_df = rez_df(combo_df, loaded_batch, 'account_no')
            # minify(fac_df)
            mdjlog.info(
                "Combo Data Shape {} using memory: {}".format(combo_df.shape, combo_df.memory_usage(deep=True).sum()))
            return True, combo_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


@profile
def conf_df(loaded_batch, mdjlog, df, rez):
    if df is not None and not df.empty:
        # del df['batch_no'], df['cycle_ver'], df['dpid'], df['status']
        del df['_score'], df['_index'], df['_type']
        df_memo = df.memory_usage(deep=True).sum()
        mdjlog.info('{} using {} of memory'.format(loaded_batch, df_memo))
        # isdf, df = rez_df(df, loaded_batch, ndx_col)
        # minify(df)
        df_shape = df.shape
        mdjlog.info("Rez len: {} | Data Shape {} using memory: {}".format(len(rez['hits']['hits']), df_shape, df_memo))
        return df, df_shape[0]
    else:
        return None, 0


def i2df(data_doc_type, data_size, dpid, loaded_batch, ndx_col):
    query = es_query(data_doc_type, dpid, loaded_batch)
    rez = es.search(index=es_i, doc_type='submissions', body={"query": query}, size=data_size, from_=0)
    
    df = Select.from_dict(rez).to_pandas()
    df = df if (data_doc_type in gs
        ) else df[df.sub_type == f"{submission_type_dict[data_doc_type]}-{data_type_dict[data_doc_type]}"]
    df.drop_duplicates(inplace=True)
    if df is not None and not df.empty:
        df.fillna('', inplace=True)

        upd8DFstatus(data_doc_type, df, ndx_col, 'Pick3D')

    return df, rez, data_size


def es_query(data_doc_type, dpid, loaded_batch):
    query = {
        "bool": {"must": [
            {"match": {"cy_dp": f"{int(loaded_batch['cycle_ver'])}-{dpid}"}},
            {"match": {"type": data_type_dict[data_doc_type]} if data_doc_type in gs else {
                "sub_type": f"{submission_type_dict[data_doc_type]}-{data_type_dict[data_doc_type]}"}}]}
    }
    return query


"""
def es_query(data_doc_type, dpid, loaded_batch):
    query = {
        "bool": {"must": [{"match": {'dpid': dpid}},
                          {"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                          {"match": {'submission': submission_type_dict[data_doc_type]}},
                          {"match": {'type': data_type_dict[data_doc_type]}}]}
    } if data_doc_type == 'fac' else {
        "bool": {"must": [{"match": {'dpid': dpid}},
                          {"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                          # {"match": {'status': 'Loaded'}}, # todo
                          {"match": {'submission': submission_type_dict[data_doc_type]}},
                          {"match": {'type': data_type_dict[data_doc_type]}}]}
    }
    # mdjlog.info(query)
    # count_down(None, 10)
    return query
"""


def upd8DFstatus(data_doc_type, df, index_col, status):
    bulk_upd8_data = []
    for rwd in df.to_dict("records"):
        try:
            data_line = {'status': status} if isinstance(status, str) else {**status}
            ndx_line = {'_index': es_i, '_op_type': 'update',
                        '_id': rwd["_id"], 'doc': {**data_line, **{'submission': submission_type_dict[data_doc_type],
                                                            'type': data_type_dict[data_doc_type], }},
            }
            bulk_upd8_data.append(ndx_line)
            try:
                if len(bulk_upd8_data) == 23457:
                    helpers.bulk(es, bulk_upd8_data)
                    es.indices.refresh()
                    bulk_upd8_data = []
            except Exception as e:
                mdjlog.error(e)
        except Exception as e:
            mdjlog.error(e)
    try:
        helpers.bulk(es, bulk_upd8_data)
        es.indices.refresh()
    except Exception as e:
        mdjlog.error(f"Unable to update index: {e}")


def corp_data(dpid, loaded_batch):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type = 'corporate_submission'
        type_count = data_doc_type.split('_')[0]
        data_doc_type, ndx_col = 'corp', 'cust_id'
        if not type_count in loaded_batch:
            loaded_batch = [dbp for dbp in
                            DataBatchProcess.objects(cycle_ver=loaded_batch['cycle_ver'], dpid=dpid, status='Loaded') if
                            any(x in dbp['segments'] for x in cs)][0]
        data_size = int(loaded_batch[type_count])
        rounds = data_size // split_var + 1
        crounds, size2use = 1 if rounds is (0 or 1) else rounds - 1, round(data_size / rounds)
        if data_size > split_var:
            corp_cc_df, size_done = None, 0
            for c in range(rounds):
                try:
                    size2use = size2use if data_size - size_done > size2use else data_size - size_done
                    corp_df, corp_rez, size = i2df(data_doc_type, size2use, dpid, loaded_batch, ndx_col)
                    if corp_df is not None and not corp_df.empty:
                        corp_df, size = conf_df(loaded_batch, mdjlog, corp_df, corp_rez)
                        if corp_cc_df is None:
                            corp_cc_df = corp_df
                        else:
                            corp_cc_df = pd.concat([corp_cc_df, corp_df])
                    else:
                        mdjlog.warn('no CORPORATE data . ..')
                    size_done += size2use
                    mdjlog.info(
                        "getting! #{} of {} Counts: {} of {} of {}".format(c, crounds, size2use, size_done, data_size))
                except Exception as e:
                    mdjlog.error(e)
            corp_cc_df = corp_cc_df[corp_cc_df.submission == 'corporate']
            return True, corp_cc_df
        else:
            corp_df, corp_rez, size2use = i2df(data_doc_type, data_size, dpid, loaded_batch, ndx_col)
            if corp_df is not None and not corp_df.empty:
                corp_df, size_done = conf_df(loaded_batch, mdjlog, corp_df, corp_rez)
                mdjlog.info(
                    "getting! #{} of {} Counts: {} of {} of {}".format('1', crounds, size2use, size_done, data_size))
                corp_df = corp_df[corp_df.submission == 'corporate']
                return True, corp_df
            else:
                mdjlog.warn('no CORPORATE data . ..')
    except Exception as e:
        mdjlog.error(e)
    return False, None


def fac_data(dpid, loaded_batch, fac_type):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type, fac_cc_df, ndx_col = 'facility_submissions', None, 'account_no'
        type_count = data_doc_type.split('_')[0]
        # data_doc_type = sbjt_type + 'fac'
        if not type_count in loaded_batch:
            loaded_batch = [dbp for dbp in
                            DataBatchProcess.objects(cycle_ver=loaded_batch['cycle_ver'], dpid=dpid, status='Loaded') if
                            any(x in dbp['segments'] for x in fs)][0]
        data_size = int(loaded_batch[type_count])
        rounds, size_done = data_size // split_var + 1, 0
        crounds, size2use = 1 if rounds is (0 or 1) else rounds - 1, round(data_size / rounds)
        if data_size > split_var:
            for c in range(rounds):
                try:
                    size2use = size2use if data_size - size_done > size2use else data_size - size_done
                    fac_df, fac_rez, size = i2df(fac_type, size2use, dpid, loaded_batch, ndx_col)
                    if fac_df is not None and not fac_df.empty:
                        fac_df, size = conf_df(loaded_batch, mdjlog, fac_df, fac_rez)
                        # return True, fac_df
                        if fac_cc_df is None:
                            fac_cc_df = fac_df
                        else:
                            fac_cc_df = pd.concat([fac_cc_df, fac_df])
                    else:
                        mdjlog.warn('no FACILITY data . ..')
                    size_done += size2use
                    mdjlog.info(
                        "getting! #{} of {} Counts: {} of {} of {}".format(c, crounds, size2use, size_done, data_size))
                except Exception as e:
                    mdjlog.error(e)
            # fac, fac_cc_df = rez_df(fac_cc_df, loaded_batch, ndx_col)
            fac_cc_df.set_index('account_no', inplace=True)
            fac_cc_df['account_no'] = fac_cc_df.index
            return True, fac_cc_df
        else:
            fac_df, fac_rez, size2use = i2df(fac_type, data_size, dpid, loaded_batch, ndx_col)
            if fac_df is not None and not fac_df.empty:
                fac_df, size_done = conf_df(loaded_batch, mdjlog, fac_df, fac_rez)
                mdjlog.info(
                    "got! #{} of {} Counts: {} of {} of {}".format('1', crounds, size2use, size_done, data_size))
                # fac, fac_df = rez_df(fac_df, loaded_batch, ndx_col)
                return True, fac_df
            else:
                mdjlog.warn('no FACILITY data . ..')
    except Exception as e:
        mdjlog.error(e)
    return False, None


def ndvdl_data(dpid, loaded_batch):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type = 'individual_submission'
        type_count = data_doc_type.split('_')[0]
        data_doc_type, ndvdl_cc_df, ndx_col = 'ndvdl', None, 'cust_id'
        if not type_count in loaded_batch:
            loaded_batch = [dbp for dbp in
                            DataBatchProcess.objects(cycle_ver=loaded_batch['cycle_ver'], dpid=dpid, status='Loaded') if
                            any(x in dbp['segments'] for x in ns)][0]
        data_size = int(loaded_batch[type_count])
        rounds, size_done = data_size // split_var + 1, 0
        crounds, size2use = 1 if rounds is (0 or 1) else rounds - 1, round(data_size / rounds)
        if data_size > split_var:
            for c in range(rounds):
                try:
                    size2use = size2use if data_size - size_done > size2use else data_size - size_done
                    ndvdl_df, ndvdl_rez, size = i2df(data_doc_type, size2use, dpid, loaded_batch, ndx_col)
                    if ndvdl_df is not None and not ndvdl_df.empty:
                        ndvdl_df, size = conf_df(loaded_batch, mdjlog, ndvdl_df, ndvdl_rez)
                        if ndvdl_cc_df is None:
                            ndvdl_cc_df = ndvdl_df
                        else:
                            ndvdl_cc_df = pd.concat([ndvdl_cc_df, ndvdl_df])
                    else:
                        mdjlog.warn('no INDIVIDUAL data . ..')
                    size_done += size2use
                    mdjlog.info(
                        "got! #{} of {} Counts: {} of {} of {}".format(c, crounds, size2use, size_done, data_size))
                except Exception as e:
                    mdjlog.error(e)
            ndvdl_cc_df = ndvdl_cc_df[ndvdl_cc_df.submission == 'individual']
            return True, ndvdl_cc_df
        
        else:
            ndvdl_df, ndvdl_rez, size2use = i2df(data_doc_type, data_size, dpid, loaded_batch, ndx_col)
            if ndvdl_df is not None and not ndvdl_df.empty:
                ndvdl_df, size_done = conf_df(loaded_batch, mdjlog, ndvdl_df, ndvdl_rez)
                mdjlog.info(
                    "got! #{} of {} Counts: {} of {} of {}".format('1', crounds, size2use, size_done, data_size))
                ndvdl_df = ndvdl_df[ndvdl_df.submission == 'individual']
                return True, ndvdl_df
            else:
                mdjlog.warn('no INDIVIDUAL data . ..')
    
    except Exception as e:
        mdjlog.error(e)
    return False, None


def fix_str(x):
    if x and str(x).isalnum():
        return x
    if x and (str(x).replace(' ', '').isdigit() or str(x).replace(' ', '').isdecimal()):
        try:
            return str(x).replace(' ', '')
        except Exception as e:
            mdjlog.error(e)
            return '' if (str(x).replace(' ', '')).isalpha() else x
    return ''


def df_rounds(args: list):
    crounds, data_size, dpid, cc_df, type, loaded_batch, mdjlog, ndx_col, rounds, size2use, size_done = args
    for c in range(rounds):
        try:
            size2use = size2use if data_size - size_done > size2use else data_size - size_done
            cc_df = df_round(cc_df, dpid, loaded_batch, mdjlog, ndx_col, size2use, type)
            size_done += size2use
            mdjlog.info(f"getting! #{c} of {crounds} Counts: {size2use} of {size_done} of {data_size}")
        except Exception as e:
            mdjlog.error(e)
    return cc_df


def df_round(cc_df, dpid, loaded_batch, mdjlog, ndx_col, size2use, type):
    df, rez, size = i2df(type, size2use, dpid, loaded_batch, ndx_col)
    if df is not None and not df.empty:
        df, size = conf_df(loaded_batch, mdjlog, df, rez)
        cc_df = pd.concat([cc_df, df]) if cc_df else df
    else:
        mdjlog.warn(f'no {type.upper()} data . ..')
    return cc_df


def grntr_data(dpid: str, loaded_batch, grntr_type: str = None):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type, df, grntr_cc_df, ndx_col = 'guarantors', None, None, 'account_no'
        data_size = int(loaded_batch[data_doc_type])
        rounds, size_done = data_size // split_var + 1, 0
        crounds, size2use = 1 if rounds is (0 or 1) else rounds - 1, round(data_size / rounds)

        if data_size > split_var:
            grntr_args = (crounds, data_size, dpid, grntr_cc_df, grntr_type, loaded_batch, mdjlog, ndx_col, rounds,
                      size2use, size_done,)
            df = df_rounds(grntr_args)
        else:
            df = df_round(df, dpid, loaded_batch, mdjlog, ndx_col, size2use, grntr_type)

        grntr_ndx_flds = ['cycle_ver', 'dpid', 'account_no', 'grntr_type', 'biz_name', 'biz_reg_no',
                          'last_name', 'first_name', 'national_id_no', 'drivin_license_no', 'bvn', 'i_pass_no', ]
        re_ndx_flds(df, grntr_ndx_flds)
        df.fillna('', inplace=True)
        df.loc[:, '_id'] = df[grntr_ndx_flds].apply(lambda x: prep_grntr_id(x), axis=1)
        df.set_index('_id', inplace=True)
        return True, df
    except Exception as e:
        mdjlog.error(e)
    return False, None


def prnc_data(dpid: str, loaded_batch, prnc_type: str = None):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type, df, grntr_cc_df, ndx_col = 'guarantors', None, None, 'account_no'

        data_size = int(loaded_batch[data_doc_type])
        rounds, size_done = data_size // split_var + 1, 0
        crounds, size2use = 1 if rounds is (0 or 1) else rounds - 1, round(data_size / rounds)
        grntr_args = (crounds, data_size, dpid, grntr_cc_df, prnc_type, loaded_batch, mdjlog, ndx_col, rounds,
                      size2use, size_done,)
        df = df_rounds(grntr_args) if (data_size > split_var
                                       ) else df_round(df, dpid, loaded_batch, mdjlog, ndx_col, size2use, prnc_type)

        grntr_ndx_flds = ['cycle_ver', 'dpid', 'account_no', 'grntr_type', 'biz_name', 'biz_reg_no',
            'last_name', 'first_name', 'national_id_no', 'drivin_license_no', 'bvn', 'i_pass_no', ]
        re_ndx_flds(df, grntr_ndx_flds)
        df.fillna('', inplace=True)
        df.loc[:, '_id'] = df[grntr_ndx_flds].apply(lambda x: prep_grntr_id(x), axis=1)
        df.set_index('_id', inplace=True)
        return True, df
    except Exception as e:
        mdjlog.error(e)
    return False, None
