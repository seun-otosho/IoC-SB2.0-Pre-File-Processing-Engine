import json

import pandas as pd
from elasticsearch import helpers
from pandasticsearch import Select

from IoCEngine.commons import cs, ns, fs, iff_sb2_modes
from IoCEngine.config.pilot import chunk_size as split_var, es, es_i
from IoCEngine.logger import get_logger
from IoCEngine.utils.file import DataBatchProcess

mdjlog = get_logger('jarvis')


# split_var = 45788


def confirm_settings(data_size):
    put = es.indices.put_settings(
        index=es_i,
        body='''
            {
                "max_result_window" : {} 
            }
        '''.format(data_size),
        ignore_unavailable=True
    )

    if 'acknowledged' not in put or put['acknowledged'] != True:
        # raise Exception('Failed to update index: %s\n\n%s' % (es_i, json.dumps(put)))
        mdjlog.error('Failed to update index: %s\n\n%s' % (es_i, json.dumps(put)))


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
        # pym_db.combined_submissions.find(
        #     {
        #         "dpid": dpid, "cycle_ver": loaded_batch['cycle_ver'], "status": "Loaded"
        #     })  # "batch_no": loaded_batch['batch_no'],
        combo_df = Select.from_dict(combo_rez).to_pandas()
        combo_df.fillna('', inplace=True)
        if not combo_df.empty:
            del combo_df['_id'], combo_df['batch_no'], combo_df['cycle_ver'], combo_df['dpid'], combo_df['status']
            del combo_df['_score'], combo_df['_index'], combo_df['_type']
            mdjlog.info(combo_df.memory_usage(deep=True).sum())
            # combo, combo_df = rez_df(combo_df, loaded_batch, 'account_no')
            # minify(fac_df)
            mdjlog.info(
                "Combo Data Shape {} using memory: {}".format(combo_df.shape, combo_df.memory_usage(deep=True).sum()))
            return True, combo_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


def conf_df(loaded_batch, mdjlog, df, rez):
    if df is not None and not df.empty:
        del df['_id'], df['batch_no'], df['cycle_ver'], df['dpid'], df['status']
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


"""
def fire_forge(fac_df, loaded_batch, sbjt, sbjt_df, ctgry_dtls, dp_meta):
    try:
        # fac_df = fac_vals(loaded_batch, fac_df)
        # sbjt_df = ndvdl_vals(loaded_batch, sbjt_df)
        syndic8data(fac_df, sbjt_df, loaded_batch, ctgry_dtls, sbjt, dp_meta)
    except Exception as e:
        mdjlog.error(e)
"""


def i2df(data_doc_type, data_size, dpid, loaded_batch, ndx_col):
    rez = es.search(index=es_i, doc_type=data_doc_type,
                    body={"query": {"bool": {"must": [{"match": {'dpid': dpid}},
                                                      {"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                      {"match": {'status': 'Loaded'}}
                                                      ]
                                             }
                                    }
                          }
                    , size=data_size, from_=0)

    df = Select.from_dict(rez).to_pandas()
    if df is not None and not df.empty:
        df.fillna('', inplace=True)
        ndx_col = 'account_no' if loaded_batch['in_mod'] in iff_sb2_modes else ndx_col
        upd8DFstatus(data_doc_type, df, ndx_col, 'Pick3D')
    return df, rez, data_size


def upd8DFstatus(data_doc_type, df, index_col, status):
    bulk_upd8_data = []
    for rw in df.itertuples():
        rwd = rw._asdict()
        data_line = {'status': status}
        ndx_line = {'_index': es_i, '_op_type': 'update', '_type': data_doc_type,
                    '_id': "-".join((rwd['dpid'], str(rwd[index_col]).strip(), str(rwd['cycle_ver']))),
                    'doc': data_line}
        bulk_upd8_data.append(ndx_line)
    try:
        helpers.bulk(es, bulk_upd8_data)
    except Exception as e:
        mdjlog.error(e)


def corp_data(dpid, loaded_batch):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type, ndx_col = 'corporate_submissions', 'cust_id'
        count_type = data_doc_type.split('_')[0]
        if not count_type in loaded_batch:
            loaded_batch = [dbp for dbp in
                            DataBatchProcess.objects(cycle_ver=loaded_batch['cycle_ver'], dpid=dpid, status='Loaded') if
                            any(x in dbp['segments'] for x in cs)][0]
        data_size = int(loaded_batch[count_type])
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
            # corp, corp_cc_df = rez_df(corp_cc_df, loaded_batch, ndx_col)
            return True, corp_cc_df
        else:
            corp_df, corp_rez, size2use = i2df(data_doc_type, data_size, dpid, loaded_batch, ndx_col)
            if corp_df is not None and not corp_df.empty:
                corp_df, size_done = conf_df(loaded_batch, mdjlog, corp_df, corp_rez)
                mdjlog.info(
                    "getting! #{} of {} Counts: {} of {} of {}".format('1', crounds, size2use, size_done, data_size))
                # corp, corp_df = rez_df(corp_df, loaded_batch, ndx_col)
                return True, corp_df
            else:
                mdjlog.warn('no CORPORATE data . ..')
    except Exception as e:
        mdjlog.error(e)
    return False, None


"""
def conf_corp_df(corp_df, corp_rez, loaded_batch, mdjlog):
    del corp_df['_id'], corp_df['batch_no'], corp_df['cycle_ver'], corp_df['dpid'], corp_df['status']
    del corp_df['_score'], corp_df['_index'], corp_df['_type'], corp_df['data_file']  # , corp_df['data_file']
    mdjlog.info(corp_df.memory_usage(deep=True).sum())
    corp, corp_df = rez_df(corp_df, loaded_batch, 'cust_id')
    # minify(corp_df)
    df_shape = corp_df.shape
    mdjlog.info(
        "Rez len: {} | Data Shape {} using memory: {}".format(len(corp_rez['hits']['hits']), df_shape,
                                                              corp_df.memory_usage(deep=True).sum()))
    return corp, corp_df, df_shape[0]
"""
"""
def conf_ndvdl_df(loaded_batch, mdjlog, ndvdl_df, ndvdl_rez):
    del ndvdl_df['_id'], ndvdl_df['batch_no'], ndvdl_df['cycle_ver'], ndvdl_df['dpid'], ndvdl_df['status']
    del ndvdl_df['_score'], ndvdl_df['_index'], ndvdl_df['_type']
    # mdjlog.info(ndvdl_df.memory_usage(deep=True).sum())
    ndvdl, ndvdl_df = rez_df(ndvdl_df, loaded_batch, 'cust_id')
    # minify(ndvdl_df)
    df_shape = ndvdl_df.shape
    mdjlog.info(
        "Rez len: {} | Data Shape {} using memory: {}".format(len(ndvdl_rez['hits']['hits']), df_shape,
                                                              ndvdl_df.memory_usage(deep=True).sum()))
    return ndvdl, ndvdl_df, df_shape[0]
"""

"""
def corpDF(data_doc_type, data_size, dpid, loaded_batch):
    corp_rez = es.search(index=es_i, doc_type=data_doc_type,
                         body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                           {"match": {'dpid': dpid}},
                                                           {"match": {'status': 'Loaded'}}
                                                           ]
                                                  }
                                         }
                               }
                         , size=data_size, from_=0)
    corp_df = Select.from_dict(corp_rez).to_pandas()
    if corp_df is not None and not corp_df.empty: corp_df.fillna('', inplace=True)
    return corp_df, corp_rez, data_size

"""


def fac_data(dpid, loaded_batch):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type, fac_cc_df, ndx_col = 'facility_submissions', None, 'account_no'
        count_type = data_doc_type.split('_')[0]
        if not count_type in loaded_batch:
            loaded_batch = [dbp for dbp in
                            DataBatchProcess.objects(cycle_ver=loaded_batch['cycle_ver'], dpid=dpid, status='Loaded') if
                            any(x in dbp['segments'] for x in fs)][0]
        data_size = int(loaded_batch[data_doc_type.split('_')[0]])
        rounds, size_done = data_size // split_var + 1, 0
        crounds, size2use = 1 if rounds is (0 or 1) else rounds - 1, round(data_size / rounds)
        if data_size > split_var:
            for c in range(rounds):
                try:
                    size2use = size2use if data_size - size_done > size2use else data_size - size_done
                    fac_df, fac_rez, size = i2df(data_doc_type, size2use, dpid, loaded_batch, ndx_col)
                    if fac_df is not None and not fac_df.empty:
                        fac_df, size = conf_df(loaded_batch, mdjlog, fac_df, fac_rez)
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
            return True, fac_cc_df
        else:
            fac_df, fac_rez, size2use = i2df(data_doc_type, data_size, dpid, loaded_batch, ndx_col)
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
        data_doc_type, ndvdl_cc_df, ndx_col = 'individual_submissions', None, 'cust_id'
        count_type = data_doc_type.split('_')[0]
        if not count_type in loaded_batch:
            loaded_batch = [dbp for dbp in
                            DataBatchProcess.objects(cycle_ver=loaded_batch['cycle_ver'], dpid=dpid, status='Loaded') if
                            any(x in dbp['segments'] for x in ns)][0]
        data_size = int(loaded_batch[count_type])
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

            # ndvdl, ndvdl_cc_df = rez_df(ndvdl_cc_df, loaded_batch, ndx_col)
            return True, ndvdl_cc_df

        else:
            ndvdl_df, ndvdl_rez, size2use = i2df(data_doc_type, data_size, dpid, loaded_batch, ndx_col)
            if ndvdl_df is not None and not ndvdl_df.empty:
                ndvdl_df, size_done = conf_df(loaded_batch, mdjlog, ndvdl_df, ndvdl_rez)
                mdjlog.info(
                    "got! #{} of {} Counts: {} of {} of {}".format('1', crounds, size2use, size_done, data_size))
                # ndvdl, ndvdl_df = rez_df(ndvdl_df, loaded_batch, ndx_col)
                return True, ndvdl_df
            else:
                mdjlog.warn('no INDIVIDUAL data . ..')

    except Exception as e:
        mdjlog.error(e)
    return False, None


"""
def ndvdlDF(data_doc_type, data_size, dpid, loaded_batch):
    ndvdl_rez = es.search(index=es_i, doc_type=data_doc_type,
                          body={"query": {"bool": {"must": [{"match": {'dpid': dpid}},
                                                            {"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                            {"match": {'status': 'Loaded'}}
                                                            ]
                                                   }
                                          }
                                }
                          , size=data_size, from_=0)
    #
    ndvdl_df = Select.from_dict(ndvdl_rez).to_pandas()
    if ndvdl_df is not None and not ndvdl_df.empty: ndvdl_df.fillna('', inplace=True)
    return ndvdl_df, ndvdl_rez, data_size
"""

"""
def rez_df(df, loaded_batch, data_type):
    dp_name, duplicated = loaded_batch['dp_name'], None
    mdjlog = get_logger(dp_name)
    mdjlog.info("Resolving {} with shape {} by {}".format(loaded_batch, df.shape, data_type))
    df[data_type].apply(fix_str)
    if loaded_batch['in_mod'] in iff_sb2_modes:
        data_type = 'account_no'
    duplicated = df[data_type].duplicated(keep='first')

    dup_df = df[duplicated]
    if not dup_df.empty:
        mdjlog.info("Resolv1nG Duplic8d with shape {} by {}".format(dup_df.shape, data_type))
        try:
            unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed' + os.sep)
            dup_df.to_excel(
                str(os.sep).join((unprocessed, '_'.join(
                    (dp_name, str(loaded_batch['cycle_ver']), datetime.now().strftime("%d%I%M%S"),
                     'duplicated_{}.xlsx'.format(data_type))))),
                index=False)
            del dup_df
        except Exception as e:
            mdjlog.error(e)

        ret_df = df[~duplicated]
    else:
        ret_df = df

    mdjlog.info("Returning Resolv3D {} with shape {} by {}".format(loaded_batch, ret_df.shape, data_type))
    return True, ret_df
"""


def fix_str(x):
    if x and str(x).isalnum():
        return x
    if x and (str(x).replace(' ', '').isdigit() or str(x).replace(' ', '').isdecimal()):
        try:
            return str(x).replace(' ', '')
        except Exception as e:
            mdjlog.error(e)
            return '' if (str(x).replace(' ', '')).isalpha() else x
    if x and str(x).isalpha():
        return ''
    else:
        return ''
