import json
import os
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from multiprocessing import Pool, Process

import pandas as pd
from elasticsearch import Elasticsearch, helpers
from pandasticsearch import Select

from IoCEngine.SHU.trans4mas import fac_vals, ndvdl_vals
from IoCEngine.commons import count_down, g_meta, mk_dp_x_dir
from IoCEngine.config.pilot import es, es_i, mpcores
from IoCEngine.cores import ppns
from IoCEngine.logger import get_logger
from IoCEngine.utils.sb2 import syndic8data

# es = Elasticsearch(timeout=100, max_retries=10, retry_on_timeout=True)  # ['172.16.2.17'],
#
# es_i = "ioc"

mdjlog = get_logger('jarvis')

split_var = 45788

"""
rez = es.search(index="raw_data", body={"query": {
    "bool" : {"must" : [
        {"match": {'dpid':'1A0L100058'}},
        {"match": {'status': 'Loaded'}}
    ]
             }
                                           }})

df = Select.from_dict(rez).to_pandas()
"""


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
        dat_cat, data_type = data_doc_type.split('_')
        data_size = int(loaded_batch[data_doc_type.split('_')[0]])
        # confirm_settings()
        combo_rez = es.search(index=es_i, doc_type=data_type,
                              body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                                {"match": {'dpid': dpid}},
                                                                {"match": {'status': 'Loaded'}},
                                                                {"match": {'category': dat_cat}}
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
            combo, combo_df = rez_df(combo_df, loaded_batch, xtrcxn_zone, 'account_no')
            # minify(fac_df)
            mdjlog.info(
                "Combo Data Shape {} using memory: {}".format(combo_df.shape, combo_df.memory_usage(deep=True).sum()))
            return combo, combo_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


def fac_data_split(dpid, loaded_batch, xtrcxn_zone, sbjt_data, sbjt):
    dp_name = loaded_batch['dp_name']
    mdjlog = get_logger(dp_name)
    datCat, data_size = sbjt, 0
    ctgry_dtls, dp_meta = g_meta(datCat, dp_name, loaded_batch)
    try:
        data_doc_type = 'facility_submissions'
        dat_cat, data_type = data_doc_type.split('_')
        if not loaded_batch[data_doc_type.split('_')[0]]:
            rez = es.search(index=es_i, doc_type=data_type,
                            body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                              {"match": {'dpid': dpid}},
                                                              {"match": {'status': 'Loaded'}},
                                                              {"match": {'category': dat_cat}}
                                                              ]
                                                     }
                                            }
                                  })
            if 'hits' in rez.keys():
                data_size = int(rez['hits']['total'])
        else:
            data_size = int(loaded_batch[data_doc_type.split('_')[0]])

        chunk_pro_list, runnin_chunks_list = [], []

        if data_size > split_var:
            rounds, size_done = data_size // split_var + 1, 0
            size2use = round(data_size / rounds)
            for c in range(rounds):
                try:
                    size2use = size2use if data_size - size_done > size2use else data_size - size_done
                    fac_df, fac_rez = facDF(data_doc_type, size2use, dpid, loaded_batch)
                    if not fac_df.empty:
                        fac, fac_df = conf_fac_df(loaded_batch, mdjlog, fac_df, fac_rez, xtrcxn_zone)
                        size_done += size2use
                        mdjlog.info(
                            "QueuinG! #{} of {} Counts: {} of {} of {}".format(c, rounds - 1, size2use, size_done,
                                                                               data_size))
                        # filter sbjt_data down to current fac_df
                        # pass it on
                        ndx_column = 'cust_id' if loaded_batch['in_mod'] in ('cdt', 'udf',) else 'account_no'
                        sbjt_df = sbjt_data[sbjt_data[ndx_column].isin(fac_df[ndx_column])]
                        fac_df = ppns(fac_vals, fac_df, loaded_batch, True)
                        # fac_df = fac_df[fac_df[ndx_column].isin(sbjt_df[ndx_column])]
                        runnin_chunks_list.append((fac_df, loaded_batch, sbjt, sbjt_df, ctgry_dtls, dp_meta))
                        # p = Process(target=fire_forge, args=(fac_df, loaded_batch, sbjt, sbjt_df, ctgry_dtls, dp_meta),
                        #             name='{}-{}-CHNK3D@{}'.format(dp_name, loaded_batch['data_type'], c))
                        # mdjlog.info(p)
                        # p.start()
                        # mdjlog.info(p)
                        # chunk_pro_list.append(p)
                        # fire_forge(fac_df, loaded_batch, sbjt, sbjt_df, ctgry_dtls, dp_meta)
                    else:
                        mdjlog.warn('no data. ..')
                except Exception as e:
                    mdjlog.error(e)

            pool = Pool(processes=mpcores)
            pool.apply_async(syndic8data, runnin_chunks_list)

            # for rc in runnin_chunks_list:
            #     # syndic8data(fac_df, sbjt_df, loaded_batch, ctgry_dtls, sbjt, dp_meta)
            #     fac_df, loaded_batch, sbjt, sbjt_df, ctgry_dtls, dp_meta = rc[0], rc[1], rc[2], rc[3], rc[4], rc[5]
            #     sbm3Data = "individual" if sbjt is 'con' else 'corporate'
            #     mdjlog.info("\n{} facility counts is {}\n{} subject count is {}".format(
            #         sbm3Data, fac_df.shape[0], sbm3Data, sbjt_df.shape[0]))
            #     p = Process(target=syndic8data, args=(fac_df, sbjt_df, loaded_batch, ctgry_dtls, sbjt, dp_meta),
            #                 name='{}-{}-CHNK3D@{}'.format(dp_name, loaded_batch['data_type'], c))
            #     mdjlog.info(p)
            #     p.start()
            #     mdjlog.info(p)
            #     chunk_pro_list.append(p)
            #     count_down(None, 5)

            # if len(chunk_pro_list) > 1:
            #     for p, rp in enumerate(chunk_pro_list):
            #         mdjlog.info("Process @# {}${}".format(p, os.getpid()))
            #         mdjlog.info(rp)
            #         # rp.close()
            #         rp.join()
            #         mdjlog.info(rp)
            #         rp.terminate()
            # return False, None
        else:
            fac_df, fac_rez = facDF(data_doc_type, data_size, dpid, loaded_batch)
            if not fac_df.empty:
                fac, fac_df = conf_fac_df(loaded_batch, mdjlog, fac_df, fac_rez, xtrcxn_zone)
                return fac, fac_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


def conf_fac_df(loaded_batch, mdjlog, fac_df, fac_rez, xtrcxn_zone):
    del fac_df['_id'], fac_df['batch_no'], fac_df['cycle_ver'], fac_df['dpid'], fac_df['status']
    del fac_df['_score'], fac_df['_index'], fac_df['_type']
    mdjlog.info(fac_df.memory_usage(deep=True).sum())
    fac, fac_df = rez_df(fac_df, loaded_batch, xtrcxn_zone, 'account_no')
    # minify(fac_df)
    mdjlog.info(
        "Rez len: {} | Data Shape {} using memory: {}".format(len(fac_rez['hits']['hits']), fac_df.shape,
                                                              fac_df.memory_usage(deep=True).sum()))
    return fac, fac_df


def fire_forge(fac_df, loaded_batch, sbjt, sbjt_df, ctgry_dtls, dp_meta):
    try:
        # fac_df = fac_vals(loaded_batch, fac_df)
        # sbjt_df = ndvdl_vals(loaded_batch, sbjt_df)
        syndic8data(fac_df, sbjt_df, loaded_batch, ctgry_dtls, sbjt, dp_meta)
    except Exception as e:
        mdjlog.error(e)


"""
def fac_data(dpid, loaded_batch, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type = 'facility_submissions'
        data_size = int(loaded_batch[data_doc_type.split('_')[0]])
        if data_size > split_var:
            ndvdl_data(dpid, loaded_batch, xtrcxn_zone, True)
        else:
            fac_df, fac_rez = facDF(data_doc_type, data_size, dpid, loaded_batch)
            if not fac_df.empty:
                fac, fac_df = conf_fac_df(loaded_batch, mdjlog, fac_df, fac_rez, xtrcxn_zone)
                return fac, fac_df
    except Exception as e:
        mdjlog.error(e)
    return False, None
"""


def facDF(data_doc_type, data_size, dpid, loaded_batch):
    dat_cat, data_type = data_doc_type.split('_')
    fac_rez = es.search(index=es_i, doc_type=data_doc_type,
                        body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                          {"match": {'dpid': dpid}},
                                                          {"match": {'status': 'Loaded'}},
                                                          {"match": {'category': dat_cat}}
                                                          ]
                                                 }
                                        }
                              }
                        , size=data_size, from_=0)
    #
    fac_df = Select.from_dict(fac_rez).to_pandas()
    fac_df.fillna('', inplace=True)
    bulk_upd8_data = []
    index_col = 'account_no'  # if in_mod in ('cmb', 'fandl', 'iff', 'mfi', 'pmi') else 'cust_id'
    for rw in fac_df.itertuples():
        rwd = rw._asdict()
        data_line = {'status': 'Pair3D'}
        ndx_line = {'_index': es_i, '_op_type': 'update', '_type': data_doc_type,
                    '_id': "-".join((rwd['dpid'], str(rwd[index_col]).strip(), str(rwd['cycle_ver']))),
                    'doc': data_line}
        # upd8line = {'update': ndx_line}
        # bulk_upd8_data.append(upd8line)
        # upd8data = {'script': 'ctx._source.count += count', 'params': data_line}
        bulk_upd8_data.append(ndx_line)
    helpers.bulk(es, bulk_upd8_data)
    return fac_df, fac_rez


def corp_data(dpid, loaded_batch, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type = 'corporate_submissions'
        data_size = int(loaded_batch[data_doc_type.split('_')[0]])
        # confirm_settings()
        corp_df, corp_rez = corpDF(data_doc_type, data_size, dpid, loaded_batch)
        if not corp_df.empty:
            corp, corp_df = conf_corp_df(corp_df, corp_rez, loaded_batch, mdjlog, xtrcxn_zone)
            # mdjlog.info(corp_df.memory_usage(deep=True).sum())
            return corp, corp_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


def conf_corp_df(corp_df, corp_rez, loaded_batch, mdjlog, xtrcxn_zone):
    del corp_df['_id'], corp_df['batch_no'], corp_df['cycle_ver'], corp_df['dpid'], corp_df['status']
    del corp_df['_score'], corp_df['_index'], corp_df['_type'], corp_df['data_file']  # , corp_df['data_file']
    mdjlog.info(corp_df.memory_usage(deep=True).sum())
    data_type = 'account_no' if loaded_batch['in_mod'] in (
        'c0mf', 'cmb', 'fandl', 'iff', 'mfi', 'phed', 'pmi',) else 'cust_id'
    corp, corp_df = rez_df(corp_df, loaded_batch, xtrcxn_zone, data_type)
    # minify(corp_df)
    mdjlog.info(
        "Rez len: {} | Data Shape {} using memory: {}".format(len(corp_rez['hits']['hits']), corp_df.shape,
                                                              corp_df.memory_usage(deep=True).sum()))
    return corp, corp_df


def corpDF(data_doc_type, data_size, dpid, loaded_batch):
    dat_cat, data_type = data_doc_type.split('_')
    corp_rez = es.search(index=es_i, doc_type=data_type,
                         body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                           {"match": {'dpid': dpid}},
                                                           {"match": {'status': 'Loaded'}},
                                                           {"match": {'customer_type': dat_cat}}
                                                           ]
                                                  }
                                         }
                               }
                         , size=data_size, from_=0)
    #
    corp_df = Select.from_dict(corp_rez).to_pandas()
    corp_df.fillna('', inplace=True)
    return corp_df, corp_rez


def fac_data(dpid, loaded_batch, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type = 'facility_submissions'
        data_size = int(loaded_batch[data_doc_type.split('_')[0]])
        if data_size > split_var:
            fac_cc_df = None
            rounds, size_done = data_size // split_var + 1, 0
            size2use = round(data_size / rounds)
            for c in range(rounds):
                try:
                    size2use = size2use if data_size - size_done > size2use else data_size - size_done
                    fac_df, fac_rez = facDF(data_doc_type, size2use, dpid, loaded_batch)
                    if not fac_df.empty:
                        fac, fac_df = conf_fac_df(loaded_batch, mdjlog, fac_df, fac_rez, xtrcxn_zone)
                        if fac_cc_df is None:
                            fac_cc_df = fac_df
                        else:
                            fac_cc_df = pd.concat([fac_cc_df, fac_df])
                    size_done += size2use
                    mdjlog.info(
                        "getting! #{} of {} Counts: {} of {} of {}".format(c, rounds, size2use, size_done, data_size))
                except Exception as e:
                    mdjlog.error(e)

            # fac_cc_df = ppns(fac_vals, fac_cc_df, loaded_batch, True)
            # fac_data_split(dpid, loaded_batch, xtrcxn_zone, fac_cc_df, 'con')
            return True, fac_cc_df
        else:
            fac_df, fac_rez = facDF(data_doc_type, data_size, dpid, loaded_batch)
            if not fac_df.empty:
                fac, fac_df = conf_fac_df(loaded_batch, mdjlog, fac_df, fac_rez, xtrcxn_zone)
                return fac, fac_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


def ndvdl_data(dpid, loaded_batch, xtrcxn_zone, from_fac=False):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        data_doc_type = 'individual_submissions'
        dat_cat, data_type = data_doc_type.split('_')
        if from_fac:
            rez = es.search(index=es_i, doc_type=data_type,
                            body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                              {"match": {'dpid': dpid}},
                                                              {"match": {'status': 'Loaded'}},
                                                              {"match": {'customer_type': dat_cat}}
                                                              ]
                                                     }
                                            }
                                  })
            if 'hits' in rez.keys():
                if 'total' in rez['hits'].keys():
                    data_size = int(rez['hits']['total'])
                else:
                    mdjlog.warn("No Individual Subject Records Found for {}".format(loaded_batch))
        else:
            data_size = int(loaded_batch[data_doc_type.split('_')[0]])
        if data_size > split_var:
            ndvdl_cc_df = None
            rounds, size_done = data_size // split_var + 1, 0
            size2use = round(data_size / rounds)
            for c in range(rounds):
                try:
                    size2use = size2use if data_size - size_done > size2use else data_size - size_done
                    ndvdl_df, ndvdl_rez = ndvdlDF(data_doc_type, size2use, dpid, loaded_batch)
                    if not ndvdl_df.empty:
                        ndvdl, ndvdl_df = conf_ndvdl_df(loaded_batch, mdjlog, ndvdl_df, ndvdl_rez, xtrcxn_zone)
                        if ndvdl_cc_df is None:
                            ndvdl_cc_df = ndvdl_df
                        else:
                            ndvdl_cc_df = pd.concat([ndvdl_cc_df, ndvdl_df])
                    size_done += size2use
                    mdjlog.info(
                        "getting! #{} of {} Counts: {} of {} of {}".format(c, rounds, size2use, size_done, data_size))
                except Exception as e:
                    mdjlog.error(e)

            # ndvdl_cc_df = ppns(ndvdl_vals, ndvdl_cc_df, loaded_batch, True)
            # fac_data_split(dpid, loaded_batch, xtrcxn_zone, ndvdl_cc_df, 'con')
            return True, ndvdl_cc_df

        else:
            ndvdl_df, ndvdl_rez = ndvdlDF(data_doc_type, data_size, dpid, loaded_batch)
            if not ndvdl_df.empty:
                ndvdl, ndvdl_df = conf_ndvdl_df(loaded_batch, mdjlog, ndvdl_df, ndvdl_rez, xtrcxn_zone)
                return ndvdl, ndvdl_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


def conf_ndvdl_df(loaded_batch, mdjlog, ndvdl_df, ndvdl_rez, xtrcxn_zone):
    del ndvdl_df['_id'], ndvdl_df['batch_no'], ndvdl_df['cycle_ver'], ndvdl_df['dpid'], ndvdl_df['status']
    del ndvdl_df['_score'], ndvdl_df['_index'], ndvdl_df['_type']
    # mdjlog.info(ndvdl_df.memory_usage(deep=True).sum())
    data_type = 'cust_id' if loaded_batch['in_mod'] in ('cdt') else 'account_no'
    ndvdl, ndvdl_df = rez_df(ndvdl_df, loaded_batch, xtrcxn_zone, data_type)
    # minify(ndvdl_df)
    mdjlog.info(
        "Rez len: {} | Data Shape {} using memory: {}".format(len(ndvdl_rez['hits']['hits']), ndvdl_df.shape,
                                                              ndvdl_df.memory_usage(deep=True).sum()))
    return ndvdl, ndvdl_df


def ndvdlDF(data_doc_type, data_size, dpid, loaded_batch):
    dat_cat, data_type = data_doc_type.split('_')
    ndvdl_rez = es.search(index=es_i, doc_type=data_type,
                          body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                            {"match": {'dpid': dpid}},
                                                            {"match": {'status': 'Loaded'}},
                                                            {"match": {'category': dat_cat}}
                                                            ]
                                                   }
                                          }
                                }
                          , size=data_size, from_=0)
    #
    ndvdl_df = Select.from_dict(ndvdl_rez).to_pandas()
    ndvdl_df.fillna('', inplace=True)
    return ndvdl_df, ndvdl_rez


def rez_df(df, loaded_batch, xtrcxn_zone, data_type):
    dp_name, duplicated = loaded_batch['dp_name'], None
    mdjlog = get_logger(dp_name)
    mdjlog.info("Resolving {} with shape {}".format(loaded_batch, df.shape))
    df[data_type].apply(fix_str)
    duplicated = df[data_type].duplicated(keep='first')

    dup_df = df[duplicated]

    if not dup_df.empty:
        mdjlog.info("Resolv1nG Duplic8d with shape {}".format(dup_df.shape))
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

    mdjlog.info("Returning Resolv3D {} with shape {}".format(loaded_batch, ret_df.shape))

    return True, ret_df


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
