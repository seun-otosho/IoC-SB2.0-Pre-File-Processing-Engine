import os
from datetime import datetime
from elasticsearch import Elasticsearch
from pandasticsearch import Select

import pandas as pd

from IoCEngine.commons import mk_dp_x_dir
from IoCEngine.logger import get_logger
from IoCEngine.utils.file import pym_db

es = Elasticsearch()

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


def combo_data(dpid, loaded_batch, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        combo_rez = es.search(index="raw_data", doc_type='combined_submissions',
                              body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                                {"match": {'dpid': dpid}},
                                                                {"match": {'status': 'Loaded'}}
                                                                ]
                                                       }
                                              }
                                    }
                              )
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


def fac_data(dpid, loaded_batch, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        fac_rez = es.search(index="raw_data", doc_type='facility_submissions',
                            body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                              {"match": {'dpid': dpid}},
                                                              {"match": {'status': 'Loaded'}}
                                                              ]
                                                     }
                                            }
                                  }
                            )
        #
        # pym_db.facility_submissions.find(
        # {
        #     "dpid": dpid, "cycle_ver": loaded_batch['cycle_ver'], "status": "Loaded"
        # })
        #
        fac_df = Select.from_dict(fac_rez).to_pandas()
        fac_df.fillna('', inplace=True)
        if not fac_df.empty:
            del fac_df['_id'], fac_df['batch_no'], fac_df['cycle_ver'], fac_df['dpid'], fac_df['status']
            del fac_df['_score'], fac_df['_index'], fac_df['_type']
            mdjlog.info(fac_df.memory_usage(deep=True).sum())
            fac, fac_df = rez_df(fac_df, loaded_batch, xtrcxn_zone, 'account_no')
            # minify(fac_df)
            mdjlog.info("Data Shape {} using memory: {}".format(fac_df.shape, fac_df.memory_usage(deep=True).sum()))
            # valid8data
            # over_dues(fac_df)
            return fac, fac_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


def corp_data(dpid, loaded_batch, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        corp_rez = es.search(index="raw_data", doc_type='corporate_submissions',
                             body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                               {"match": {'dpid': dpid}},
                                                               {"match": {'status': 'Loaded'}}
                                                               ]
                                                      }
                                             }
                                   }
                             )
        #
        # pym_db.corporate_submissions.find(
        #     {"dpid": dpid, "cycle_ver": loaded_batch['cycle_ver'], "status": "Loaded"}
        # )
        #
        corp_df = Select.from_dict(corp_rez).to_pandas()
        corp_df.fillna('', inplace=True)
        if not corp_df.empty:
            del corp_df['_id'], corp_df['batch_no'], corp_df['cycle_ver'], corp_df['dpid'], corp_df['status']
            del corp_df['_score'], corp_df['_index'], corp_df['_type'], corp_df['data_file'], corp_df['data_file']
            mdjlog.info(corp_df.memory_usage(deep=True).sum())
            data_type = 'account_no' if loaded_batch['in_mod'] in (
                'c0mf', 'cmb', 'fandl', 'iff', 'mfi', 'phed', 'pmi',) else 'cust_id'
            corp, corp_df = rez_df(corp_df, loaded_batch, xtrcxn_zone, data_type)
            # minify(corp_df)
            mdjlog.info("Data Shape {} using memory: {}".format(corp_df.shape, corp_df.memory_usage(deep=True).sum()))
            # mdjlog.info(corp_df.memory_usage(deep=True).sum())
            return corp, corp_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


def ndvdl_data(dpid, loaded_batch, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    try:
        ndvdl_rez = es.search(index="raw_data", doc_type='individual_submissions',
                              body={"query": {"bool": {"must": [{"match": {'cycle_ver': loaded_batch['cycle_ver']}},
                                                                {"match": {'dpid': dpid}},
                                                                {"match": {'status': 'Loaded'}}
                                                                ]
                                                       }
                                              }
                                    }
                              )
        #
        # pym_db.individual_submissions.find(
        #     {"dpid": dpid, "cycle_ver": loaded_batch['cycle_ver'], "status": "Loaded"}
        # )
        #
        ndvdl_df = Select.from_dict(ndvdl_rez).to_pandas()
        ndvdl_df.fillna('', inplace=True)
        if not ndvdl_df.empty:
            del ndvdl_df['_id'], ndvdl_df['batch_no'], ndvdl_df['cycle_ver'], ndvdl_df['dpid'], ndvdl_df['status']
            del ndvdl_df['_score'], ndvdl_df['_index'], ndvdl_df['_type']
            mdjlog.info(ndvdl_df.memory_usage(deep=True).sum())
            ndvdl, ndvdl_df = rez_df(ndvdl_df, loaded_batch, xtrcxn_zone, 'cust_id')
            # minify(ndvdl_df)
            mdjlog.info("Data Shape {} using memory: {}".format(ndvdl_df.shape, ndvdl_df.memory_usage(deep=True).sum()))
            return ndvdl, ndvdl_df
    except Exception as e:
        mdjlog.error(e)
    return False, None


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
        return str(int(x))
    if x and str(x).isalpha():
        return ''
    else:
        return ''
