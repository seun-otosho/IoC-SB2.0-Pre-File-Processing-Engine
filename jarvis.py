#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Created 2016

@author: tolorun
"""


import os
from itertools import chain

from IoCEngine import drop_zone
# from IoCEngine.SHU.categorize import minify
from IoCEngine.SHU.trans4mas import corp_vals, ndvdl_vals, fac_vals
from IoCEngine.celeryio import app
from IoCEngine.commons import (all_all_modes, all_corp_modes, all_ndvdl_modes, cf, cs, fs, nf, ns, getID,
                               mk_dir, mk_dp_x_dir, cdt_udf_modes, sngl_sbjt_in_modes, sgmnt_def, g_meta, gs, ps)
from IoCEngine.config.pilot import chunk_size as split_var
from IoCEngine.cores import ppns
from IoCEngine.logger import get_logger
from IoCEngine.utils.db2data import combo_data, fac_data, corp_data, ndvdl_data, upd8DFstatus
from IoCEngine.utils.file import DataFiles, DataBatchProcess, dict_file, xtrct_file_details
from IoCEngine.utils.file_reader import xtrct_all_data, xtrct_ff_data, xtrct_ws_data
from IoCEngine.utils.sb2 import syndic8data
# client = Executor()
from auto_bots import upd8batch

mdjlog = get_logger('jarvis')
load3Dbatch = None
# code keep
# and any(x in fs for x in loaded_batch['segments'])

mk_dir(drop_zone)


@app.task(name='route_file')
def route_file(file_data):
    mdjlog = get_logger(file_data['dp_name'])
    if isinstance(file_data, str):
        file_data = DataFiles.objects.get(id=file_data)
    # data_pro_bat_no = False
    try:
        file_name = file_data['file_name']
        try:
            try:
                if file_data['xtra_nfo'].lower() == "data-loaded":
                    pass
            except Exception as e:
                mdjlog.error(e)
            if '_all' in file_name:
                file_data.update(batch_no=getID())
                xtrct_all_data(file_data.reload())  # data_pro_bat_no =
            elif file_data['data_type'] in cs + fs + gs + ns + ps + ('combo',):
                if file_name.lower().endswith(('.csv', '.txt')):
                    xtrct_ff_data(file_data)  # data_pro_bat_no =
                    # get_file_set(file_data)  # get other files in set
                if file_name.lower().endswith(('.xls', '.xlsx', )):
                    xtrct_ws_data(file_data)  # data_pro_bat_no =
                    # get_file_set(file_data)  # get other files in set
                    # mdjlog.error("""\nHi\nWell done\nThe extension .xls for file {} is no longer supported.
                    #         Please save the file with .xlsx and let's do this again.\nThank you""".format(file_name))
        except Exception as e:
            mdjlog.error(e)
        return
    except Exception as e:
        mdjlog.error(e)
    return


@app.task(name='route_filed_data')
def route_filed_data(file_data):
    cycle_ver, dpid = file_data["cycle_ver"], file_data["dpid"]
    dp_name, load3Dbatch, loaded_batches = '', None, DataBatchProcess.objects(
        cycle_ver=cycle_ver, dpid=dpid, status='Loaded')  # .first()
    loaded_batch = [ld for ld in loaded_batches if ld.data_type == file_data.data_type][0]
    combo, combo_df, corp, corp_df, corpfac, corpfac_df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df = re_init_vars()
    Bs2u, mdjlog, syndibatch = [], get_logger(loaded_batches[0]['dp_name']), getID()
    load3DSegments = list(chain.from_iterable([lb['segments'] for lb in loaded_batches]))
    load3DSgmnts = []
    for sgmnt in load3DSegments:
        load3DSgmnts.append(sgmnt_def[sgmnt]) if sgmnt in sgmnt_def.keys() else None
        if sgmnt in ('allcorp',): load3DSgmnts.append('corporate'), load3DSgmnts.append('facility')
        if sgmnt in ('allndvdl',): load3DSgmnts.append('facility'), load3DSgmnts.append('individual')
    
    dp_name, xtrcxn_zone = loaded_batch['dp_name'], mk_dp_x_dir(loaded_batch['dp_name'].upper())
    if loaded_batch['data_type'] in sngl_sbjt_in_modes:
        for loaded_batch in loaded_batches:
            try:
                if loaded_batch['status'] == 'Loaded':
                    load3Dbatch = loaded_batch
                    load3Dbatch['load3DSegments'] = list(set(load3DSgmnts))
                    if not (any(x in fs for x in load3DSegments) or not any(x in cs + ns for x in load3DSegments)
                    or not any(x in gs + ps for x in load3DSegments)):
                        mdjlog.info("Not enough data matching data segment to process submission for data provider")
                        return
                    Bs2u.append(load3Dbatch)
            
            except Exception as e:
                mdjlog.error(e)
        if any(b.facility > 90000 for b in loaded_batches if 'facility' in b):
            pass  # todo pass on to handle large files
        
        if 'facility' in loaded_batch and loaded_batch['facility'] > split_var * 3:
            rez_chunk_synd(corpfac, dpid, loaded_batch, load3DSegments, Bs2u)
            mdjlog.info("\n#E$3D")
            return
        else:
            try:
                corp, corp_df, corpfac, corpfac_df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df = rez_single_data(
                    corp, corpfac, ndvdl, ndvdlfac, dpid, loaded_batch, load3DSegments)
            except Exception as e:
                mdjlog.error(e)
    else:
        load3Dbatch = loaded_batch
        if loaded_batch['data_type'] in all_all_modes:
            # check list of loaded segments
            Bs2u.append(loaded_batch)
            try:
                corp, corp_df, corpfac, corpfac_df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df = rez_combined_data(
                    corp, corpfac, dpid, loaded_batch, ndvdl, ndvdlfac)
            except Exception as e:
                mdjlog.error(e)
        if loaded_batch['data_type'] in ('combo',):
            # check list of loaded segments
            Bs2u.append(loaded_batch)
            combo, combo_df = rez_combo_data(combo, combo_df, dpid, loaded_batch)
    ndx_column = 'account_no' if load3Dbatch['in_mod'] not in cdt_udf_modes else 'cust_id'
    
    if combo:
        try:
            # corp_vals(load3Dbatch, corp2df, )
            combo_df = ppns(ndvdl_vals, combo_df, load3Dbatch, True)
            # fac_vals(load3Dbatch, corpfac2df, )
            combo_df = ppns(fac_vals, combo_df, load3Dbatch, True)
            # todo del corp_df, corpfac_df, fac_df
            mdjlog.info("\ncombined data count is {}".format(combo_df.shape[0]))
            # count_down(None, 10)
            syndifiles = syndic8data(combo_df, None, load3Dbatch, 'con')
            
            upd8batch(load3Dbatch, Bs2u)  # , syndifiles
        except Exception as e:
            mdjlog.error(e)
    
    if corp and ndvdl:
        # filter overlapping customers
        # corp_df, ndvdl_df = rez_inter_customers(load3Dbatch, corp_df, ndvdl_df, )
        mdjlog.info('passing. ..')
    
    if corpfac and ndvdlfac:
        # filter overlapping facilities
        # corpfac_df, ndvdlfac_df = rez_inter_facilities(load3Dbatch, corpfac_df, ndvdlfac_df, )
        mdjlog.info('passing. ..')
    
    mdjlog.info("corp: {}, corpfac: {}".format(corp_df is not None, corpfac_df is not None))
    if corpfac and corp:
        handle_corporate_data(dp_name, load3Dbatch, mdjlog, corp, corp_df, corpfac, corpfac_df, ndx_column, Bs2u)
        # corp_argz = (dp_name, load3Dbatch, mdjlog, corp, corp_df, corpfac, corpfac_df, ndx_column)
        # corp_pro = mupro.Process(target=handle_corporate_data, args=corp_argz)
        # corp_pro.start()
        # upd8batch(load3Dbatch, batches2use)  # , syndifiles
    mdjlog.info("ndvdl: {}, ndvdlfac: {}".format(ndvdl_df is not None, ndvdlfac_df is not None))
    if ndvdlfac and ndvdl:
        handle_individual_data(dp_name, load3Dbatch, mdjlog, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df, ndx_column, Bs2u)
        # ndvdl_argz = (dp_name, load3Dbatch, mdjlog, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df, ndx_column)
        # ndvdl_pro = mupro.Process(target=handle_individual_data, args=ndvdl_argz)
        # ndvdl_pro.start()
        # upd8batch(load3Dbatch, batches2use)  # , syndifiles
    combo, combo_df, corp, corp_df, corpfac, corpfac_df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df = re_init_vars()
    mdjlog.info("\n#E$3D")


def rez_chunk_synd(corpfac, dpid, loaded_batch, load3DSegments, Bs2u):
    mdjlog, load3DSegments = get_logger(loaded_batch['dp_name']), list(set(load3DSegments))
    mdjlog.info(loaded_batch)
    from IoCEngine.utils.db2data import conf_df, ndvdl_data, i2df
    ndx_column = 'cust_id' if loaded_batch['in_mod'] in cdt_udf_modes else 'account_no'
    if any(x in cs for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
        corp, corp_df = corp_data(dpid, loaded_batch)
        data_size = int(loaded_batch['facility'])
        fac_type = 'corpfac' if 'fac' not in load3DSegments else 'fac'
        rounds, size_done = data_size // split_var + 1, 0
        crounds, size2use = 1 if rounds is (0 or 1) else rounds - 1, round(data_size / rounds)
        if corp_df is not None and not corp_df.empty:
            for c in range(rounds):
                try:
                    size2use = size2use if data_size - size_done > size2use else data_size - size_done
                    fac_df, fac_rez, size = i2df(fac_type, size2use, dpid, loaded_batch, 'account_no')
                    if fac_df is not None and not fac_df.empty:
                        fac_df, size = conf_df(loaded_batch, mdjlog, fac_df, fac_rez)
                        fac_df.set_index('account_no', inplace=True)
                        fac_df['account_no'] = fac_df.index
                        mdjlog.info(
                            "getting! #{} of {} Counts: {} of {} of {}".format(c, crounds, size2use, size_done,
                                                                               data_size))
                        corp2df = corp_df[corp_df[ndx_column].isin(fac_df[ndx_column])]
                        mdjlog.info("corp: {}, corpfac: {}".format(corp2df is not None, fac_df is not None))
                        handle_corporate_data(loaded_batch['dp_name'], loaded_batch, mdjlog, corp, corp2df, True,
                                              fac_df, ndx_column, Bs2u, 'chunk_mode')
                        # if c > 0 and c % 2 > 0: count_down(None, 5)
                    else:
                        corp_df['dpid'], corp_df['cycle_ver'] = loaded_batch['dpid'], loaded_batch['cycle_ver']
                        upd8DFstatus('corp', corp_df, ndx_column, 'Loaded')
                        mdjlog.warn('no FACILITY data . ..')
                except Exception as e:
                    mdjlog.error(e)

    if any(x in ns for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
        ndvdl, ndvdl_df = ndvdl_data(dpid, loaded_batch)
        data_size = int(loaded_batch['facility'])
        fac_type = 'ndvdlfac'  # if 'fac' not in load3DSegments else 'fac'
        rounds, size_done = data_size // split_var + 1, 0
        crounds, size2use = 1 if rounds is (0 or 1) else rounds - 1, round(data_size / rounds)
        if ndvdl_df is not None and not ndvdl_df.empty:
            for c in range(rounds):
                try:
                    size2use = size2use if data_size - size_done > size2use else data_size - size_done
                    fac_df, fac_rez, size = i2df(fac_type, size2use, dpid, loaded_batch, 'account_no')
                    if fac_df is not None and not fac_df.empty:
                        fac_df, size = conf_df(loaded_batch, mdjlog, fac_df, fac_rez)
                        fac_df.set_index('account_no', inplace=True)
                        fac_df['account_no'] = fac_df.index
                        mdjlog.info(
                            "getting! #{} of {} Counts: {} of {} of {}".format(c, crounds, size2use, size_done,
                                                                               data_size))
                        ndvdl2df = ndvdl_df[ndvdl_df[ndx_column].isin(fac_df[ndx_column])]
                        mdjlog.info("ndvdl: {}, ndvdlfac: {}".format(ndvdl2df is not None, fac_df is not None))
                        handle_individual_data(loaded_batch['dp_name'], loaded_batch, mdjlog, ndvdl, ndvdl2df, True,
                                               fac_df, ndx_column, Bs2u, 'chunk_mode')
                        # if c > 0 and c % 2 > 0: count_down(None, 5)
                    else:
                        ndvdl_df['dpid'], ndvdl_df['cycle_ver'] = loaded_batch['dpid'], loaded_batch['cycle_ver']
                        upd8DFstatus('ndvdl', ndvdl_df, ndx_column, 'Loaded')
                        mdjlog.warn('no FACILITY data . ..')
                except Exception as e:
                    mdjlog.error(e)
    
    mdjlog.info('done. ..')
    upd8batch(loaded_batch, Bs2u)


def re_init_vars():
    combo, corpfac, ndvdlfac, corp, ndvdl = False, False, False, False, False
    combo_df, corp_df, corpfac_df, ndvdlfac_df, ndvdl_df = None, None, None, None, None
    return combo, combo_df, corp, corp_df, corpfac, corpfac_df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df


def handle_individual_data(dp_name, load3Db, mdjlog, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df, ndx_column, b2u,
                           chunk_mode=None):
    try:
        # batches2use, mdjlog, syndibatch = [], get_logger(loaded_batches[0]['dp_name']), getID()
        ndvdlfac_df_no_sbjt = ndvdlfac_df[~ndvdlfac_df[ndx_column].isin(ndvdl_df[ndx_column])]
        ndvdl_df_no_crdt = ndvdl_df[~ndvdl_df[ndx_column].isin(ndvdlfac_df[ndx_column])]
        if not ndvdlfac_df_no_sbjt.empty:
            error_cat, error_desc = 'Dependent Records Error', (
                'Individual Credit Record reported without CORRESPONDING Subject Record')
            # todo we can now decide to drop records with exceptions
            # todo and log then as log_df_xcpxns(load3Dbatch, ndvdlfac_df_no_sbjt, None, (error_cat, error_desc,), False)
        if not ndvdl_df_no_crdt.empty:
            error_cat, error_desc = 'Dependent Records Error', (
                'Individual Subject Record reported without CORRESPONDING Credit Record')
            # todo we can now decide to drop records with exceptions
            # todo and log then as log_df_xcpxns(load3Dbatch, ndvdl_df_no_crdt, None, (error_cat, error_desc,), True)
        
        if ndvdl and ndvdlfac:
            # todo so we validate right here
            # todo R3Visit xcpxn_rex(ndvdlfac_df, ndvdl_df, 'individual', load3Dbatch)
            # fac_vals(load3Dbatch, ndvdlfac2df, )
            # ndvdlfac2df = ppns(fac_vals, ndvdlfac_df, load3Dbatch, True)
            ndvdl2df = ndvdl_vals(load3Db, ndvdl_df, )
            # ndvdl2df = ppns(ndvdl_vals, ndvdl_df, load3Dbatch, True)
            ndvdlfac2df = fac_vals(load3Db, ndvdlfac_df, )
            # ndvdlfac2df = ppns(fac_vals, ndvdlfac_df, load3Dbatch, True)
            # xcpxn_rex(ndvdlfac2df, ndvdl2df, 'individual', load3Dbatch)
            # todo del fac_df, ndvdl_df, ndvdlfac_df
            mdjlog.info(
                f"""individual facility counts is {ndvdlfac2df.shape[0]} individual subject count is {ndvdl2df.shape[0]}
                """)
            datCat = 'con'
            ctgry_dtls, dp_meta = g_meta(datCat, dp_name, load3Db)
            syndifiles = syndic8data(ndvdlfac2df, ndvdl2df, load3Db, ctgry_dtls, datCat, dp_meta, b2u, chunk_mode)
            # upd8batch(load3Dbatch, batches2use)  # , sb2file, syndifiles
    except Exception as e:
        mdjlog.error(e)


def handle_corporate_data(dp_name, load3Db, mdjlog, corp, corp_df, corpfac, corpfac_df, ndx_column, b2u,
                          chunk_mode=None):
    try:
        corpfac_df_no_sbjt = corpfac_df[~corpfac_df[ndx_column].isin(corp_df[ndx_column])]
        corp_df_no_crdt = corp_df[~corp_df[ndx_column].isin(corpfac_df[ndx_column])]
        if not corpfac_df_no_sbjt.empty:
            error_cat, error_desc = 'Dependent Records Error', (
                'Corporate Credit Record reported without CORRESPONDING Subject Record')
            # todo we can now decide to drop records with exceptions
            # todo and log then as log_df_xcpxns(load3Dbatch, corpfac_df_no_sbjt, None, (error_cat, error_desc,), False)
        if not corp_df_no_crdt.empty:
            error_cat, error_desc = 'Dependent Records Error', (
                'Corporate Subject Record reported without CORRESPONDING Credit Record')
            # todo we can now decide to drop records with exceptions
            # todo and log then as log_df_xcpxns(load3Dbatch, corp_df_no_crdt, None, (error_cat, error_desc,), True)
        
        if corp and corpfac:
            corp2df = corp_vals(load3Db, corp_df, )
            # corp2df = ppns(corp_vals, corp_df, load3Dbatch, True)
            corpfac2df = fac_vals(load3Db, corpfac_df, )
            # corpfac2df = ppns(fac_vals, corpfac_df, load3Dbatch, True)
            # todo R3Visit xcpxn_rex(corpfac2df, corp2df, 'corporate', load3Dbatch)
            # todo del corp_df, corpfac_df, fac_df
            mdjlog.info(f"""\n\t\t\tcorporate facility counts is {corpfac2df.shape[0]}
            corporate subject count is {corp2df.shape[0]}""")
            datCat = 'com'
            ctgry_dtls, dp_meta = g_meta(datCat, dp_name, load3Db)
            syndifiles = syndic8data(corpfac2df, corp2df, load3Db, ctgry_dtls, datCat, dp_meta, b2u, chunk_mode)  #:
            # upd8batch(load3Dbatch, batches2use)  # , syndifiles
    except Exception as e:
        mdjlog.error(e)


# def now_index(df):
#     df['_id'] = df['_id'].apply(lambda x: '-'.join(x.split('-')[:-1]))
#     pass

def rez_combined_data(corp, corpfac, dpid, loaded_batch, ndvdl, ndvdlfac):
    mdjlog = get_logger(loaded_batch['dp_name'])
    load3DSegments = loaded_batch['segments']
    mdjlog.info(load3DSegments)
    ndx_column = 'cust_id' if loaded_batch['in_mod'] in cdt_udf_modes else 'account_no'
    if any(x in cs for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
        corp, corp_df = corp_data(dpid, loaded_batch)
    if (corp and corp_df is not None) or loaded_batch['data_type'] in all_corp_modes:
        if any(x in cf for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
            fac_type = 'corpfac' if 'fac' not in load3DSegments else 'fac'
            corpfac, corpfac_df = fac_data(dpid, loaded_batch, fac_type)
            try:
                corpfac2df = corpfac_df[corpfac_df[ndx_column].isin(corp_df[ndx_column])]
            except Exception as e:
                mdjlog.warn(e)
    elif loaded_batch['data_type'] == 'all':
        corp_df, corpfac2df = None, None
    if any(x in ns for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
        ndvdl, ndvdl_df = ndvdl_data(dpid, loaded_batch)
    if (ndvdl and ndvdl_df is not None) or loaded_batch['data_type'] in ('all', 'allndvdl'):
        if any(x in nf for x in load3DSegments) and loaded_batch['status'] == 'Loaded' and 'fac' not in load3DSegments:
            fac_type = 'ndvdlfac' if 'fac' not in load3DSegments else 'fac'
            ndvdlfac, ndvdlfac_df = fac_data(dpid, loaded_batch, fac_type)
        elif 'fac' in load3DSegments:
            ndvdlfac, ndvdlfac_df = True, corpfac_df
        try:
            ndvdlfac2df = ndvdlfac_df[ndvdlfac_df[ndx_column].isin(ndvdl_df[ndx_column])]
        except Exception as e:
            mdjlog.warn(e)
    if loaded_batch['data_type'] == 'all':
        del corpfac_df, ndvdlfac_df
        return corp, corp_df, corpfac, corpfac2df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac2df
    if loaded_batch['data_type'] in all_corp_modes:
        del corpfac_df
        return corp, corp_df, corpfac, corpfac2df, False, None, False, None
    if loaded_batch['data_type'] in all_ndvdl_modes:
        del ndvdlfac_df
        return False, None, False, None, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac2df


def rez_combo_data(combo, combo_df, dpid, loaded_batch):
    mdjlog = get_logger(loaded_batch['dp_name'])
    combo, combo_df = combo_data(dpid, loaded_batch)
    return combo, combo_df


def rez_single_data(corp, corpfac, ndvdl, ndvdlfac, dpid, loaded_batch, load3DSegments):
    mdjlog, load3DSegments = get_logger(loaded_batch['dp_name']), list(set(load3DSegments))
    mdjlog.info(load3DSegments)
    ndx_column = 'cust_id' if loaded_batch['in_mod'] in cdt_udf_modes else 'account_no'
    corpfac2df, ndvdlfac2df = None, None
    if any(x in cs for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
        try:
            fac_type = 'corpfac' if 'fac' not in load3DSegments else 'fac'
            corpfac, corpfac_df = fac_data(dpid, loaded_batch, fac_type)
            if corpfac_df is not None and not corpfac_df.empty:  # and loaded_batch['data_type'] in cf:
                if any(x in cf for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
                    corp, corp_df = corp_data(dpid, loaded_batch)
                    try:
                        if corpfac_df is None:
                            corpfac2df = None
                            corp_df['dpid'], corp_df['cycle_ver'] = loaded_batch['dpid'], loaded_batch['cycle_ver']
                            upd8DFstatus('corp', corp_df, ndx_column, 'Loaded')
                        else:
                            if not corpfac_df.empty:
                                corpfac2df = corpfac_df[corpfac_df[ndx_column].isin(corp_df[ndx_column])]
                    except Exception as e:
                        corpfac_df, corpfac2df = None
        except Exception as e:
            mdjlog.warn(e)
    else:
        corp, corp_df, corpfac, corpfac_df, corpfac2df = False, None, False, None, None
    
    if any(x in ns for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
        if 'fac' in load3DSegments and corpfac_df is not None:
            ndvdlfac, ndvdlfac_df = True, corpfac_df
        else:
            fac_type = 'ndvdlfac' if 'fac' not in load3DSegments else 'fac'
            ndvdlfac, ndvdlfac_df = fac_data(dpid, loaded_batch, fac_type)
        if ndvdlfac_df is not None and not ndvdlfac_df.empty:  # and loaded_batch['data_type'] in nf:
            if any(x in nf for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
                ndvdl, ndvdl_df = ndvdl_data(dpid, loaded_batch)
                try:
                    if ndvdlfac_df is None:
                        ndvdlfac2df = None
                        ndvdl_df['dpid'], ndvdl_df['cycle_ver'] = loaded_batch['dpid'], loaded_batch['cycle_ver']
                        upd8DFstatus('ndvdl', ndvdl_df, ndx_column, 'Loaded')
                    else:
                        if not ndvdlfac_df.empty:
                            ndvdlfac2df = ndvdlfac_df[ndvdlfac_df[ndx_column].isin(ndvdl_df[ndx_column])]
                except Exception as e:
                    ndvdlfac2df = None
    
    else:
        ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df, ndvdlfac2df = False, None, False, None, None
    
    mdjlog.info("corp: {}, corpfac: {}, ndvdl: {}, ndvdlfac: {}".format(
        corp_df is not None, corpfac2df is not None, ndvdl_df is not None, ndvdlfac2df is not None))
    if corpfac_df is not None and not corpfac_df.empty: del corpfac_df
    if ndvdlfac_df is not None and not ndvdlfac_df.empty: del ndvdlfac_df
    return corp, corp_df, corpfac, corpfac2df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac2df


def rez_single_data_cf(corp, corpfac, ndvdl, ndvdlfac, dpid, loaded_batch, load3DSegments):
    mdjlog, load3DSegments = get_logger(loaded_batch['dp_name']), list(set(load3DSegments))
    ndx_column = 'cust_id' if loaded_batch['in_mod'] in cdt_udf_modes else 'account_no'
    corpfac2df, ndvdlfac2df = None, None
    if any(x in cs for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
        corp, corp_df = corp_data(dpid, loaded_batch)
        if corp:  # and loaded_batch['data_type'] in cf:
            if any(x in cf for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
                fac_type = 'corpfac' if 'fac' not in load3DSegments else 'fac'
                corpfac, corpfac_df = fac_data(dpid, loaded_batch, fac_type)
                try:
                    if corpfac_df is None:
                        corpfac2df = None
                        corp_df['dpid'], corp_df['cycle_ver'] = loaded_batch['dpid'], loaded_batch['cycle_ver']
                        upd8DFstatus('corp', corp_df, ndx_column, 'Loaded')
                    else:
                        if not corpfac_df.empty:
                            corpfac2df = corpfac_df[corpfac_df[ndx_column].isin(corp_df[ndx_column])]
                except Exception as e:
                    corpfac_df, corpfac2df = None
    
    else:
        corp, corp_df, corpfac, corpfac_df, corpfac2df = False, None, False, None, None
    
    if any(x in ns for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
        ndvdl, ndvdl_df = ndvdl_data(dpid, loaded_batch)
        if ndvdl:  # and loaded_batch['data_type'] in nf:
            if any(x in nf for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
                if 'fac' in load3DSegments and corpfac_df is not None:
                    ndvdlfac, ndvdlfac_df = True, corpfac_df
                else:
                    fac_type = 'ndvdlfac' if 'fac' not in load3DSegments else 'fac'
                    ndvdlfac, ndvdlfac_df = fac_data(dpid, loaded_batch, fac_type)
                try:
                    if ndvdlfac_df is None:
                        ndvdlfac2df = None
                        ndvdl_df['dpid'], ndvdl_df['cycle_ver'] = loaded_batch['dpid'], loaded_batch['cycle_ver']
                        upd8DFstatus('ndvdl', ndvdl_df, ndx_column, 'Loaded')
                    else:
                        if not ndvdlfac_df.empty:
                            ndvdlfac2df = ndvdlfac_df[ndvdlfac_df[ndx_column].isin(ndvdl_df[ndx_column])]
                except Exception as e:
                    ndvdlfac2df = None
    
    else:
        ndvdl, ndvdl_df, ndvdlfac, ndvdlfac2df = False, None, False, None
    
    mdjlog.info("corp: {}, corpfac: {}, ndvdl: {}, ndvdlfac: {}".format(
        corp_df is not None, corpfac2df is not None, ndvdl_df is not None, ndvdlfac2df is not None))
    if corpfac_df is not None and not corpfac_df.empty: del corpfac_df
    if ndvdlfac_df is not None and not ndvdlfac_df.empty: del ndvdlfac_df
    return corp, corp_df, corpfac, corpfac2df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac2df


def rez_inter_customers(load3Dbatch, corp_df, ndvdl_df, ):
    dp_name, data_type = load3Dbatch['dp_name'], load3Dbatch['data_type']
    ndx_column = 'cust_id' if load3Dbatch['in_mod'] in cdt_udf_modes else 'account_no'
    corp_in_ndvdl_df = corp_df[corp_df[ndx_column].isin(ndvdl_df[ndx_column])].sort_values(by=ndx_column)
    ndvdl_in_corp_df = ndvdl_df[ndvdl_df[ndx_column].isin(corp_df[ndx_column])].sort_values(by=ndx_column)
    mdjlog.info("resolving overlaps")
    
    if not corp_in_ndvdl_df.empty:  # and data_type == 'all'
        unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed')
        corp_in_ndvdl_df.to_excel(unprocessed + os.sep + 'corporate_customers_in_individual.xlsx', index=False)
        corp2df = corp_df[~corp_df[ndx_column].isin(ndvdl_df[ndx_column])]
    else:
        corp2df = corp_df
    
    if not ndvdl_in_corp_df.empty:
        unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed')
        ndvdl_in_corp_df.to_excel(unprocessed + os.sep + 'individual_customers_in_corporate.xlsx', index=False)
        ndvdl2df = ndvdl_df[~ndvdl_df[ndx_column].isin(corp_df[ndx_column])]
    else:
        ndvdl2df = ndvdl_df
    
    return corp2df, ndvdl2df


def rez_inter_facilities(load3Dbatch, corpfac_df, ndvdlfac_df, ):
    dp_name, data_type = load3Dbatch['dp_name'], load3Dbatch['data_type']
    mdjlog = get_logger(dp_name)
    ndx_column = 'cust_id' if load3Dbatch['in_mod'] in cdt_udf_modes else 'account_no'
    corpfac_in_ndvdlfac_df = corpfac_df[corpfac_df[ndx_column].isin(ndvdlfac_df[ndx_column])].sort_values(
        by=ndx_column)
    ndvdlfac_in_corpfac_df = ndvdlfac_df[ndvdlfac_df[ndx_column].isin(corpfac_df[ndx_column])].sort_values(
        by=ndx_column)
    mdjlog.info("resolving overlaps")
    if not corpfac_in_ndvdlfac_df.empty and data_type == 'all':
        unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed')
        corpfac_in_ndvdlfac_df.to_excel(unprocessed + os.sep + 'corporate_facilities_in_individual.xlsx', index=False)
        corpfac2df = corpfac_df[~corpfac_df[ndx_column].isin(ndvdlfac_df[ndx_column])]
    else:
        corpfac2df = corpfac_df
    if not ndvdlfac_in_corpfac_df.empty and data_type == 'all':
        unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed')
        ndvdlfac_in_corpfac_df.to_excel(unprocessed + os.sep + 'individual_facilities_in_corporate.xlsx', index=False)
        ndvdlfac2df = ndvdlfac_df[~ndvdlfac_df[ndx_column].isin(corpfac_df[ndx_column])]
    else:
        ndvdlfac2df = ndvdlfac_df
    return corpfac2df, ndvdlfac2df


def get_crdt_file(file_dict):
    files_in_dir = [file for file in os.listdir(drop_zone) if len(file.split('_')) >= 4]
    # mdjlog.info("{}".format(files_in_dir))
    s_f = None
    files_in_dir.remove(file_dict['file_name'])
    mdjlog.info("Checking for data file to pair with {}.".format(file_dict['file_name']))
    for i, f in enumerate(files_in_dir):
        file_id = dict_file(f, xtrct_file_details(f))
        f_dict = DataFiles.objects.get(id=file_id)
        if (f_dict['dp_name'] == file_dict['dp_name']
                # and f_dict['fl_ndx'] == file_dict['fl_ndx']
                and f_dict['in_mod'] == file_dict['in_mod']
                and f_dict['out_mod'] == file_dict['out_mod']
                and f_dict['xtra_nfo'] == file_dict['xtra_nfo']
                and f_dict['cycle_ver'] == file_dict['cycle_ver']
                and (f_dict['data_type'] in ('fac', file_dict['data_type'] + 'fac'))):
            return f
    return s_f


def get_subj_file(file_dict):
    mdjlog = get_logger(file_dict['file_name'].split('_')[0])
    files_in_dir = [file for file in os.listdir(drop_zone) if len(file.split('_')) >= 4]
    # mdjlog.info("{}".format(files_in_dir))
    s_f = None
    files_in_dir.remove(file_dict['file_name'])
    mdjlog.info("Checking for data file to pair with {}.".format(file_dict['file_name']))
    for i, f in enumerate(files_in_dir):
        file_id = dict_file(f, xtrct_file_details(f))
        f_dict = DataFiles.objects.get(id=file_id)
        if (file_dict and f_dict['dp_name'] == file_dict['dp_name']
                # and file_dict['fl_ndx'] == f_dict['fl_ndx']
                and f_dict['in_mod'] == file_dict['in_mod']
                and f_dict['out_mod'] == file_dict['out_mod']
                and f_dict['xtra_nfo'] == file_dict['xtra_nfo']
                and f_dict['cycle_ver'] == file_dict['cycle_ver']
                and (f_dict['data_type'] == file_dict['data_type'][:4] or f_dict['data_type'] in ('comm', 'cons'))):
            return f
    return s_f


@app.task(name='get_file_set')
def get_file_set(file_dict_data):
    file = file_dict_data['file_name']
    subj_file = file if file_dict_data['data_type'] in ('comm', 'cons') else None
    try:
        if not subj_file:
            subj_file = get_subj_file(file_dict_data)
            if not subj_file:
                missing_subj_file(file_dict_data)
                return
            crdt_file = file
        else:
            crdt_file = get_crdt_file(file_dict_data)
        
        if not crdt_file:
            missing_crdt_file(file_dict_data)
            return
        
        # mdjlog.info("\nProcessing:\n\t Credit File: {}\n\tSubject File: {}".format(crdt_file, subj_file))
        
        if crdt_file and file_dict_data['data_type'] in ('comm', 'cons'):
            crdt_file_dtls = DataFiles.objects(file_name=crdt_file).first()
            if crdt_file_dtls['status'] != 'Loaded':
                route_file.delay(str(crdt_file_dtls.id))
            else:
                mdjlog.info("\t Credit File: {} already Loaded. ..".format(crdt_file))
        
        if subj_file and file_dict_data['data_type'] in ('commfac', 'consfac', 'fac'):
            sbjt_file_dtls = DataFiles.objects(file_name=subj_file).first()
            
            if sbjt_file_dtls['status'] != 'Loaded':
                route_file.delay(str(sbjt_file_dtls.id))
            else:
                mdjlog.info("\tSubject File: {} already Loaded. ..".format(subj_file))
    
    except Exception as e:
        mdjlog.error(e)


def missing_crdt_file(file_dict_data):
    mdjlog = get_logger(file_dict_data['dp_name'])
    # mdjl_logger(file_dict_data['mmbr'])
    msg = """
\nHi!,\nWell done!\ni'm expecting a subject data file for credit data file {}
i think you have forgotten to drop it.
Please simply drop the subject file and i can take care of the rest.
Thank you.""".format(file_dict_data['file_name'])
    mdjlog.error(msg)


def missing_subj_file(file_dict_data):
    mdjlog = get_logger(file_dict_data['dp_name'])
    # mdjl_logger(file_dict_data['mmbr'])
    msg = """
\nHi!,\nWell done\ni'm expecting a subject data file for credit data file {}
i think you have forgotten to drop it.
Please simply drop the subject file and i can take care of the rest.
Thank you.""".format(file_dict_data['file_name'])
    mdjlog.error(msg)
