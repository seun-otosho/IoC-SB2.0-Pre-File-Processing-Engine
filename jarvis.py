#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
# from dask.distributed import Client, Executor
from itertools import chain

from IoCEngine import drop_zone
# from IoCEngine.SHU.categorize import minify
from IoCEngine.SHU.trans4mas import corp_vals, ndvdl_vals, fac_vals
from IoCEngine.celeryio import app
from IoCEngine.commons import count_down, getID, mk_dir, mk_dp_x_dir
from IoCEngine.cores import ppns
from IoCEngine.logger import get_logger
from IoCEngine.utils.db2data import combo_data, fac_data, corp_data, ndvdl_data
from IoCEngine.utils.file import DataFiles, DataBatchProcess, dict_file, xtrct_file_details
from IoCEngine.utils.file_reader import xtrct_all_data, xtrct_ff_data, xtrct_ws_data
from IoCEngine.utils.sb2 import syndic8data

# client = Executor()

cs, fs, ns = ['corp', ], ['corpfac', 'fac', 'ndvdlfac', ], ['ndvdl', ]
mdjlog = get_logger('jarvis')
load3Dbatch = None

mk_dir(drop_zone)


@app.task(name='route_file')
def route_file(file_data):
    mdjlog = get_logger(file_data['dp_name'])
    if isinstance(file_data, str):
        file_data = DataFiles.objects.get(id=file_data)
    data_pro_bat_no = False
    try:
        file_name = file_data['file_name']
        if '_all' in file_name:
            file_data.update(batch_no=getID())
            data_pro_bat_no = xtrct_all_data(file_data.reload())
        elif file_data['data_type'] in fs or file_data['data_type'] in cs + ns + ['combo']:
            if file_name.lower().endswith(('.csv', '.txt')):
                data_pro_bat_no = xtrct_ff_data(file_data)
                # get_file_set(file_data)  # get other files in set
            if file_name.lower().endswith('.xlsx'):
                data_pro_bat_no = xtrct_ws_data(file_data)
                # get_file_set(file_data)  # get other files in set
            if file_name.lower().endswith('.xls'):
                mdjlog.error("""\nHi\nWell done\nThe extension .xls for file {} is no longer supported.
                             Please save the file with .xlsx and let's do this again.\nThank you""".format(file_name))
                return
    except Exception as e:
        mdjlog.error(e)


# code keep
# and any(x in fs for x in loaded_batch['segments'])

@app.task(name='route_filed_data')
def route_filed_data(dpid):
    # get data batch no for submission and process data
    # todo get loaded batches
    # global load3Dbatch
    dp_name, load3Dbatch, loaded_batches = '', None, DataBatchProcess.objects(dpid=dpid, status='Loaded')  # .first()
    combo, corpfac, ndvdlfac, fac, corp, ndvdl = False, False, False, False, False, False
    combo_df, corp_df, corpfac_df, ndvdlfac_df, fac_df, ndvdl_df = None, None, None, None, None, None
    batches2use, mdjlog, syndibatch = [], get_logger(loaded_batches[0]['dp_name']), getID()
    for loaded_batch in loaded_batches:
        try:
            if loaded_batch['status'] == 'Loaded':
                dp_name, xtrcxn_zone = loaded_batch['dp_name'], mk_dp_x_dir(loaded_batch['dp_name'].upper())
                load3Dbatch = loaded_batch
                if loaded_batch['data_type'] in ('corp', 'corpfac', 'fac', 'ndvdl', 'ndvdlfac',):
                    load3DSegments = list(chain.from_iterable([lb['segments'] for lb in loaded_batches]))
                    if not any(x in fs for x in load3DSegments) or not any(x in cs + ns for x in load3DSegments):
                        mdjlog.info("Not enough data matching data segment to process submission for data provider")
                        return
                    batches2use.append(load3Dbatch)
                    corp, corp_df, corpfac, corpfac_df, fac, fac_df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df = \
                        rez_single_data(corp, corp_df, corpfac, corpfac_df, dpid, fac, fac_df, loaded_batch, ndvdl,
                                        ndvdl_df, ndvdlfac, ndvdlfac_df, xtrcxn_zone)

                if loaded_batch['data_type'] in ('all', 'allcorp', 'allndvdl',):
                    # check list of loaded segments
                    batches2use.append(load3Dbatch)
                    corp, corp_df, corpfac, corpfac_df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df = \
                        rez_combined_data(corp, corp_df, corpfac, corpfac_df, dpid, loaded_batch, ndvdl, ndvdl_df,
                                          ndvdlfac, ndvdlfac_df, xtrcxn_zone)
                if loaded_batch['data_type'] in ('combo',):
                    # check list of loaded segments
                    batches2use.append(load3Dbatch)
                    combo, combo_df = rez_combo_data(combo, combo_df, dpid, loaded_batch, xtrcxn_zone)
        except Exception as e:
            mdjlog.error(e)

    if combo:
        # corp_vals(load3Dbatch, corp2df, )
        combo_df = ppns(ndvdl_vals, combo_df, load3Dbatch, True)
        # fac_vals(load3Dbatch, corpfac2df, )
        combo_df = ppns(fac_vals, combo_df, load3Dbatch, True)
        # todo del corp_df, corpfac_df, fac_df
        mdjlog.info("\ncombined data count is {}".format(combo_df.shape[0]))
        count_down(None, 10)
        syndifiles = syndic8data(combo_df, None, load3Dbatch, 'con')

        upd8batch(load3Dbatch, batches2use)  # , syndifiles

    corp_in_ndvdl, corpfac_in_ndvdlfac, ndvdl_in_corp, ndvdlfac_in_corpfac = False, False, False, False
    corp2df, corpfac2df, fac2df, ndvdl2df, ndvdlfac2df = None, None, None, None, None
    # if corp and ndvdl:
    #     # filter overlapping customers
    #     corp2df, corp_in_ndvdl, ndvdl2df, ndvdl_in_corp = \
    #         rez_inter_customers(corp2df, corp_df, corp_in_ndvdl, load3Dbatch, ndvdl2df, ndvdl_df, ndvdl_in_corp)
    #     if corp_in_ndvdl:
    #         corp_df = corp2df
    #
    #     if ndvdl_in_corp:
    #         ndvdl_df = ndvdl2df

    # if corpfac and ndvdlfac:
    #     # filter overlapping facilities
    #     corpfac2df, corpfac_in_ndvdlfac, ndvdlfac2df, ndvdlfac_in_corpfac = \
    #         rez_inter_facilities(corpfac2df, corpfac_df, corpfac_in_ndvdlfac, load3Dbatch, ndvdlfac2df, ndvdlfac_df,
    #                              ndvdlfac_in_corpfac)

    # if corpfac_in_ndvdlfac:
    #     corpfac_df = corpfac2df
    #
    # if ndvdlfac_in_corpfac:
    #     ndvdlfac_df = ndvdlfac2df


    # let's validate

    mdjlog.info("fac: {}, corp: {}, corpfac: {}".format(fac, corp, corpfac))
    if (corpfac or fac) and corp:
        try:
            if fac and not corpfac:
                corpfac, corpfac_df = True, fac_df

                # todo del corpfac_df if corpfac_df else None

            if corp and corpfac:
                if load3Dbatch['in_mod'] == 'cdt':
                    # corp_df_no_fac = corp_df[~corp_df.cust_id.isin(corpfac_df.cust_id)]
                    # corpfac_df_no_corp = corpfac_df[~corpfac_df.cust_id.isin(corp_df.cust_id)]
                    # corp2df = corp_df #[corp_df.cust_id.isin(corpfac_df.cust_id)]
                    # corpfac2df = corpfac_df #[corpfac_df.cust_id.isin(corp_df.cust_id)]
                    pass

                elif load3Dbatch['in_mod'] in ('fandl', 'iff', 'cmb', 'mfi', 'pmi',):
                    # corp_df_no_fac = corp_df[~corp_df.account_no.isin(corpfac_df.account_no)]
                    # corpfac_df_no_corp = corpfac_df[~corpfac_df.account_no.isin(corp_df.account_no)]
                    # corp2df = corp_df #[corp_df.account_no.isin(corpfac_df.account_no)]
                    # if corp2df.empty:
                    #     corp2df = corp_df
                    # corpfac2df = corpfac_df #[corpfac_df.account_no.isin(corp_df.account_no)]
                    # if corpfac2df.empty:
                    #     corpfac2df = corpfac_df
                    pass

                if corp and corpfac:
                    # corp_vals(load3Dbatch, corp2df, )
                    corp2df = ppns(corp_vals, corp_df, load3Dbatch, True)
                    # fac_vals(load3Dbatch, corpfac2df, )
                    corpfac2df = ppns(fac_vals, corpfac_df, load3Dbatch, True)
                    # todo del corp_df, corpfac_df, fac_df
                    mdjlog.info(
                        "\ncorporate facility counts is {}\ncorporate subject count is {}".format(corpfac2df.shape[0],
                                                                                                  corp2df.shape[0]))
                    count_down(None, 10)
                    syndifiles = syndic8data(corpfac2df, corp2df, load3Dbatch, 'com')  #:
                    # load3Dbatch.update(batch_no=syndibatch)
                    # load3Dbatch.reload()
                    #     head, sb2file = os.path.split(syndi_fp)
                    #     load3Dbatch.update(status='Syndicated', sb2file=sb2file)
                    # store tail from ~> head, tail = os.path.split(syndi_fp) with batch

                    upd8batch(load3Dbatch, batches2use)  # , syndifiles
        except Exception as e:
            mdjlog.error(e)

    mdjlog.info("fac: {}, ndvdl: {}, ndvdlfac: {}".format(fac, ndvdl, ndvdlfac))
    if (ndvdlfac or fac) and ndvdl:
        try:
            if fac and not ndvdlfac:
                ndvdlfac, ndvdlfac_df = True, fac_df
            if load3Dbatch['in_mod'] == 'cdt':
                # ndvdl_df_no_fac = ndvdl_df[~ndvdl_df.cust_id.isin(ndvdlfac_df.cust_id)]
                # ndvdlfac_df_no_corp = ndvdlfac_df[~ndvdlfac_df.cust_id.isin(ndvdl_df.cust_id)]
                # ndvdl2df = ndvdl_df #[ndvdl_df.cust_id.isin(ndvdlfac_df.cust_id)]
                # ndvdlfac2df = ndvdlfac_df #[ndvdlfac_df.cust_id.isin(ndvdl_df.cust_id)]
                pass

            elif load3Dbatch['in_mod'] in ('fandl', 'iff', 'cmb', 'mfi', 'pmi',):
                # ndvdl_df_no_fac = ndvdl_df[~ndvdl_df.account_no.isin(ndvdlfac_df.account_no)]
                # fac_df_no_corp = ndvdlfac_df[~ndvdlfac_df.account_no.isin(ndvdl_df.account_no)]
                # if ndvd
                # ndvdl2df = ndvdl_df #[ndvdl_df.account_no.isin(ndvdlfac_df.account_no)]
                # if ndvdl2df.empty:
                #     ndvdl2df = ndvdl_df
                # ndvdlfac2df = ndvdlfac_df #[ndvdlfac_df.account_no.isin(ndvdl_df.account_no)]
                # if ndvdlfac2df.empty:
                #     ndvdlfac2df = ndvdlfac_df
                pass

            if ndvdl and ndvdlfac:
                # ndvdl_vals(load3Dbatch, ndvdl2df, )
                ndvdl2df = ppns(ndvdl_vals, ndvdl_df, load3Dbatch, True)
                # fac_vals(load3Dbatch, ndvdlfac2df, )
                ndvdlfac2df = ppns(fac_vals, ndvdlfac_df, load3Dbatch, True)
                # todo del fac_df, ndvdl_df, ndvdlfac_df
                mdjlog.info(
                    "\nindividual facility counts is {}\nindividual subject count is {}".format(ndvdlfac2df.shape[0],
                                                                                                ndvdl2df.shape[0]))
                count_down(None, 10)
                syndifiles = syndic8data(ndvdlfac2df, ndvdl2df, load3Dbatch, 'con')  #:
                # load3Dbatch.update(batch_no=syndibatch)
                # load3Dbatch.reload()
                # head, sb2file = os.path.split(syndi_fp)
                # store tail from ~> head, tail = os.path.split(syndi_fp) with batch
                upd8batch(load3Dbatch, batches2use)  # , sb2file, syndifiles
        except Exception as e:
            mdjlog.error(e)


@app.task(name='upd8batch')
def upd8batch(process3DBatch, batches2U):  # , sb2file, syndifiles
    # if not process3DBatch['data_type'] in ('all', 'allcorp', 'allndvdl',):
    mdjlog = get_logger(batches2U[0]['dp_name'])
    for loaded_batch in batches2U:
        if '_fac_' not in loaded_batch['file_name']:
            data_file = DataFiles.objects(dp_name=loaded_batch['dp_name'], cycle_ver=loaded_batch['cycle_ver'],
                                          dpid=loaded_batch['dpid'], in_mod=loaded_batch['in_mod'],
                                          out_mod=loaded_batch['out_mod'],
                                          data_type=loaded_batch['data_type'], ).first()
            # , batch_no=loaded_batch['batch_no']
            try:
                loaded_batch.update(batch_no=process3DBatch['batch_no'], status='Syndicated')  # , sb2file=sb2file
            except Exception as e:
                mdjlog.error(e)
            try:
                data_file.update(batch_no=process3DBatch['batch_no'], status='Syndicated', )
            except Exception as e:
                mdjlog.error(e)
            mdjlog.info('Batch {} & Data File {} Updat3D.'.format(loaded_batch, data_file))
            # load3Dbatch.update(batch_no=load3Dbatch['batch_no'], status='Syndicated')  # , sb2file=sb2file


def rez_combined_data(corp, corp_df, corpfac, corpfac_df, dpid, loaded_batch, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df,
                      xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    load3DSegments = loaded_batch['segments']
    if 'corp' in load3DSegments and loaded_batch['status'] == 'Loaded':
        corp, corp_df = corp_data(dpid, loaded_batch, xtrcxn_zone)
    if corp or loaded_batch['data_type'] in ('all', 'allcorp'):
        if any(x in ('corpfac', 'fac',) for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
            corpfac, corpfac_df = fac_data(dpid, loaded_batch, xtrcxn_zone)
    if 'ndvdl' in load3DSegments and loaded_batch['status'] == 'Loaded':
        ndvdl, ndvdl_df = ndvdl_data(dpid, loaded_batch, xtrcxn_zone)
    if ndvdl or loaded_batch['data_type'] in ('all', 'allndvdl'):
        if any(x in ('fac', 'ndvdlfac',) for x in load3DSegments) and loaded_batch['status'] == 'Loaded':
            ndvdlfac, ndvdlfac_df = fac_data(dpid, loaded_batch, xtrcxn_zone)
    return corp, corp_df, corpfac, corpfac_df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df


def rez_combo_data(combo, combo_df, dpid, loaded_batch, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    combo, combo_df = combo_data(dpid, loaded_batch, xtrcxn_zone)
    return combo, combo_df


def rez_single_data(corp, corp_df, corpfac, corpfac_df, dpid, fac, fac_df, loaded_batch, ndvdl, ndvdl_df, ndvdlfac,
                    ndvdlfac_df, xtrcxn_zone):
    mdjlog = get_logger(loaded_batch['dp_name'])
    if not corpfac and loaded_batch['status'] == 'Loaded' and 'corpfac' in loaded_batch['segments']:
        corpfac, corpfac_df = fac_data(dpid, loaded_batch, xtrcxn_zone)
    if not fac and loaded_batch['status'] == 'Loaded' and 'fac' in loaded_batch['segments']:
        fac, fac_df = fac_data(dpid, loaded_batch, xtrcxn_zone)
    if not ndvdlfac and loaded_batch['status'] == 'Loaded' and 'ndvdlfac' in loaded_batch['segments']:
        ndvdlfac, ndvdlfac_df = fac_data(dpid, loaded_batch, xtrcxn_zone)
    if not corp and loaded_batch['status'] == 'Loaded' and any(x in cs for x in loaded_batch['segments']):
        corp, corp_df = corp_data(dpid, loaded_batch, xtrcxn_zone)
    if not ndvdl and loaded_batch['status'] == 'Loaded' and any(x in ns for x in loaded_batch['segments']):
        ndvdl, ndvdl_df = ndvdl_data(dpid, loaded_batch, xtrcxn_zone)
    return corp, corp_df, corpfac, corpfac_df, fac, fac_df, ndvdl, ndvdl_df, ndvdlfac, ndvdlfac_df


def rez_inter_customers(corp2df, corp_df, corp_in_ndvdl, load3Dbatch, ndvdl2df, ndvdl_df, ndvdl_in_corp):
    dp_name, data_type = load3Dbatch['dp_name'], load3Dbatch['data_type']
    corp_in_ndvdl_df = corp_df[corp_df['cust_id'].isin(ndvdl_df['cust_id'])].sort_values(by="cust_id")
    ndvdl_in_corp_df = ndvdl_df[ndvdl_df['cust_id'].isin(corp_df['cust_id'])].sort_values(by="cust_id")
    if not corp_in_ndvdl_df.empty and data_type == 'all':
        unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed')
        corp_in_ndvdl_df.to_excel(unprocessed + os.sep + 'corporate_customers_in_individual.xlsx', index=False)
        corp2df = corp_df[~corp_df['cust_id'].isin(ndvdl_df['cust_id'])]
        corp_in_ndvdl = True
    if not ndvdl_in_corp_df.empty and data_type == 'all':
        unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed')
        ndvdl_in_corp_df.to_excel(unprocessed + os.sep + 'individual_customers_in_corporate.xlsx', index=False)
        ndvdl2df = ndvdl_df[~ndvdl_df['cust_id'].isin(corp_df['cust_id'])]
        ndvdl_in_corp = True
    return corp2df, corp_in_ndvdl, ndvdl2df, ndvdl_in_corp


def rez_inter_facilities(corpfac2df, corpfac_df, corpfac_in_ndvdlfac, load3Dbatch, ndvdlfac2df, ndvdlfac_df,
                         ndvdlfac_in_corpfac):
    dp_name, data_type = load3Dbatch['dp_name'], load3Dbatch['data_type']
    mdjlog = get_logger(dp_name)
    corpfac_in_ndvdlfac_df = corpfac_df[corpfac_df['account_no'].isin(ndvdlfac_df['account_no'])].sort_values(
        by="account_no")
    ndvdlfac_in_corpfac_df = ndvdlfac_df[ndvdlfac_df['account_no'].isin(corpfac_df['account_no'])].sort_values(
        by="account_no")
    if not corpfac_in_ndvdlfac_df.empty and data_type == 'all':
        unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed')
        corpfac_in_ndvdlfac_df.to_excel(unprocessed + os.sep + 'corporate_facilities_in_individual.xlsx', index=False)
        corpfac2df = corpfac_df[~corpfac_df['account_no'].isin(ndvdlfac_df['account_no'])]
        corpfac_in_ndvdlfac = True
    if not ndvdlfac_in_corpfac_df.empty and data_type == 'all':
        unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed')
        ndvdlfac_in_corpfac_df.to_excel(unprocessed + os.sep + 'individual_facilities_in_corporate.xlsx', index=False)
        ndvdlfac2df = ndvdlfac_df[~ndvdlfac_df['account_no'].isin(corpfac_df['account_no'])]
        ndvdlfac_in_corpfac = True
    return corpfac2df, corpfac_in_ndvdlfac, ndvdlfac2df, ndvdlfac_in_corpfac


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


"""
def process_file_o(file, plof, dyfrmt=None):
    # mdjlog = logger(file.split('_')[0])
    mdjlog = get_logger(file.split('_')[0])
    cmmf_data, cmms_data, cnmf_data, cnms_data, data_list, df_dict = None, None, None, None, [], {}
    try:
        if '_all_' in file:
            # mdjlog.info("began reading file {} @ {}".format(file, right_now()))
            xlwb = openpyxl.load_workbook(drop_zone + file, data_only=True)
            sh_names = xlwb.get_sheet_names()

            # mdjlog.info("completed reading file {} @ {}".format(file, right_now()))
            data_list = [confirm_worksheet_datatype(name, plof) for name in sh_names]
            data_list = [data_item for data_item in data_list if data_item != None]
            # df_dict['mnfst'] = data_list
            for i, sgmnt in enumerate(data_list):
                df = None
                ws = xlwb.get_sheet_by_name(data_list[i][1])
                datal = list(ws.values)
                cols = next(ws.values)[:]
                datai = (islice(r, 0, None) for r in datal)
                df = pd.DataFrame(datai, columns=cols, index=None)
                # if sgmnt[0] in ('cxcf'): xfac_data = df
                # if sgmnt[0] in ('cmcf', 'cmmf'): commfac_data = df
                # if sgmnt[0] in ('cmcs', 'cmms'): commsubj_data = df
                # if sgmnt[0] in ('cncf', 'cnmf'): consfac_data = df
                # if sgmnt[0] in ('cncs', 'cnms'): conssubj_data = df
                df_dict[sgmnt[0]] = df

            df_dict['mnfst'] = data_list
            return df_dict
        if '_allcomm_' in file:
            mdjlog.info("began reading file {} @ {}".format(file, right_now()))
            xlwb = openpyxl.load_workbook(drop_zone + file, data_only=True)
            sh_names = xlwb.get_sheet_names()

            mdjlog.info("completed reading file {} @ {}".format(file, right_now()))
            data_list = [confirm_worksheet_datatype(name, plof) for name in sh_names]
            data_list = [data_item for data_item in data_list if data_item != None]
            # df_dict['mnfst'] = data_list
            for i, sgmnt in enumerate(data_list):
                ws = xlwb.get_sheet_by_name(data_list[i][1])
                datal = list(ws.values)
                cols = next(ws.values)[:]
                datai = (islice(r, 0, None) for r in datal)
                df = pd.DataFrame(datai, columns=cols, index=None)
                # if sgmnt[0] in ('cmcf', 'cmmf'): consfac_data = df
                # if sgmnt[0] in ('cmcs', 'cmms'): conssubj_data = df
                df_dict[sgmnt[0]] = df

            mdjlog.info("returning dataframe for {} @ {}".format(file, right_now()))

            # return consfac_data, conssubj_data
            df_dict['mnfst'] = data_list
            return df_dict

        if '_allcons_' in file:
            mdjlog.info("began reading file {} @ {}".format(file, right_now()))
            xlwb = openpyxl.load_workbook(drop_zone + file, data_only=True)
            sh_names = xlwb.get_sheet_names()

            mdjlog.info("completed reading file {} @ {}".format(file, right_now()))
            data_list = [confirm_worksheet_datatype(name, plof) for name in sh_names]
            data_list = [data_item for data_item in data_list if data_item != None]
            df_dict = {}
            # df_dict['mnfst'] = data_list
            for i, sgmnt in enumerate(data_list):
                ws = xlwb.get_sheet_by_name(data_list[i][1])
                datal = list(ws.values)
                cols = next(ws.values)[:]
                datai = (islice(r, 0, None) for r in datal)
                df = pd.DataFrame(datai, columns=cols, index=None)
                # if sgmnt[0] in ('cncf', 'cnmf'): consfac_data = df
                # if sgmnt[0] in ('cncs', 'cnms'): conssubj_data = df
                df_dict[sgmnt[0]] = df

            mdjlog.info("returning dataframe for {} @ {}".format(file, right_now()))

            # return consfac_data, conssubj_data
            df_dict['mnfst'] = data_list
            return df_dict

        if 'fac_' in file:
            mdjlog.info("began reading file {} @ {}".format(file, right_now()))
            if file.lower().endswith('.xls'):
                mdjlog.error("\nHi\nWell done\nThe extension .xls for file {} is not supported\n"
                             "Please save the file with .xlsx and let's do this again.\nThank you".format(file))
                return
            if file.lower().endswith('.xlsx'):
                df = readXLfiles(file)
            if file.lower().endswith(('.csv', '.txt')):
                df = readFLATfiles(file)
            if df is None:
                mdjlog.error("\nHi\nWell done\nThere seem to be a problem with reading the file {} for processing\n"
                             "Please check and let's do this again.\nThank you".format(file))
                return
            mdjlog.info("completed reading file {} @ {}".format(file, right_now()))

            if '_comm' in file and 'fac_' in file:
                if '_cmb_' in file:
                    sgmnt = 'cmcf'
                if '_mfi_' in file:
                    sgmnt = 'cmmf'
                data_list.append(sgmnt)
                df_dict[sgmnt] = df
            elif '_cons' in file and 'fac_' in file:
                if '_cmb_' in file:
                    sgmnt = 'cncf'
                if '_mfi_' in file:
                    sgmnt = 'cnmf'
                data_list.append(sgmnt)
                df_dict[sgmnt] = df
            mdjlog.info("returning dataframe for {} @ {}".format(file, right_now()))
            # if sgmnt is 'cmmf': return cmmf_data
            # if sgmnt is 'cnmf': return cnmf_data
            df_dict['mnfst'] = data_list
            return df_dict

        # else: and not a cmb file
        if not '_cmb_' in file:
            if '_comm_' in file or '_cons_' in file:
                mdjlog.info("began reading file {} @ {}".format(file, right_now()))
                if file.lower().endswith('.xls'):
                    mdjlog.error("\nHi\nWell done\nThe extension .xls for file {} is not supported\n"
                                 "Please save the file with .xlsx and let's do this again.\nThank you".format(file))
                    return
                if file.lower().endswith('.xlsx'):
                    df = readXLfiles(file)
                if file.lower().endswith('.csv') or file.lower().endswith('.txt'):
                    df = readFLATfiles(file)
                if df is None:
                    mdjlog.error("\nHi\nWell done\nThere seem to be a problem with reading the file {} for processing\n"
                                 "Please check and let's do this again.\nThank you".format(file))
                    return
                mdjlog.info("completed reading file {} @ {}".format(file, right_now()))

                if '_comm_' in file:
                    # d8cols, sgmnt, cmms_data = d8s_cols['sb2_comm'], 'cmms', df
                    if '_cmb_' in file: sgmnt = 'cmcs'
                    if '_mfi_' in file: sgmnt = 'cmms'
                    data_list.append(sgmnt)
                    df_dict[sgmnt] = df
                elif '_cons_' in file:
                    # d8cols, sgmnt, cnms_data = d8s_cols['sb2_cons'], 'cnms', df
                    if '_cmb_' in file: sgmnt = 'cncs'
                    if '_mfi_' in file: sgmnt = 'cnms'
                    data_list.append(sgmnt)
                    df_dict[sgmnt] = df

                mdjlog.info("returning dataframe for {} @ {}".format(file, right_now()))

                # if sgmnt is 'cmms': return cmms_data
                # if sgmnt is 'cnms': return cnms_data
                # df_dict['mnfst'] = data_list
                df_dict['mnfst'] = data_list
                return df_dict
    except Exception as e:
        print("file error {} @ {}".format(e, right_now()))
        # mdjlog.info("file error {} @ {}".format(e, right_now()))
        return


def main_process_re(file_dict_data, mdjlog, plof=None):
    # mdjlog.info('{}'.format(sys.version))
    # mdjlog = logger()(file.split('_')[0])
    # mdjlog = logger(file_dict_data['mmbr'])  # mdjl_logger(file_dict_data['mmbr'])
    dpid, d8reported = file_dict_data['dpid'], file_dict_data['d8report3D']

    dy1st = file_dict_data['dayfirst'] if 'dayfirst' in file_dict_data.keys() \
                                          and file_dict_data['dayfirst'] is not None else False

    file = file_dict_data['file']
    if '_all_' in file.lower():  # and (file.lower().endswith('.xls') or file.lower().endswith('.xlsx')):
        try:
            # cmmf_data, cmms_data, cnmf_data, cnms_data = process_file(file, plof)
            df_dict = process_file(file, plof)
            data_list = df_dict.pop('mnfst', None)
            # for data_item in data_list:
            #     exec('{}_data = df_dict[data_item]')
            # cmmf_data = [df_dict[sgmnt] for sgmnt in data_list if sgmnt == 'cmmf'][0]

            if file_dict_data['dt_mode'] in ('iff', 'sb2'):
                data_list = [i[0] for i in data_list]
                cmmf_data = df_dict[data_list.pop(data_list.index('cmmf'))] if 'cmmf' in data_list else None
                cmms_data = df_dict[data_list.pop(data_list.index('cmms'))] if 'cmms' in data_list else None
                cnmf_data = df_dict[data_list.pop(data_list.index('cnmf'))] if 'cnmf' in data_list else None
                cnms_data = df_dict[data_list.pop(data_list.index('cnms'))] if 'cnms' in data_list else None
                optimus_prime(cmmf_data, cmms_data, cnmf_data, cnms_data, d8reported, dpid, file, plof)
                return
            if file_dict_data['dt_mode'] in ('cdt', 'udf'):
                rodimus_prime(file_dict_data, df_dict, mdjlog, data_list)
                return
        except Exception as e:
            mdjlog.error(e)
            return
    if '_allcomm_' in file.lower():  # and (file.lower().endswith('.xls') or file.lower().endswith('.xlsx')):
        try:
            # df_dict = process_file(file, plof)#cmmf_data, cmms_data
            df_dict = process_file(file, plof)
            data_list = df_dict.pop('mnfst', None)
            if file_dict_data['dt_mode'] in ('iff', 'sb2'):
                data_list = [i[0] for i in data_list]
                cmmf_data = df_dict[data_list.pop(data_list.index('cmmf'))] if 'cmmf' in data_list else None
                cmms_data = df_dict[data_list.pop(data_list.index('cmms'))] if 'cmms' in data_list else None
                optimus_prime(cmmf_data, cmms_data, None, None, d8reported, dpid, file, plof)
            if file_dict_data['dt_mode'] in ('cdt', 'udf'):
                rodimus_prime(file_dict_data, df_dict, mdjlog, data_list)
                return
        except Exception as e:
            mdjlog.error(e)
            return
    if '_allcons_' in file.lower():  # and (file.lower().endswith('.xls') or file.lower().endswith('.xlsx')):
        try:
            df_dict = process_file(file, plof)
            data_list = df_dict.pop('mnfst', None)
            if file_dict_data['dt_mode'] in ('iff', 'sb2'):
                data_list = [i[0] for i in data_list]
                cnmf_data = df_dict[data_list.pop(data_list.index('cnmf'))] if 'cnmf' in data_list else None
                cnms_data = df_dict[data_list.pop(data_list.index('cnms'))] if 'cnms' in data_list else None
                # optimus_prime(None, None, cnmf_data, cnms_data, d8reported, dpid, file, plof)
            # if file_dict_data['dt_mode'] in ('cdt', 'udf'):
            rodimus_prime(file_dict_data, df_dict, mdjlog, data_list)
            return
        except Exception as e:
            mdjlog.error(e)
            return

    if file_dict_data['dt_type'].lower() in ('commfac', 'consfac', 'fac',):
        # cmmf_data, cmms_data, cnmf_data, cnms_data, file = \
        try:
            prcss_files_set(dy1st, file, file_dict_data, plof, d8reported, dpid, mdjlog)
            return
        except Exception as e:
            mdjlog.error(e)
            return

    if file_dict_data['dt_type'].lower() in ('comm', 'cons',):
        # cmmf_data, cmms_data, cnmf_data, cnms_data, file = \
        try:
            prcss_files_set(dy1st, file, file_dict_data, plof, d8reported, dpid, mdjlog)
            return
        except Exception as e:
            mdjlog.error(e)
            return

"""
