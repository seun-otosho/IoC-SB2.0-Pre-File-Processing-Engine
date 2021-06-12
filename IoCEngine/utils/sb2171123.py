#!/usr/bin/python3
# -*- coding: utf-8 -*-


import inspect
import multiprocessing as mp
import os
import re
import unicodedata
from datetime import datetime

import numpy as np
import pandas as pd
from elasticsearch import helpers

from IoCEngine import xtrcxn_area
from IoCEngine.commons import cdt_udf_modes, count_down, get_logger, mk_dir, right_now, std_out
from IoCEngine.config.pilot import chunk_size, es, es_i, mpcores
from IoCEngine.utils.data_modes import iff
from IoCEngine.utils.file import DataFiles, SB2FileInfo

a, b, c = 1, 2, 3

# todo override for dev and test
mpcores = 3


def var_name(var):
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    return [var_name for var_name, var_val in callers_local_vars if var_val is var][0]


# print(var_name(b))


def re_str(str_data, mdjlog):
    mdjlog = get_logger(mdjlog)
    try:
        if type(str_data) == str:
            return ''.join(
                # (c for c in unicodedata.normalize('NFD', unicode(str_data, "utf-8")) if
                (c for c in unicodedata.normalize('NFD', str_data) if
                 unicodedata.category(c) != 'Mn'))

            # if type(str_data) == unicode:
        return ''.join((c for c in unicodedata.normalize('NFD', str_data) if unicodedata.category(c) != 'Mn'))

    except Exception as e:
        mdjlog.error("{}\n{}\n{}".format(e, str_data, type(str_data)))
        # str_re =  unicodedata.normalize('NFKD', unicode(str_data, 'ISO-8859-1')).encode('ASCII', 'ignore')
        str_re = unicodedata.normalize('NFKD', str_data).encode('ASCII', 'ignore')
        return str_re


def clean_a_line(fac_str):
    # df_str = '|'.join(str(c) for c in df_row.values.tolist()[0])
    df_str = re.sub('\s+', ' ', fac_str).strip().replace('|-|', '||').replace(
        '|NULL|', '||').replace('|Null|', '||').replace('|null|', '||').replace(
        '|NIL|', '||').replace('|Nil|', '||').replace('|nil|', '||').replace('None', '').replace('…', '')
    df_str = df_str.replace('.0|', '|') if '.0|' in df_str else df_str
    return df_str


def fix_fac_missing(crdt_data, meta_data):
    crdt_data.insert(0, 'prev_dpid', '')
    crdt_data.insert(0, 'dpid', meta_data['dpid'])


def syndidata(lz):
    crdt_data, sbjt_data, meta_data, dp_meta, datCat, hdr, sb2file = lz[0], lz[1], lz[2], lz[3], lz[4], lz[5], lz[6]
    mdjlog, dpid, syndicaxn_complt = get_logger(meta_data['dp_name']), meta_data['dpid'], False
    try:
        dp_name = meta_data['dp_name'].split('_')[0].lower()  # , syndi_data_list, []
        institution_cat, submxn_pt = dp_meta[dp_name][2], dp_meta[dp_name][1]
        # file_dtls = DataFiles.objects(dpid=meta_data['dpid'], cycle_ver=meta_data['cycle_ver'], status='Loaded').first()
        meta_data.reload()
        # crdt_copy = crdt_data.copy()
        # sbjt_copy = sbjt_data.copy()
        # try:
        #     loaded_batch = DataBatchProcess.objects(batch_no=meta_data['batch_no'], dpid=meta_data['dpid'],
        #                                             status='Loaded').first()
        # except:
        #     loaded_batch = DataBatchProcess.objects(dpid=meta_data['dpid'], status='Loaded').first()
        loaded_batch = meta_data
        # dp_name = loaded_batch['dp_name']
        # cust_id, ac_no, data_list, file = None, None, [], file_dtls['file_name']  # , fac_lines, sub_lines [], [],

        # if meta_data['data_type'] == 'combo':
        #     cust_ds, faci_ds = 'combined_submissions', 'combined_submissions'
        #     cust_rec_col, faci_rec_col = pym_db[cust_ds], pym_db[faci_ds]
        # else:
        #     # cust_ds = 'corporate_submissions' if 'com' in datCat else 'individual_submissions'
        #     # cust_rec_col = pym_db[cust_ds]
        #     # faci_ds = 'facility_submissions'
        #     # faci_rec_col = pym_db[faci_ds]
        sbjt_data.fillna('', inplace=True)
        crdt_data.fillna('', inplace=True)
        try:
            iff_fac_cols, iff_sbjt_cols = None, None
            if loaded_batch['out_mod'] == 'cmb':
                if datCat == 'com':
                    fac_sgmnt, sbjt_sgmnt = 'CMCF', 'CMCS'
                    iff_sbjt_cols = iff()['cmb']['corp']  # ['comm']
                    iff_fac_cols = iff()['cmb']['corpfac']  # ['fac']
                if datCat == 'con':
                    fac_sgmnt, sbjt_sgmnt = 'CNCF', 'CNCS'
                    iff_sbjt_cols = iff()['cmb']['ndvdl']  # ['cons']
                    iff_fac_cols = iff()['cmb']['ndvdlfac']  # ['fac']

            if loaded_batch['out_mod'] == 'mfi' or (
                            loaded_batch['in_mod'] == loaded_batch['out_mod'] and loaded_batch['in_mod'] == 'iff'):
                if datCat in ('com', 'mfcom'):
                    fac_sgmnt, sbjt_sgmnt = 'CMMF', 'CMMS'
                    iff_sbjt_cols = iff()['mfi']['comm']
                if datCat in ('con', 'mfcon'):
                    fac_sgmnt, sbjt_sgmnt = 'CNMF', 'CNMS'
                    iff_sbjt_cols = iff()['mfi']['cons']
                iff_fac_cols = iff()['mfi']['fac']

            if loaded_batch['out_mod'] == 'pmi':
                if datCat in ('com', 'mgcom'):
                    fac_sgmnt, sbjt_sgmnt = 'MMCF', 'MMCS'
                    iff_sbjt_cols = iff()['pmi']['comm']
                if datCat in ('con', 'mgcon'):
                    fac_sgmnt, sbjt_sgmnt = 'MNCF', 'MNCS'
                    iff_sbjt_cols = iff()['pmi']['cons']
                iff_fac_cols = iff()['pmi']['fac']

            # # a_fac_line = fac_sgmnt + '|' + meta_data['dpid'] + '||' + a_fac_line
            # crdt_data.insert(0, 'prev_dpid', '')
            # crdt_data.insert(0, 'dpid', meta_data['dpid'])
            # crdt_data.insert(0, 'segment', fac_sgmnt)
            # # a_sbjt_line = sbjt_sgmnt + '|' + meta_data['dpid'] + '|' + a_sbjt_line
            # sbjt_data.insert(0, 'dpid', meta_data['dpid'])
            # sbjt_data.insert(0, 'segment', sbjt_sgmnt)

            dataCount, idx = 0, 0
            syndi_dir = xtrcxn_area + os.path.sep + meta_data['dp_name'].split('_')[0].upper() + os.path.sep
            mk_dir(syndi_dir)
            syndi_iter, syndi_data_list = [], []
            # # todo remove later
            # if crdt_data.shape[0] > 5000:
            #     crdt_syndi_chunks = np.array_split(crdt_data, crdt_data.shape[0] // 5000)
            #
            #     for crdt_s_chunk in crdt_syndi_chunks:
            #         if meta_data['in_mod'] == 'cdt':
            #             try:
            #                 sbjt_s_data = sbjt_data[sbjt_data.cust_id.isin(crdt_s_chunk.cust_id)]
            #             except Exception as e:
            #                 mdjlog.error("{}".format(e))
            #         elif meta_data['in_mod'] in ('cmb', 'fandl', 'iff', 'mfi', 'phed', 'pmi',):
            #             try:
            #                 sbjt_s_data = sbjt_data[sbjt_data.account_no.isin(crdt_s_chunk.account_no)]
            #             except Exception as e:
            #                 mdjlog.error("{}".format(e))
            #         if sbjt_s_data is not None or not sbjt_s_data.empty:
            #             syndi_iter.append(
            #                 (crdt_s_chunk, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_s_data,
            #                  sbjt_sgmnt))
            #         mdjlog.info("\nCrdt {} \nSbjt {} \nfor {}".format(crdt_s_chunk.shape, sbjt_s_data.shape, sb2file))
            #     # with futures.ProcessPoolExecutor() as executor:
            #     with futures.ThreadPoolExecutor(max_workers=5) as executor:
            #         syndi_rez = executor.map(syndi_pairs, syndi_iter)
            # else:
            #     todo remove later

            # mdjlog.info("Pairing data for Syndicated file: {} written @ {}".format(sb2file, right_now()))
            syndi_rez = [syndi_pairs(
                (crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt, datCat))]
            # dataCount, syndi_data_list, syndicaxn_complt = syndi_pairs((ac_no, crdt_data, cust_id, dataCount, dp_name,
            #                                                             fac_sgmnt, iff_fac_cols, iff_sbjt_cols, mdjlog,
            #                                                             meta_data, sb2file, sbjt_data, sbjt_sgmnt,
            #                                                             syndicaxn_complt))
            # syndi_prts = len(syndi_rez)
            for rez in syndi_rez:
                dataCount, syndi_data_list = dataCount + rez[0], syndi_data_list + rez[1]
                # mdjlog.info("joining {} from {} parts".format(dataCount, syndi_prts))
            syndi_data_list.insert(0, hdr)
            mdjlog.info('\n' + '\n' + '#' * 128)
            structures = int(dataCount)
            sstructures = str(int(structures))
            mdjlog.info("\n\n{} {} structures processed from {} @ {}".format(sstructures, datCat.upper(), meta_data,
                                                                             right_now()))
            footer = '|'.join(['TLTL', dpid, submxn_pt, sstructures])
            # sb2file_handler.write(footer)
            syndi_data_list.append(footer)
            sb2file_handler = open(syndi_dir + sb2file, 'w')
            sb2file_handler.write('\n'.join(syndi_data_list))
            sb2file_handler.close()
            mdjlog.info("Syndicated file: {} written @ {}".format(sb2file, right_now()))
            # processes = []
            # upd8sjdt_recs.delay(loaded_batch, sbjt_data, cust_rec_col)
            # sbjt_copy = sbjt_data.copy()
            # ppns(upd8sjdt_recs, sbjt_copy, [loaded_batch, cust_rec_col])
            # mdjlog.info(sbjt_copy.shape)
            # p = mp.Process(target=upd8sjdt_recs, args=[[loaded_batch, cust_rec_col], sbjt_copy], )
            # p.start()
            # upd8crdt_recs.delay(loaded_batch, crdt_data, cust_rec_col)
            # crdt_copy = crdt_data.copy()
            # ppns(upd8crdt_recs, crdt_copy, [loaded_batch, faci_rec_col])
            # mdjlog.info(crdt_copy.shape)
            # p = mp.Process(target=upd8crdt_recs, args=[[loaded_batch, cust_rec_col], crdt_copy], )
            # p.start()
            # for p, rp in enumerate(processes):
            #     mdjlog.info("Process @# {}".format(p))
            #     rp.join()
            try:
                sb2fi_d = {'dp_name': meta_data['dp_name'], 'batch': meta_data['batch_no'], 'data_cat': datCat,
                           'cycle_ver': meta_data['cycle_ver'], 'status': 'Syndicated', 'structures': structures,
                           'facilities': crdt_data.shape[0], 'subjects': sbjt_data.shape[0], 'dpid': meta_data['dpid'],
                           'sb2file': sb2file, }
            except:
                sb2fi_d = {'dp_name': meta_data['dp_name'], 'batch': meta_data['batch_no'], 'data_cat': datCat,
                           'cycle_ver': meta_data['cycle_ver'], 'status': 'Syndicated', 'structures': structures,
                           'dpid': meta_data['dpid'], 'sb2file': sb2file, }

            sb2fi = SB2FileInfo(**sb2fi_d)
            sb2fi.save()
            if syndicaxn_complt:
                mdjlog.info("Syndication Completed Successfully idx: {} and crdt data: {}".format(idx, crdt_data.shape))
            print('#Kingdom ThinGz')
            return
        except Exception as e:
            mdjlog.error("{}".format(e))
        return
    except Exception as e:
        mdjlog.error("{}".format(e))
    return


def syndi_pairs(targs):
    crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt, dataCat = \
        targs[0], targs[1], targs[2], targs[3], targs[4], targs[5], targs[6], targs[7], targs[8]
    dpid, dp_name, syndicaxn_complt, syndi_data_list = meta_data['dpid'], meta_data['dp_name'], None, []
    mdjlog, dataCount, cust_id, ac_no, cycle_ver = get_logger(dp_name), 0, None, None, meta_data['cycle_ver']
    dataCatCheck = ('con', 'mfcon', 'mgcon')
    bulk_upd8_data = []
    if dataCat not in dataCatCheck:
        fac_type, i_type = {'corporate': 'yes'}, 'corporate_submissions'
    else:
        fac_type, i_type = {'individual': 'yes'}, 'individual_submissions'
    crdt_shape, ac_no_list, cust_id_list = crdt_data.shape[0], [], []
    mdjlog.info("PairinG data for Syndicate file {} & counting @#{}_of_{}".format(sb2file, dataCount, crdt_shape))

    if 'branch_code' in crdt_data:
        crdt_data['branch_code'] = crdt_data.branch_code.apply(lambda x: str(x)[-4:] if len(str(x)) > 4 else str(x))
    if 'branch_code' in sbjt_data:
        sbjt_data['branch_code'] = sbjt_data.branch_code.apply(lambda x: str(x)[-4:] if len(str(x)) > 4 else str(x))

    for idx in crdt_data.index:
        try:
            ac_no = crdt_data.ix[idx]['account_no']

            if str(ac_no).replace('_', '').replace('-', '').strip() != '':
                if not str(ac_no).replace('_', '').strip().replace('-', '').strip().replace(' ', '').strip().isalpha():

                    if not meta_data['data_type'] == 'combo':
                        try:
                            subj = sbjt_data.loc[sbjt_data['account_no'] == ac_no]
                            cust_id = subj['cust_id']
                        except Exception as e:
                            cust_id = crdt_data.ix[idx]['cust_id']
                            subj = sbjt_data.loc[sbjt_data['cust_id'] == cust_id]

                    else:
                        a_sbjt_line = '|'.join(
                            [str(crdt_data.ix[idx][k][0]) if k in crdt_data.ix[idx] else '' for k in iff_sbjt_cols])

                    if not subj.empty:
                        if subj.shape[0] > 1:
                            subj = subj.iloc[0]

                        if isinstance(subj, pd.core.frame.DataFrame):
                            try:
                                subj = subj.reset_index()
                                cust_id = subj['cust_id']
                                cust_id = cust_id[0] if isinstance(cust_id, pd.core.series.Series) else cust_id
                            except Exception as e:
                                pass

                        if 'account_no' not in subj:
                            subj['account_no'] = ac_no
                        try:
                            if isinstance(subj.branch_code, str):
                                branch_code = subj.branch_code
                            elif isinstance(subj.branch_code, pd.core.series.Series):
                                branch_code = subj['branch_code'][0]
                        except Exception as e:
                            if isinstance(subj.branch_code, pd.core.series.Series):
                                branch_code = subj.branch_code.values[0]
                        branch_code = str(branch_code)

                        if not branch_code:
                            branch_code = '001'

                        subj['branch_code'] = branch_code

                        crdt_i_data = crdt_data.ix[idx]
                        if not 'branch_code' in crdt_i_data:
                            crdt_i_data['branch_code'] = branch_code

                        a_fac_line = '|'.join([str(crdt_i_data[k]) if k in crdt_i_data else '' for k in iff_fac_cols])
                        del crdt_i_data
                        a_fac_line = clean_a_line(a_fac_line)
                        a_fac_line = fac_sgmnt + '|' + meta_data['dpid'] + '||' + a_fac_line

                        try:
                            a_sbjt_line = '|'.join([str(subj[k][0]) if k in subj else '' for k in iff_sbjt_cols])
                        except Exception as e:
                            # mdjlog.error("{} | customer ID {} with account no: {}".format(e, cust_id, ac_no))
                            a_sbjt_line = '|'.join([str(subj[k]) if k in subj else '' for k in iff_sbjt_cols])

                        del subj

                        a_sbjt_line = clean_a_line(a_sbjt_line)
                        a_sbjt_line = sbjt_sgmnt + '|' + meta_data['dpid'] + '|' + a_sbjt_line
                        a_fac_line = re_str(a_fac_line, dp_name)
                        a_sbjt_line = re_str(a_sbjt_line, dp_name)
                        a_fac_line = re_str(a_fac_line, dp_name)
                        a_sbjt_line = re_str(a_sbjt_line, dp_name)

                        data_line = {'status': 'Syndicated', 'sb2file': sb2file}
                        syndi_data_list.append(a_fac_line)
                        ac_no2use = str(ac_no) if isinstance(ac_no, str) else str(ac_no[0])
                        # ndx_line = {'_index': es_i, '_op_type': 'update', '_type': 'facility_submissions',
                        ndx_line = {'_index': es_i, '_op_type': 'update', '_type': 'facility_submissions',
                                    '_id': "-".join((dpid, ac_no2use, str(cycle_ver))),
                                    'doc': {**data_line, **{'cust_type': i_type.split('_')[0]}, **fac_type}}
                        bulk_upd8_data.append(ndx_line)
                        ac_no_list.append(str(ac_no2use))
                        syndi_data_list.append(a_sbjt_line)
                        custID2use = str(cust_id) if isinstance(cust_id, str) else str(cust_id[0])
                        cust_id_list.append(str(custID2use))
                        custID2use = custID2use if meta_data['in_mod'] in cdt_udf_modes else ac_no2use
                        ndx_line = {'_index': es_i, '_op_type': 'update', '_type': i_type,
                                    '_id': "-".join((dpid, custID2use, str(cycle_ver))), 'doc': data_line}
                        bulk_upd8_data.append(ndx_line)
                        del a_fac_line, a_sbjt_line

                        dataCount += 1
                        syndicaxn_complt = True

                        log_syndi_pro(crdt_shape, dataCount, mdjlog, sb2file)
                    else:
                        # mdjlog.info("account no: {} guess with no customer".format(ac_no))
                        pass

            if not idx% 55 or  not idx% 77 or  not idx% 88:
                std_out('PairingG {} data for file {} @ index {} of {}\t'.format(dp_name, sb2file, idx, crdt_shape))
        except Exception as e:
            mdjlog.error("{} | customer ID {} with account no: {}".format(e, cust_id, ac_no))

        del ac_no, cust_id
        # bar.update(idx)

    try:
        bulk_upd8_gnrt = gener8(bulk_upd8_data)
        helpers.bulk(es, bulk_upd8_gnrt)  #
    except Exception as e:
        # pass
        mdjlog.error(e)
    ac_count, cust_count = len(set(ac_no_list)), len(set(cust_id_list))
    mdjlog.info(
        "Syndicate file {} will contain pairs from {} facilities and {} subjects".format(sb2file, ac_count, cust_count))

    return dataCount, syndi_data_list, syndicaxn_complt


def gener8(l):
    for e in l: yield e


def log_syndi_pro(crdt_shape, dataCount, mdjlog, sb2file):
    if crdt_shape >= 5000:
        if not dataCount % 5137:
            mdjlog.info("PairinG data file {} & counting @#{}_of_{}".format(sb2file, dataCount, crdt_shape))
    elif crdt_shape >= 500 and crdt_shape < 5000:
        if not dataCount % 1357:
            mdjlog.info("PairinG data file {} & counting @#{}_of_{}".format(sb2file, dataCount, crdt_shape))
    elif crdt_shape < 500:
        if not dataCount % 135:
            mdjlog.info("PairinG data file {} & counting @#{}_of_{}".format(sb2file, dataCount, crdt_shape))


# @app.task(name='upd8sbdt_recs')
def upd8sjdt_recs(largs, sbdt_data):
    loaded_batch, cust_rec_col = largs[0], largs[1]
    cycle_ver, mdjlog = loaded_batch['cycle_ver'], get_logger(loaded_batch['dp_name'])
    for idx, sbjt_d in sbdt_data.interrows():
        if loaded_batch['in_mod'] in ('cmb', 'fandl', 'iff', 'mfi', 'pmi',):  # , upsert=True
            try:
                ac_no = sbjt_d.account_no
                cust_rec_col.update(
                    {"dpid": loaded_batch['dpid'], "account_no": ac_no, "cycle_ver": cycle_ver},
                    {'$set': {'status': 'Syndicated'}}
                )
            except Exception as e:
                mdjlog.error("{} | customer record: {}".format(e, sbjt_d))
        else:
            cust_id = sbjt_d.cust_id
            try:
                cust_rec_col.update(
                    {"dpid": loaded_batch['dpid'], "cust_id": cust_id, "cycle_ver": cycle_ver},
                    {'$set': {'status': 'Syndicated'}}
                )  # , upsert=True
            except Exception as e:
                cust_rec_col.update(
                    {"dpid": loaded_batch['dpid'], "cust_id": 'NOMETER', "cycle_ver": cycle_ver},
                    {'$set': {'status': 'Syndicated'}}
                )


# @app.task(name='upd8crdt_recs')
def upd8crdt_recs(largs, crdt_data):
    loaded_batch, faci_rec_col = largs[0], largs[1]
    cycle_ver, mdjlog = loaded_batch['cycle_ver'], get_logger(loaded_batch['dp_name'])
    for idx, crdt in crdt_data.interrows():
        try:
            ac_no = crdt.account_no
            faci_rec_col.update(
                {"dpid": loaded_batch['dpid'], "account_no": ac_no, "cycle_ver": cycle_ver},
                {'$set': {'status': 'Syndicated'}}
            )
        except Exception as e:
            mdjlog.error("{} | facility record: {}".format(e, crdt))


def syndic8data(crdt_data, sbjt_data, meta_data, ctgry_dtls, datCat, dp_meta):
    mdjlog, sb2files, tasks = get_logger(meta_data['dp_name']), [], []
    file_dtls = DataFiles.objects(cycle_ver=meta_data['cycle_ver'], dpid=meta_data['dpid'], status='Loaded').first()
    try:
        dp_name = meta_data['dp_name'].split('_')[0].lower()
        instCat = dp_meta[dp_name][2]
        institution_cat, submxn_pt = dp_meta[dp_name][2], dp_meta[dp_name][1]
        sgmnt = instCat + datCat if instCat not in ('cb',) else datCat
        d8reported = file_dtls['date_reported']
        dpid = meta_data['dpid']
        syndi_dir = xtrcxn_area + os.path.sep + meta_data['dp_name'].split('_')[0].upper() + os.path.sep
        mk_dir(syndi_dir)
        data_size, runnin_chunks_list = crdt_data.shape[0], []

        if data_size > chunk_size:
            # syndi_chunk_pro(crdt_data, ctgry_dtls, d8reported, datCat, data_size, dp_meta, dpid, mdjlog, meta_data,
            #                 runnin_chunks_list, sbjt_data, sgmnt, submxn_pt)
            p = mp.Process(target=syndi_chunk_pro,
                           args=(crdt_data, ctgry_dtls, d8reported, datCat, data_size, dp_meta, dpid, mdjlog, meta_data,
                                 runnin_chunks_list, sbjt_data, sgmnt, submxn_pt), )
            p.start()

            # pool.close()
            # pool.join()
        else:
            try:
                mdjlog.info(
                    "\n\nsyndicating. .. \n\t{} credits with \n\t{} subjects".format(crdt_data.shape, sbjt_data.shape))
            except:
                mdjlog.info("\n\nsyndicating. .. \n\t{} combined data".format(crdt_data.shape))

            data_list_fn, data_list_hdr = syndi_params(ctgry_dtls, d8reported, dpid, mdjlog, sgmnt, submxn_pt)

            # syndidata((crdt_data, sbjt_data, meta_data, dp_meta, datCat, data_list_hdr, data_list_fn))
            targz = (crdt_data, sbjt_data, meta_data, dp_meta, datCat, data_list_hdr, data_list_fn)
            p = mp.Process(target=syndidata, args=[targz])
            p.start()
            # snglpool = mp.Pool(processes=1)
            # snglpool.map(syndidata, [(crdt_data, sbjt_data, meta_data, dp_meta, datCat, data_list_hdr, data_list_fn)])

        # if len(processes) > 1:
        #     for p, rp in enumerate(processes):
        #         mdjlog.info("Process @# {}${}".format(p, os.getpid()))
        #         # rp.close()
        #         rp.join()
        #         rp.terminate()

        print('#YHVH')
        return

    except Exception as e:
        mdjlog.error("{}".format(e))


def syndi_chunk_pro(crdt_data, ctgry_dtls, d8reported, datCat, data_size, dp_meta, dpid, mdjlog, meta_data,
                    runnin_chunks_list, sbjt_data, sgmnt, submxn_pt):
    rounds, size_done = data_size // chunk_size + 1, 0
    rcount, size2use = rounds - 1, round(data_size / rounds)
    crdt_data_chunks = np.array_split(crdt_data, rounds)
    for idx, crdt_data_chunk in enumerate(crdt_data_chunks):
        try:
            # if idx < 2:  # todo remove after debug
            ndx_column = 'cust_id' if meta_data['in_mod'] in ('cdt', 'udf',) else 'account_no'
            sbjt_df = sbjt_data[sbjt_data[ndx_column].isin(crdt_data_chunk[ndx_column])]
            mdjlog.info("\n\nsyndicating CHUNK. .. \n\t{} credits with \n\t{} subjects".format(crdt_data_chunk.shape,
                                                                                               sbjt_df.shape))
            data_list_fn, data_list_hdr = syndi_params(ctgry_dtls, d8reported, dpid, mdjlog, sgmnt, submxn_pt)

            runnin_chunks_list.append(
                (crdt_data_chunk, sbjt_df, meta_data, dp_meta, datCat, data_list_hdr, data_list_fn))
            size_done += size2use
            mdjlog.info(
                "\nQueuinG! #{} of {} Counts: {} of {} of {}".format(idx, rcount, size2use, size_done, data_size))
            count_down(None, 1)
        except Exception as e:
            mdjlog.error(e)
    pool = mp.Pool(processes=mpcores)
    pool.map(syndidata, runnin_chunks_list)
    return


def syndi_params(ctgry_dtls, d8reported, dpid, mdjlog, sgmnt, submxn_pt):
    process_date = datetime.utcnow().strftime('%d-%b-%Y')
    process_time = datetime.utcnow().strftime('%H%M%S')
    data_list_fn = '-'.join([dpid, ctgry_dtls[sgmnt][0].upper(), d8reported, process_time]) + '.dlt'
    data_list_fn = data_list_fn.upper()
    data_list_hdr = '|'.join(['HDHD', dpid, submxn_pt, d8reported, process_time, process_date, ctgry_dtls[sgmnt][1]])
    mdjlog.info("\nabout to . .. syndicate {}".format(data_list_fn))
    return data_list_fn, data_list_hdr

# nu_of_chunks = crdt_data.shape[0] // chunk_size + 1
# try:
#     crdt_data['cust_id'] = crdt_data.cust_id.apply(
#         lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
# except:
#     pass
# todo not sure this works so well
# parts = nu_of_chunks
# pool = mp.Pool(nu_of_chunks)
# crdt_data_chunks = np.array_split(crdt_data, parts)
# processes = []
# crdt_packs = len(crdt_data_chunks)
# mdjlog.info(
#     "should have {} {} from the submission".format(crdt_packs, 'processes' if crdt_packs > 1 else 'process'))
# if parts > 1:
#     # for idx, crdt_data_chunk in enumerate(crdt_data_chunks):
#     #     process_date = datetime.utcnow().strftime('%d-%b-%Y')
#     #     process_time = datetime.utcnow().strftime('%H%M%S')
#     #     data_list_fn = '-'.join([dpid, ctgry_dtls[sgmnt][0].upper(), process_date, process_time]) + '.dlt'
#     #     data_list_hdr = '|'.join(
#     #         ['HDHD', dpid, submxn_pt, d8reported, process_time, process_date, ctgry_dtls[sgmnt][1]])
#     #
#     #     data_list_fn = data_list_fn.upper()
#     #     mdjlog.info("about to . .. syndicate {}".format(data_list_fn))
#     #     if meta_data['in_mod'] == 'cdt':
#     #         try:
#     #             sbjt_data_chunk = sbjt_data[sbjt_data.cust_id.isin(crdt_data_chunk.cust_id)]
#     #         except Exception as e:
#     #             mdjlog.error("{}".format(e))
#     #     elif meta_data['in_mod'] in ('cmb', 'fandl', 'iff', 'mfi', 'phed', 'pmi',):
#     #         try:
#     #             sbjt_data_chunk = sbjt_data[sbjt_data.account_no.isin(crdt_data_chunk.account_no)]
#     #         except Exception as e:
#     #             mdjlog.error("{}".format(e))
#     #     sb2files.append(data_list_fn)
#
#         # # todo this need to be on both levels
#         # if meta_data['data_type'] == 'combo':
#         #     mdjlog.info("combo data chunk @#{} : {}".format(idx, crdt_data_chunk.shape))
#         #     p = mp.Process(target=syndidata,
#         #                    args=[crdt_data_chunk, None, meta_data, dp_meta, datCat, data_list_hdr,
#         #                          data_list_fn], )
#         #     p.start()
#         #     mdjlog.info("Starting a process. ..")
#         #     processes.append(p)
#         #     time.sleep(1)
#         # else:
#         if sbjt_data_chunk is not None or not sbjt_data_chunk.empty:
#             mdjlog.info(" credit data chunk @#{} : {}".format(idx, crdt_data_chunk.shape))
#             mdjlog.info("subject data chunk @#{} : {}".format(idx, sbjt_data_chunk.shape))
#             p = mp.Process(target=syndidata,
#                            args=[crdt_data_chunk, sbjt_data_chunk, meta_data, dp_meta, datCat,
#                                  data_list_hdr,
#                                  data_list_fn], )
#             p.start()
#             mdjlog.info("Starting a process. ..")
#             processes.append(p)
#
#             time.sleep(1)
#         else:
#             mdjlog.info("subject data chunk @#{} : {} is empty".format(idx, sbjt_data_chunk.shape))
# else:
