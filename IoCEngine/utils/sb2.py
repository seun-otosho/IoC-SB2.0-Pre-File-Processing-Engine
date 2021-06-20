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
from IoCEngine.commons import (fig_str, iff_sb2_modes, count_down, get_logger, mk_dir,
                               right_now, gs, ps, std_out, time_t)
from IoCEngine.config.pilot import chunk_size, es, es_i, es_ii, mpcores
from IoCEngine.utils.data_modes import iff
from IoCEngine.utils.db2data import grntr_data, prnc_data

from IoCEngine.utils.file import DataFiles, SB2FileInfo
from auto_bots import upd8batch

a, b, c = 1, 2, 3


# todo override for dev and test
# mpcores = 3


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
    df_str = df_str.replace('"', '')
    df_str = df_str.replace('.0|', '|') if '.0|' in df_str else df_str
    return df_str


def fix_fac_missing(crdt_data, meta_data):
    crdt_data.insert(0, 'prev_dpid', '')
    crdt_data.insert(0, 'dpid', meta_data['dpid'])


def syndidata(lz):
    crdt_data, sbjt_data, meta_data, dp_meta, datCat, hdr, sb2file, syndi_dir = lz[:8]
    b2u, mdjlog, dpid, syndicaxn_complt = lz[7], get_logger(meta_data['dp_name']), meta_data['dpid'], False

    try:
        chunk_mode = lz[9] if lz[9] is not None else None
    except:
        chunk_mode = None
    try:
        dp_name = meta_data['dp_name'].lower()  # , syndi_data_list, []
        institution_cat, submxn_pt = dp_meta[dp_name][2], dp_meta[dp_name][1]
        # file_dtls = DataFiles.objects(dpid=meta_data['dpid'], cycle_ver=meta_data['cycle_ver'], status='Loaded').first()
        meta_data.reload()

        loaded_batch = meta_data
        sbjt_data.fillna('', inplace=True)
        crdt_data.fillna('', inplace=True)

        loaded_gs = [sgmnt for sgmnt in [b for b in b2u] if sgmnt.segments[0] in gs or gs[0] in sgmnt][0]
        grntr, gs_data = grntr_data(meta_data['dpid'], loaded_gs, gs[0])
        gs_data = gs_data[gs_data.account_no.isin(crdt_data.account_no)]
        grntr = not gs_data.empty

        if datCat == 'com':
            loaded_ps = [sgmnt for sgmnt in [b for b in b2u] if sgmnt.segments[0] in ps or ps[0] in sgmnt][0]
            prnc, ps_data = prnc_data(meta_data['dpid'], loaded_ps, ps[0])
            ps_data = ps_data[ps_data.cust_id.isin(sbjt_data.cust_id)]
            prnc = not ps_data.empty

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

            dataCount, idx = 0, 0
            syndi_dir = xtrcxn_area + os.path.sep + meta_data['dp_name'].split('_')[0].upper() + os.path.sep
            mk_dir(syndi_dir)
            syndi_iter, syndi_data_list = [], []
            syndi_complt, iocnow, syndi_crdt_data, syndi_sbjt_data = None, None, None, None

            # mdjlog.info("Pairing data for Syndicated file: {} written @ {}".format(sb2file, right_now()))
            syndi_pair_args = (
                crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt, datCat,
                syndi_dir
            )
            syndi_rez = [syndi_pairs(syndi_pair_args)]

            for rez in syndi_rez:
                dataCount, syndi_data_list = dataCount + rez[0], syndi_data_list + rez[1]
                syndi_complt, iocnow, syndi_crdt_data, syndi_sbjt_data = rez[2], rez[3], rez[4], rez[5]
                # mdjlog.info("joining {} from {} parts".format(dataCount, syndi_prts))
            syndi_data_list.insert(0, hdr)
            mdjlog.info('\n' + '\n' + '#' * 128)
            structures = int(dataCount)
            sstructures = str(int(structures))
            mdjlog.info(f"\n\n{sstructures} {datCat.upper()} structures processed from {meta_data} @ {right_now()}")
            footer = '|'.join(['TLTL', dpid, submxn_pt, sstructures])
            # sb2file_handler.write(footer)
            syndi_data_list.append(footer)
            sb2file_handler = open(syndi_dir + sb2file, 'w')
            sb2file_handler.write('\n'.join(syndi_data_list))
            sb2file_handler.close()
            mdjlog.info("Syndicated file: {} written @ {}".format(sb2file, right_now()))

            try:
                # crdtp = mp.Process(target=upd8DFstatus, args=[syndi_crdt_data, dp_name, es_i, 'submissions'])
                # crdtp.start()
                upd8DFstatus(syndi_crdt_data, dp_name, es_i, 'submissions')
            except Exception as e:
                mdjlog.error(e)

            try:
                # sbjtp = mp.Process(target=upd8DFstatus, args=[syndi_sbjt_data, dp_name, es_i, 'submissions'])
                # sbjtp.start()
                upd8DFstatus(syndi_sbjt_data, dp_name, es_i, 'submissions')
            except Exception as e:
                mdjlog.error(e)

            try:
                # iocnowp = mp.Process(target=currindx, args=[iocnow, mdjlog])
                # iocnowp.start()
                currindx(iocnow, mdjlog)
            except Exception as e:
                mdjlog.error(e)

            # count_down(None, )
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
            if chunk_mode is None:
                upd8batch(loaded_batch, b2u)  # meta_data
            if syndicaxn_complt:
                mdjlog.info(f"Syndication Completed Successfully : {idx=} and | {crdt_data.shape=}")
            print('#Kingdom ThinGz')
            return
        except Exception as e:
            mdjlog.error("{}".format(e))
        return
    except Exception as e:
        mdjlog.error("{}".format(e))
    return


def syndi_pairs(targs):
    crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt, dataCat = targs[:9]
    # syndi_dir, iocnow = targs[9], []
    dpid, dp_name, syndicaxn_complt, syndi_data_list = meta_data['dpid'], meta_data['dp_name'], None, []
    mdjlog, dataCount, cust_id, ac_no, cycle_ver = get_logger(dp_name), 0, None, None, meta_data['cycle_ver']
    # dataCatCheck = ('con', 'mfcon', 'mgcon')
    # bulk_upd8_data = []
    # if dataCat not in dataCatCheck:
    #     fac_type, i_type = {'corporate': 'yes'}, 'corporate_submissions'
    # else:
    #     fac_type, i_type = {'individual': 'yes'}, 'individual_submissions'
    ndx_column = 'account_no' if meta_data['in_mod'] in iff_sb2_modes else 'cust_id'
    crdt_data['cycle_ver'], sbjt_data['cycle_ver'] = meta_data['cycle_ver'], meta_data['cycle_ver']
    # crdt_data = crdt_data[crdt_data[ndx_column].isin(sbjt_data[ndx_column])]  # may remove
    crdt_shape, ac_no_list, cust_id_list = crdt_data.shape[0], [], []
    mdjlog.info("PairinG data for Syndicate file {} & counting @#{}_of_{}".format(sb2file, dataCount, crdt_shape))
    # fix branch code
    if 'branch_code' in crdt_data:
        try:
            crdt_data['branch_code'] = crdt_data.branch_code.apply(lambda x: str(x)[-4:] if len(str(x)) > 4 else str(x))
        except Exception as e:
            mdjlog.error(e)
            crdt_data['branch_code'] = '001'
    if 'branch_code' in sbjt_data:
        try:
            sbjt_data['branch_code'] = sbjt_data.branch_code.apply(lambda x: str(x)[-4:] if len(str(x)) > 4 else str(x))
        except Exception as e:
            mdjlog.error(e)
            sbjt_data['branch_code'] = '001'

    # crdt_data.to_excel(syndi_dir+'CrdtData{}.xlsx'.format(cycle_ver))
    # sbjt_data.to_excel(syndi_dir+'SbjtData{}.xlsx'.format(cycle_ver))

    # if not meta_data['data_type'] == 'combo':
    #     try:
    #         subj = sbjt_data.loc[sbjt_data['account_no'] == ac_no]
    #         cust_id = subj['cust_id']
    #     except Exception as e:
    #         cust_id = crdt_data.ix[idx]['cust_id']
    #         subj = sbjt_data.loc[sbjt_data['cust_id'] == cust_id]
    #
    # else:
    #     #todo NEED to be implemented for combined data
    #     # a_sbjt_line = '|'.join(
    #     #     [str(crdt_data.ix[idx][k][0]) if k in crdt_data.ix[idx] else '' for k in iff_sbjt_cols])
    #     pass

    if meta_data['in_mod'] not in iff_sb2_modes:
        branch_code_df = sbjt_data[['cust_id', 'branch_code']]
        account_no_df = crdt_data[['cust_id', 'account_no']]
        crdt_data = pd.merge(crdt_data, branch_code_df, on='cust_id', how='inner')
        iff_sbjt_data = pd.merge(account_no_df, sbjt_data, on='cust_id', how='inner')
    else:
        iff_sbjt_data = sbjt_data

    # missing_fac_cols = list(set(iff_fac_cols) - set(crdt_data.columns))
    #     # if len(iff_fac_cols) > len(crdt_data.column) else list(set(crdt_data.column) - set(iff_fac_cols))
    #
    # missing_sbjt_cols = list(set(iff_sbjt_cols) - set(sbjt_data.columns))
    #     # if len(iff_sbjt_cols) > len(sbjt_data.column) else list(set(sbjt_data.column) - set(iff_sbjt_cols))

    iff_crdt_data = crdt_data.reindex(columns=list(iff_fac_cols))
    iff_sbjt_data = iff_sbjt_data.reindex(columns=list(iff_sbjt_cols))

    for col in iff_fac_cols:
        if col not in iff_crdt_data.columns:
            iff_crdt_data[col] = ''
    for col in iff_sbjt_cols:
        if col not in iff_sbjt_data.columns:
            iff_sbjt_data[col] = ''

    iff_crdt_data.branch_code.fillna('001', inplace=True)
    iff_sbjt_data.branch_code.fillna('001', inplace=True)

    iff_crdt_data.insert(0, 'prev_dpid', '')
    iff_crdt_data.insert(0, 'dpid', meta_data['dpid'])
    iff_crdt_data.insert(0, 'segment', fac_sgmnt)
    iff_sbjt_data.insert(0, 'dpid', meta_data['dpid'])
    iff_sbjt_data.insert(0, 'segment', sbjt_sgmnt)
    mdjlog.info(iff_crdt_data.shape)
    # iff_crdt_data.to_excel(syndi_dir+'IFFCrdt{}Data.xlsx'.format(cycle_ver))
    mdjlog.info(iff_sbjt_data.shape)
    # iff_sbjt_data.to_excel(syndi_dir+'IFFSbjt{}Data.xlsx'.format(cycle_ver))
    # re_dupz(iff_crdt_data, 'account_no', mdjlog)
    # re_dupz(iff_sbjt_data, 'account_no', mdjlog)
    # iff_crdt_data.to_excel(syndi_dir+'IFFCrdtData{}NoDupz.xlsx'.format(cycle_ver))
    # iff_sbjt_data.to_excel(syndi_dir+'IFFSbjtData{}NoDupz.xlsx'.format(cycle_ver))
    iff_crdt_data.fillna('', inplace=True)
    iff_sbjt_data.fillna('', inplace=True)
    mdjlog.info(iff_crdt_data.shape)
    mdjlog.info(iff_sbjt_data.shape)

    # mdjlog.info("meta_data['in_mod'] : {}".format(meta_data['in_mod']))
    # mdjlog.info("meta_data['out_mod'] : {}".format(meta_data['out_mod']))

    # if meta_data['in_mod'] in ('cdt',) and meta_data['out_mod'] not in ('mfi',):
    #     mdjlog.info('checking. ..')
    #     iff_crdt_data['int_overdue_amt'] = 0
    #     iff_crdt_data['int_overdue_days'] = 0
    #     mdjlog.info('fix3D int_overdue_amt & int_overdue_days fields not provided in CDT Data File Submissions')

    # iff_sbjt_data.to_csv('{}_SBJT_{}.txt'.format(dp_name, str(cycle_ver)), sep='|', index=False)

    for idx in iff_crdt_data.index:
        try:
            try:
                cidx = int(idx) if str(idx).isdigit() or isinstance(idx, (int, np.int64)) else int(
                ''.join([s for s in list(idx) if s.isdigit()]))
            except Exception as e:
                mdjlog.error(e)
            #     cidx = cidx if isinstance(cidx, int) else float(cidx)
            # todo ac_no, _idx = iff_crdt_data.ix[idx]['account_no'], not cidx % 55 or not cidx % 77 or not cidx % 88
            ac_no, _idx = iff_crdt_data.iloc[idx]['account_no'], not cidx % 55 or not cidx % 77 or not cidx % 88
            #
            # if str(ac_no).replace('_', '').replace('-', '').strip() != '':
            #     if not str(ac_no).replace('_', '').strip().replace('-', '').strip().replace(' ', '').strip().isalpha():
            #
            try:
                subj = iff_sbjt_data.loc[iff_sbjt_data['account_no'] == ac_no]
                cust_id = subj['cust_id']
            except Exception as e:
                mdjlog.error(e)
                # cust_id = iff_crdt_data.ix[idx]['cust_id']
                # subj = iff_sbjt_data.loc[iff_sbjt_data['cust_id'] == cust_id]
            # if not subj.empty:
            # if subj.shape[0] > 1:
            #     subj = subj.iloc[0]

            # if isinstance(subj, pd.core.frame.DataFrame):
            #     try:
            #         subj = subj.reset_index()
            #         cust_id = subj['cust_id']
            #         cust_id = cust_id[0] if isinstance(cust_id, pd.core.series.Series) else cust_id
            #     except Exception as e:
            #         pass

            # if 'account_no' not in subj:
            #     subj['account_no'] = ac_no
            # try:
            #     if 'branch_code' in subj:
            #         if isinstance(subj.branch_code, str):
            #             branch_code = subj.branch_code
            #         elif isinstance(subj.branch_code, pd.core.series.Series):
            #             branch_code = subj['branch_code'][0]
            # except Exception as e:
            #     if 'branch_code' in subj:
            #         if isinstance(subj.branch_code, pd.core.series.Series):
            #             branch_code = subj.branch_code.values[0]
            #     else:
            #         branch_code = '001'
            #
            # branch_code = str(branch_code)

            # if not branch_code:
            #     branch_code = '001'
            #
            # subj['branch_code'] = branch_code

            crdt_i_data = iff_crdt_data.iloc[idx] # todo iff_crdt_data.ix[idx]
            # if not 'branch_code' in crdt_i_data:
            #     crdt_i_data.loc['branch_code'] = branch_code

            a_fac_line = '|'.join(str(c) for c in crdt_i_data.tolist())
            # del crdt_i_data
            a_fac_line = clean_a_line(a_fac_line)
            # a_fac_line = fac_sgmnt + '|' + meta_data['dpid'] + '||' + a_fac_line

            try:
                a_sbjt_line = '|'.join(str(c) for c in subj.iloc[0].tolist())
                # a_sbjt_line = '|'.join(str(c) for c in subj.values.tolist()[0])
            except Exception as e:
                # mdjlog.error("{} | customer ID {} with account no: {}".format(e, cust_id, ac_no))
                a_sbjt_line = '|'.join(str(c) for c in subj.tolist())

            # crdt_i_data['status'], crdt_i_data['sb2file'] = 'Syndicated', sb2file
            # subj['status'], subj['sb2file'] = 'Syndicated', sb2file

            a_sbjt_line = clean_a_line(a_sbjt_line)
            # a_sbjt_line = sbjt_sgmnt + '|' + meta_data['dpid'] + '|' + a_sbjt_line
            a_fac_line = re_str(a_fac_line, dp_name)
            a_sbjt_line = re_str(a_sbjt_line, dp_name)
            # a_fac_line = re_str(a_fac_line, dp_name)
            # a_sbjt_line = re_str(a_sbjt_line, dp_name)

            syndi_data_list.append(a_fac_line)
            custID2use = str(cust_id) if isinstance(cust_id, str) else str(cust_id.iloc[0])
            cust_id_list.append(str(custID2use))
            syndi_data_list.append(a_sbjt_line)
            ac_no2use = str(ac_no) if isinstance(ac_no, str) else str(ac_no.iloc[0])
            ac_no_list.append(str(ac_no2use))

            dataCount += 1
            syndicaxn_complt = True

            log_syndi_pro(crdt_shape, dataCount, mdjlog, sb2file)
            # else:
            #     # mdjlog.info("account no: {} guess with no customer".format(ac_no))
            #     pass

            # if not idx % 55 or not idx % 77 or not idx % 88:
            if _idx:
                std_out('PairinG {} data for file {} @ index {} of {} @{}'.format(dp_name, sb2file, idx, crdt_shape,
                                                                                  time_t()))
        except Exception as e:
            mdjlog.error("{} | customer ID {} with account no: {}".format(e, cust_id, ac_no))

            # del ac_no, cust_id

    # todo begin  fix
    del iff_crdt_data, iff_sbjt_data
    syndi_crdt_data = crdt_data.loc[crdt_data['account_no'].isin(ac_no_list)]
    syndi_crdt_data['sb2file'], syndi_crdt_data['status'] = sb2file, 'Syndicated'
    syndi_crdt_data['_index'], syndi_crdt_data['_type'] = es_i, 'submissions'
    # , syndi_crdt_data['_op_type'] , 'update'
    syndi_crdt_data = syndi_crdt_data[['_index', '_type', '_id', 'sb2file', 'status']]
    # crdtp = mp.Process(target=upd8DFstatus, args=[syndi_crdt_data, dp_name, es_i, 'submissions'])
    # # upd8DFstatus(syndi_crdt_data, dp_name, es_i, 'submissions')
    # try:
    #     crdtp.start()
    # except Exception as e:
    #     mdjlog.error(e)
    # bulk_upd8data = [
    #     {
    #         '_index': es_i, '_op_type': 'update', '_type': 'submissions', 'doc': {}
    #     } for i, c in enumerate(syndi_crdt_data.to_dict('records'))
    # ]

    # data_line = {'status': 'Syndicated', 'sb2file': sb2file}
    # ndx_line = {'_index': es_i, '_op_type': 'update', '_type': 'facility_submissions',
    # ndx_line = {'_index': es_i, '_op_type': 'update', '_type': 'submissions',
    #             'submission': submission_type_dict[dataCat], 'type': 'facility',
    #             '_id': crdt_i_data['_id'], 'doc': data_line}
    # bulk_upd8_data.append(ndx_line)
    # bulk_upd8_data = syndi_crdt_data.to_dict('records')

    #
    syndi_sbjt_data = sbjt_data.loc[sbjt_data['cust_id'].isin(cust_id_list)]
    syndi_sbjt_data['sb2file'], syndi_sbjt_data['status'] = sb2file, 'Syndicated'
    syndi_sbjt_data['_index'], syndi_sbjt_data['_type'] = es_i, 'submissions'
    # , syndi_sbjt_data['_op_type'] , 'update'
    syndi_sbjt_data = syndi_sbjt_data[['_index', '_type', '_id', 'sb2file', 'status']]
    # sbjtp = mp.Process(target=upd8DFstatus, args=[syndi_sbjt_data, dp_name, es_i, 'submissions'])
    # # upd8DFstatus(syndi_sbjt_data, dp_name, es_i, 'submissions')
    # try:
    #     sbjtp.start()
    # except Exception as e:
    #     mdjlog.error(e)
    # custID2use = custID2use if meta_data['in_mod'] in cdt_udf_modes else ac_no2use
    # ndx_line = {'_index': es_i, '_op_type': 'update', '_type': i_type,
    # id_ = subj['_id'] if isinstance(subj['_id'], str) else subj['_id'][0]
    # ndx_line = {'_index': es_i, '_op_type': 'update', '_type': 'submissions',
    #             'submission': submission_type_dict[dataCat], 'type': data_type_dict[dataCat],
    #             '_id': id_, 'doc': data_line}
    # bulk_upd8_data.append(ndx_line)
    # bulk_upd8_data = bulk_upd8_data + syndi_sbjt_data.to_dict('records')

    # crdt_i_data['_id'] = '-'.join(crdt_i_data['_id'].split('-')[:-1]) if isinstance(
    #     crdt_i_data['_id'], str) else '-'.join(crdt_i_data['_id'][0].split('-')[:-1])
    # subj['_id'] = '-'.join(subj['_id'].split('-')[:-1]) if isinstance(
    #     subj['_id'], str) else '-'.join(subj['_id'][0].split('-')[:-1])
    #
    # crdt_i_data['_index'], subj['_index'] = es_ii, es_ii
    # crdt_i_data['_type'], subj['_type'] = 'current', 'current'

    crdt_data = crdt_data.loc[crdt_data['account_no'].isin(ac_no_list)]
    sbjt_data = sbjt_data.loc[sbjt_data['cust_id'].isin(cust_id_list)]

    crdt_data['_index'], sbjt_data['_index'] = es_ii, es_ii
    crdt_data['sb2file'], crdt_data['status'] = sb2file, 'Syndicated'
    sbjt_data['sb2file'], sbjt_data['status'] = sb2file, 'Syndicated'
    crdt_data['submission'] = 'corporate' if 'com' in dataCat else 'individual'
    crdt_data['_type'], sbjt_data['_type'] = 'current', 'current'

    crdt_data['_id'] = crdt_data._id.apply(lambda x: '-'.join(x.split('-')[:-1]))
    sbjt_data['_id'] = sbjt_data._id.apply(lambda x: '-'.join(x.split('-')[:-1]))

    # iocnow.append(normalize_dict(crdt_i_data))
    iocnow = crdt_data.to_dict('records')
    # iocnow.append(normalize_dict(subj))
    iocnow = iocnow + sbjt_data.to_dict('records')
    # del a_fac_line, a_sbjt_line, crdt_i_data, subj
    # todo end fix

    ac_count, cust_count = len(set(ac_no_list)), len(set(cust_id_list))
    mdjlog.info(
        "Syndicate file {} will contain pairs from {} facilities and {} subjects".format(sb2file, ac_count, cust_count))
    # try:
    #     # if log_check(crdt_shape, dataCount):
    #     # # option 0
    #     # p = mp.Process(target=upd8pair3D, args=[bulk_upd8_data, iocnow, mdjlog])
    #     # p.start()
    #     # option 0
    #     p = mp.Process(target=currindx, args=[iocnow, mdjlog])
    #     p.start()
    #     # # option 1
    #     # # with futures.ThreadPoolExecutor() as executor:
    #     # with futures.ProcessPoolExecutor() as executor:
    #     #     executor.map(upd8pair3D, [(bulk_upd8_data, iocnow, mdjlog)])
    #     # # option 2
    #     # # upd8pair3D(bulk_upd8_data, iocnow, mdjlog)
    #     # currindx(iocnow, mdjlog)
    # except Exception as e:
    #     mdjlog.error(e)
    del crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt, dataCat
    del targs
    return dataCount, syndi_data_list, syndicaxn_complt, iocnow, syndi_crdt_data, syndi_sbjt_data


def re_dupz(df, ndx, mdjlog):
    ndx_nunique = df[ndx].nunique()
    dups = df.shape[0] - ndx_nunique
    if dups > 0:
        mdjlog.warn(
            'dropping {} duplicate {} from {} to have {} unique records'.format(dups, ndx, df.shape[0], ndx_nunique))
        df.drop_duplicates(subset=[ndx], keep='first', inplace=True)
    else:
        mdjlog.info("no duplicates")


def normalize_dict(ivar):
    d = ivar.to_dict('records') if isinstance(ivar, pd.DataFrame) else ivar.to_dict()
    d = d if isinstance(d, dict) else d[0]
    d = {i: d[i] for i in d if d[i] not in ('', None,)}
    d.update({i: d[i].item() for i in d if isinstance(d[i], np.int64)})
    return d


def currindx(iocnow, mdjlog):
    try:
        iocnow_gnrt = gener8(iocnow)
        helpers.bulk(es, iocnow_gnrt)
        es.indices.refresh()
    except Exception as e:
        # count_down(None, 12)
        # mdjlog.error(e)
        helpers.bulk(es, iocnow_gnrt)
        es.indices.refresh()


# def upd8pair3D(bulk_upd8_data, iocnow, mdjlog):
#     try:
#         upd8DFstatus()
#         bulk_upd8_gnrt = gener8(bulk_upd8_data)
#         helpers.bulk(es, bulk_upd8_gnrt)  #
#     except Exception as e:
#         mdjlog.error(e)
#     try:
#         upd8DFstatus()
#         iocnow_gnrt = gener8(iocnow)
#         helpers.bulk(es, iocnow_gnrt)  #
#     except Exception as e:
#         mdjlog.error(e)


def upd8DFstatus(df, dp_name, es_i, es_t):
    mdjlog = get_logger(dp_name)
    bulk_upd8_data = []
    for idx in df.index:
        try:
            rwd = df.iloc[idx] # todo df.ix[idx]
            # if isinstance(rwd['_id'], pd.series.Series): pass
            # data_line = {'status': status} if isinstance(status, str) else {**status}
            ndx_line = {'_index': es_i, '_op_type': 'update', '_type': es_t,
                        '_id': rwd['_id'] if isinstance(rwd['_id'], str) else rwd['_id'].iloc[0]
                , 'doc': {'status': rwd['status'] if isinstance(rwd['status'], str) else rwd['status'].iloc[0],
                          'sb2file': rwd['sb2file'] if isinstance(rwd['sb2file'], str) else rwd['sb2file'].iloc[0]
                          }}
            bulk_upd8_data.append(ndx_line)
            if len(bulk_upd8_data) == 7895:
                try:
                    bulk_upd8_data = gener8(bulk_upd8_data)
                    helpers.bulk(es, bulk_upd8_data)
                    es.indices.refresh()
                    bulk_upd8_data = []
                except Exception as e:
                    # pass
                    # mdjlog.error(e)
                    count_down(None, 1)
                    helpers.bulk(es, bulk_upd8_data)
                    # es.indices.refresh()
                    bulk_upd8_data = []
        except Exception as e:
            mdjlog.error(e)
            bulk_upd8_data = []
            # pass
    bulk_upd8_data = list(bulk_upd8_data) if not isinstance(bulk_upd8_data, list) else bulk_upd8_data
    if len(bulk_upd8_data) > 0:
        count_down(None, 1)
        try:
            bulk_upd8_data = gener8(bulk_upd8_data)
            helpers.bulk(es, bulk_upd8_data)
            es.indices.refresh()
        except Exception as e:
            mdjlog.error(e)
            count_down(None, 1)
            helpers.bulk(es, bulk_upd8_data)
            es.indices.refresh()
    mdjlog.info('done. ..')


def gener8(l):
    for e in l: yield e


def log_check(total, running):
    if total >= 5000:
        rv = not running % 5137
    elif total >= 500 and total < 5000:
        rv = not running % 1357
    elif total < 500:
        rv = not running % 135
    if total == running and not rv:
        rv = not total % running
    return rv


def log_syndi_pro(crdt_shape, dataCount, mdjlog, sb2file):
    if log_check(crdt_shape, dataCount):
        mdjlog.info(f"PairinG data file {sb2file} & counting @#{dataCount} of {crdt_shape} @{time_t()}\n")


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


def syndic8data(crdt_data, sbjt_data, meta_data, ctgry_dtls, datCat, dp_meta, b2u, chunk_mode=None):
    mdjlog, sb2files, tasks = get_logger(meta_data['dp_name']), [], []
    file_dtls = DataFiles.objects(cycle_ver=meta_data['cycle_ver'], dpid=meta_data['dpid'], status='Loaded').first()
    # re_dupz(crdt_data, mdjlog)
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
            chnk_args = (crdt_data, ctgry_dtls, d8reported, datCat, data_size, dp_meta, dpid, mdjlog, meta_data,
                     runnin_chunks_list, sbjt_data, sgmnt, submxn_pt, b2u, syndi_dir, chunk_mode)
            # p = mp.Process(target=syndi_chunk_pro, args=chnk_args, )
            # p.start()
            # multi_pro(syndi_chunk_pro, chnk_args)
            syndi_chunk_pro(chnk_args)  # alt

        else:
            try:
                mdjlog.info(f"\n\nsyndicating. .. \n\t{crdt_data.shape} credits with \n\t{sbjt_data.shape} subjects")
            except:
                mdjlog.info(f"\n\nsyndicating. .. \n\t{crdt_data.shape} combined data")

            data_list_fn, data_list_hdr = syndi_params(ctgry_dtls, d8reported, dpid, mdjlog, sgmnt, submxn_pt)
            #
            args = (crdt_data, sbjt_data, meta_data, dp_meta, datCat, data_list_hdr, data_list_fn, b2u, syndi_dir,
                chunk_mode)
            #
            # multi_pro(syndidata, [args])
            syndidata(args)  # alt
            #
        print('\n#YHVH')
        mdjlog.info(fig_str("#YHVH"))
        return

    except Exception as e:
        mdjlog.error("{}".format(e))


def multi_pro(func, args):
    p = mp.Process(target=func, args=args, )
    p.start()


def syndi_chunk_pro(crdt_data, ctgry_dtls, d8reported, datCat, data_size, dp_meta, dpid, mdjlog, meta_data,
                    runnin_chunks_list, sbjt_data, sgmnt, submxn_pt, b2u, syndi_dir, chunk_mode):
    rounds, size_done = data_size // chunk_size + 1, 0
    rcount, size2use = rounds - 1, round(data_size / rounds)
    crdt_data_chunks = np.array_split(crdt_data, rounds)
    for idx, crdt_data_chunk in enumerate(crdt_data_chunks):
        try:
            # if idx < 2:  # todo remove after debug
            ndx_column = 'cust_id' if meta_data['in_mod'] in ('cdt', 'udf',) else 'account_no'
            sbjt_df = sbjt_data[sbjt_data[ndx_column].isin(crdt_data_chunk[ndx_column])]
            # crdt_data_chunk = crdt_data_chunk[crdt_data_chunk[ndx_column].isin(sbjt_df[ndx_column])]  # todo remove
            mdjlog.info("\n\nsyndicating CHUNK. .. \n\t{} credits with \n\t{} subjects".format(crdt_data_chunk.shape,
                                                                                               sbjt_df.shape))
            data_list_fn, data_list_hdr = syndi_params(ctgry_dtls, d8reported, dpid, mdjlog, sgmnt, submxn_pt)

            runnin_chunks_list.append(
                (crdt_data_chunk, sbjt_df, meta_data, dp_meta, datCat, data_list_hdr, data_list_fn, b2u, syndi_dir))
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
    process_date = datetime.now().strftime('%d-%b-%Y')
    process_time = datetime.now().strftime('%H%M%S')
    data_list_fn = '-'.join([dpid, ctgry_dtls[sgmnt][0].upper(), d8reported, process_time]) + '.dlt'
    data_list_fn = data_list_fn.upper()
    data_list_hdr = '|'.join(['HDHD', dpid, submxn_pt, d8reported, process_time, process_date, ctgry_dtls[sgmnt][1]])
    mdjlog.info("\nabout to . .. syndicate {}".format(data_list_fn))
    return data_list_fn, data_list_hdr
