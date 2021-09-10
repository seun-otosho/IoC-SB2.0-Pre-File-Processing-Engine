#!/usr/bin/python3
# -*- coding: utf-8 -*-
import inspect
import multiprocessing as mp
import re
import unicodedata
from datetime import datetime
from logging import Logger
from multiprocessing.pool import Pool
from os import sep

import numpy as np
import pandas as pd
from auto_bots import upd8batch
from elasticsearch import helpers
from IoCEngine import xtrcxn_area
from IoCEngine.commons import (count_down, fig_str, get_logger, gs,
                               iff_sb2_modes, mk_dir, profile, ps, right_now,
                               std_out, time_t, multi_pro)
from IoCEngine.config.pilot import chunk_size, cores2u, es, es_i, es_ii
from IoCEngine.SHU.trans4mas import (df_d8s2str, gender_dict, grntr_typ,
                                     rlxn_typ)
from IoCEngine.utils.data_modes import iff
from IoCEngine.utils.db2data import grntr_data, prnc_data
from IoCEngine.utils.file import DataFiles, SB2FileInfo

a, b, c = 1, 2, 3


# todo override for dev and test
# mpcores = 3


def var_name(var):
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    return [var_name for var_name, var_val in callers_local_vars if var_val is var][0]


def re_str(str_data: str, mdjlog: Logger):
    try:
        return ''.join((c for c in unicodedata.normalize('NFD', str_data) if unicodedata.category(c) != 'Mn'))
    except Exception as e:
        mdjlog.error(f"{e=}\t|\t{str_data=}\t|\t{type(str_data)=}")
        str_re = unicodedata.normalize('NFKD', str_data).encode('ASCII', 'ignore')
        return str_re


def clean_a_line(str: str, mdjlog: Logger):
    try:
        df_str = re.sub('\s+', ' ', str).strip()
        df_str = df_str.replace('|-|', '||').replace('|nan|', '||').replace('|NULL|', '||').replace(
            '|Null|', '||').replace('|null|', '||').replace('|NIL|', '||').replace('|Nil|', '||').replace(
            '|nil|', '||').replace('None', '').replace('…', '').replace('|nan', '|')
        df_str = df_str.replace('"', '')
        df_str = df_str.replace('.0|', '|') if '.0|' in df_str else df_str
    except Exception as e:
        mdjlog.error(f"{e=}\t|\t{df_str=}\t|\t{type(df_str)=}")
    return df_str


def fix_fac_missing(crdt_data: pd.DataFrame, meta_data: dict):
    crdt_data.insert(0, 'prev_dpid', '')
    crdt_data.insert(0, 'dpid', meta_data['dpid'])


def syndidata(lz: tuple):
    crdt_data, sbjt_data, meta_data, dp_meta, datCat, hdr, sb2file, b2u = lz[:8]
    mdjlog, dpid, syndicaxn_complt = get_logger(meta_data['dp_name']), meta_data['dpid'], False
    grntr, gs_data, prnc, ps_data, fac_sgmnt, sbjt_sgmnt, gs_sgmnt, ps_sgmnt = nones(8)
    chunk_info = lz[10]
    mdjlog.info(chunk_info)
    try:
        chunk_mode = lz[9] if lz[9] is not None else None
    except:
        chunk_mode = None
    try:
        dp_name = meta_data['dp_name'].lower()  # , syndi_data_list, []
        institution_cat, submxn_pt = dp_meta[dp_name][2], dp_meta[dp_name][1]
        meta_data.reload()

        loaded_batch = meta_data
        sbjt_data.fillna('', inplace=True)
        crdt_data.fillna('', inplace=True)

        try:
            loaded_gs = [lb for lb in b2u if gs[0] in lb.segments][0]
            grntr, gs_data = grntr_data(meta_data['dpid'], loaded_gs, gs[0])
            gs_data = gs_data[gs_data.account_no.isin(crdt_data[crdt_data.cust_id.isin(sbjt_data.cust_id)].account_no)]
            gs_vals(gs_data, mdjlog)
            grntr = not gs_data.empty
        except Exception as e:
            mdjlog.error(e)

        try:
            if datCat == 'com':
                loaded_ps = [lb for lb in b2u if ps[0] in lb.segments][0]
                prnc, ps_data = prnc_data(meta_data['dpid'], loaded_ps, ps[0])
                ps_data = ps_data[ps_data.cust_id.isin(sbjt_data.cust_id)]
                ps_vals(mdjlog, ps_data)
                prnc = not ps_data.empty
        except Exception as e:
            mdjlog.error(e)

        try:
            gs_cols, iff_fac_cols, iff_sbjt_cols, ps_cols = nones(4)
            if loaded_batch['out_mod'] == 'cmb':
                fac_sgmnt, gs_cols, gs_sgmnt, iff_fac_cols, iff_sbjt_cols, ps_sgmnt, sbjt_sgmnt = cmb_syndi_vars(
                    datCat, grntr, prnc)

            if loaded_batch['out_mod'] == 'mfi' or (
                    loaded_batch['in_mod'] == loaded_batch['out_mod'] and loaded_batch['in_mod'] == 'iff'):
                fac_sgmnt, gs_cols, gs_sgmnt, iff_fac_cols, iff_sbjt_cols, ps_sgmnt, sbjt_sgmnt = mfi_syndi_vars(
                    datCat, grntr, prnc)

            if loaded_batch['out_mod'] == 'pmi':
                fac_sgmnt, gs_cols, gs_sgmnt, iff_fac_cols, iff_sbjt_cols, ps_sgmnt, sbjt_sgmnt = pmi_syndi_vars(
                    datCat, grntr, prnc)
            if prnc:
                ps_cols = iff()[ps[0]]

            dataCount, idx = 0, 0
            syndi_dir = xtrcxn_area + sep + meta_data['dp_name'].split('_')[0].upper() + sep
            mk_dir(syndi_dir)
            syndi_iter, syndi_data_list = [], []
            syndi_complt, iocnow, syndi_crdt_data, syndi_sbjt_data = nones(4)

            if grntr and prnc:
                syndi_pair_args = (
                    crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt,
                    datCat, syndi_dir, chunk_info, gs_data, gs_sgmnt, gs_cols, ps_data, ps_sgmnt, ps_cols,
                )
            elif grntr and not prnc:
                syndi_pair_args = (
                    crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt,
                    datCat, syndi_dir, chunk_info, gs_data, gs_sgmnt, gs_cols,
                )
            elif not grntr and prnc:
                syndi_pair_args = (
                    crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt,
                    datCat, syndi_dir, chunk_info, ps_data, ps_sgmnt, ps_cols,
                )
            else:
                syndi_pair_args = (
                    crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt,
                    datCat, syndi_dir, chunk_info, *nones(7),
                )
            syndi_rez = [syndi_pairs(syndi_pair_args)]

            for rez in syndi_rez:
                dataCount, syndi_data_list = dataCount + rez[0], syndi_data_list + rez[1]
                syndi_complt, iocnow, syndi_crdt_data, syndi_sbjt_data = rez[2], rez[3], rez[4], rez[5]
            syndi_data_list.insert(0, hdr)
            mdjlog.info('\n' + '\n' + '#' * 128)
            structures = int(dataCount)
            sstructures = str(int(structures))
            mdjlog.info(f"\n\n{sstructures} {datCat.upper()} structures processed from {meta_data} @ {right_now()}")
            footer = '|'.join(['TLTL', dpid, submxn_pt, sstructures])
            syndi_data_list.append(footer)
            sb2file_handler = open(syndi_dir + sb2file, 'w')
            sb2file_handler.write('\n'.join(syndi_data_list))
            sb2file_handler.close()
            mdjlog.info(f"Syndicated file: {sb2file} written @ {right_now()}")

            str_date = str(datetime.now().date())
            dp_syndi_logger = get_logger(f"{meta_data['dp_name']}-{str_date}", mini=True)
            dp_syndi_logger.info("|".join(( meta_data['dp_name'], str_date, datCat, sstructures, sb2file, )))

            upd8segmnts(dp_name, iocnow, mdjlog, syndi_crdt_data, syndi_sbjt_data)

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
            mdjlog.info(chunk_info)
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


def ps_vals(mdjlog: Logger, ps_data: pd.DataFrame):
    ps_data.loc[:, "prnc_type"] = "002"
    ps_data.loc[:, 'psxn_in_biz'] = ps_data.psxn_in_biz.apply(lambda x: rlxn_typ[x.lower()])
    ps_data.loc[:, 'pri_addr_typ'] = '001'
    df_d8s2str(ps_data, mdjlog)


def gs_vals(gs_data: pd.DataFrame, mdjlog: Logger):
    gs_data.loc[:, 'grntr_type'] = gs_data.grntr_type.apply(lambda x: grntr_typ[x.lower()])
    gs_data.loc[:, 'incorp_date'] = gs_data[['grntr_type', 'birth_incorp_date']].apply(
        lambda x: x[1] if x[0] == '001' else '', axis=1)
    gs_data.loc[:, 'birth_incorp_date'] = gs_data[['grntr_type', 'birth_incorp_date']].apply(
        lambda x: x[1] if x[0] == '002' else '', axis=1)
    gs_data['gender'] = gs_data[['grntr_type', 'gender']].apply(
        lambda x: gender_dict.get(str(x[1]).strip().upper(), '') if x[0] == '002' else '', axis=1)
    gs_data.loc[:, 'pri_addr_typ'] = '001'
    df_d8s2str(gs_data, mdjlog)


def syndi_pairs(targs: tuple):
    crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt, dataCat = targs[:9]
    gs, gs_df, gs_sgmnt, gs_cols, ps, ps_df, ps_sgmnt, ps_cols = nones(8)
    chunk_info, pair_syndi_txt = targs[10], ""
    dpid, dp_name, syndicaxn_complt, syndi_data_list = meta_data['dpid'], meta_data['dp_name'], None, []
    mdjlog, dataCount, cust_id, ac_no, cycle_ver = get_logger(dp_name), 0, None, None, meta_data['cycle_ver']

    crdt_data['cycle_ver'], sbjt_data['cycle_ver'] = meta_data['cycle_ver'], meta_data['cycle_ver']
    crdt_shape, ac_no_list, cust_id_list = crdt_data.shape[0], [], []
    sbjt_prnc_dict = {c: None for c in sbjt_data.cust_id}
    # fix branch code
    fix_branch(mdjlog, crdt_data)
    fix_branch(mdjlog, sbjt_data)

    # todo NEED to be implemented for combined data
    # a_sbjt_line = '|'.join(
    #     [str(crdt_data.ix[idx][k][0]) if k in crdt_data.ix[idx] else '' for k in iff_sbjt_cols])

    if meta_data['in_mod'] not in iff_sb2_modes:
        branch_code_df = sbjt_data[['cust_id', 'branch_code']]
        account_no_df = crdt_data[['cust_id', 'account_no']]
        crdt_data = crdt_data[[c for c in crdt_data.columns if c not in 'branch_code']]
        crdt_data = pd.merge(crdt_data, branch_code_df, on='cust_id', how='inner')
        sbjt_data = sbjt_data[[c for c in sbjt_data if c != 'account_no']]
        iff_sbjt_data = pd.merge(account_no_df, sbjt_data, on='cust_id', how='inner')
    else:
        iff_sbjt_data = sbjt_data

    if 'com' in dataCat:
        try:
            ps_df, sndx = targs[14] if len(targs) > 14 else targs[11], 'cust_id'
            c_seg = targs[15] if len(targs) > 14 else targs[12]
        except Exception as e:
            mdjlog.error(f"{e=}")
            ps_df, sndx, c_seg = nones(3)

        if c_seg == 'CMRS':
            ps_df = pd.merge(account_no_df, ps_df, on=sndx, how='inner')
            ps_args = ps_df, *targs[12:14], True, [
                'branch_code', 'cust_id', 'account_no', 'biz_name', ], 'cust_id', True
            ps, ps_df = related_df(iff_sbjt_data, ps_args, mdjlog, meta_data)

    try:
        c_seg = targs[12] if len(targs) > 11 else None

        if c_seg and c_seg[2:] == 'GS':
            gs_args = *targs[11:14], True, ['branch_code', 'account_no'], 'account_no', True
            gs, gs_df = related_df(crdt_data, gs_args, mdjlog, meta_data)
    except Exception as e:
        mdjlog.error(f"{e=}")

    mdjlog.info(crdt_data.shape)
    mdjlog.info(iff_sbjt_data.shape)
    iff_crdt_data = crdt_data.reindex(columns=iff_fac_cols)
    iff_sbjt_data = iff_sbjt_data.reindex(columns=iff_sbjt_cols)
    mdjlog.info(f"{iff_crdt_data.shape=}")
    mdjlog.info(f"{iff_sbjt_data.shape=}")

    iff_crdt_data.branch_code.fillna('001', inplace=True)
    iff_crdt_data.insert(0, 'prev_dpid', '')
    iff_crdt_data.insert(0, 'dpid', meta_data['dpid'])
    iff_crdt_data.insert(0, 'segment', fac_sgmnt)
    iff_crdt_data.fillna('', inplace=True)

    iff_sbjt_data.branch_code.fillna('001', inplace=True)
    iff_sbjt_data.insert(0, 'dpid', meta_data['dpid'])
    iff_sbjt_data.insert(0, 'segment', sbjt_sgmnt)
    iff_sbjt_data.fillna('', inplace=True)

    dataFacCount, gsFacCount, acctGsCount, psCustCount, custPsCount, psCustList = 0, 0, 0, 0, 0, []
    std_out(f"PairinG data for Syndicate file {sb2file} & counting @#{dataFacCount + 1}_of_{crdt_shape}\t\t")
    shape_str = f"StartinG::{crdt_data.shape=}\t|\tEnrich3D::{iff_crdt_data.shape}\t"
    iff_crdt_data.drop_duplicates('account_no', inplace=True)
    iff_crdt_data.reset_index(drop=True, inplace=True)
    shape_str += f"|\tDeDuplicat3D::{iff_crdt_data.shape}"
    mdjlog.info(shape_str)
    for idx in iff_crdt_data.index:
        try:
            try:
                cidx = int(idx) if (str(idx).isdigit() or isinstance(idx, (int, np.int64))
                                    ) else int(''.join([s for s in list(idx) if s.isdigit()]))
            except Exception as e:
                mdjlog.error(e)

            try:
                ac_no, _idx = iff_crdt_data.iloc[idx]['account_no'], not cidx % 55 or not cidx % 77 or not cidx % 88
            except Exception as e:
                mdjlog.error(e)
                ac_no, _idx = iff_crdt_data.loc[idx]['account_no'], not cidx % 55 or not cidx % 77 or not cidx % 88
                mdjlog.error(f"{ac_no=}, {_idx=}")

            try:
                subj = iff_sbjt_data.loc[iff_sbjt_data['account_no'] == ac_no]
                cust_id = subj['cust_id']
                dataFacCount += 1
            except Exception as e:
                mdjlog.error(e)

            crdt_i_data = iff_crdt_data.iloc[idx]
            a_fac_line = re_str(clean_a_line('|'.join(str(c) for c in crdt_i_data.tolist()), mdjlog), mdjlog)

            try:
                a_sbjt_line = re_str(clean_a_line('|'.join(str(c) for c in subj.iloc[0].tolist()), mdjlog), mdjlog)
            except Exception as e:
                del e
                a_sbjt_line = re_str(clean_a_line('|'.join(str(c) for c in subj.tolist()), mdjlog), mdjlog)

            syndi_data_list.append(a_fac_line)
            custID2use = str(cust_id) if isinstance(cust_id, str) else str(cust_id.iloc[0])
            cust_id_list.append(str(custID2use))
            syndi_data_list.append(a_sbjt_line)
            ac_no2use = str(ac_no) if isinstance(ac_no, str) else str(ac_no.iloc[0])
            ac_no_list.append(str(ac_no2use))

            if 'com' in dataCat:
                prnc_df = ps_df[ps_df.cust_id == custID2use].drop_duplicates() if ps else pd.DataFrame()
                # std_out(f"{prnc_df.empty=} {prnc_df.shape=}")
                if not prnc_df.empty and not sbjt_prnc_dict[custID2use]:
                    custPsCount, psCustCount, syndi_data_list = related_data_line(custPsCount, prnc_df, psCustCount,
                                                                                  syndi_data_list, mdjlog)
                    psCustList.append(custID2use)
                    sbjt_prnc_dict[custID2use] = True

            grntr_df = gs_df[gs_df.account_no == ac_no].drop_duplicates() if gs else pd.DataFrame()
            # std_out(f"{grntr_df.empty=} {grntr_df.shape=}")
            if not grntr_df.empty:
                acctGsCount, gsFacCount, syndi_data_list = related_data_line(acctGsCount, grntr_df, gsFacCount,
                                                                             syndi_data_list, mdjlog)

            syndicaxn_complt = True

            log_syndi_pro(crdt_shape, dataFacCount, mdjlog, sb2file, len(syndi_data_list), chunk_info)

            if _idx:
                pair_syndi_txt = (f'PairinG {dp_name} data for file {sb2file} @ index {idx:,} of {crdt_shape:,}'
                                  f' @{time_t()} of {chunk_info[9:]}\t\t')
                std_out(pair_syndi_txt)
        except Exception as e:
            mdjlog.error(f"{e} | customer ID {cust_id} with account no: {ac_no}")

    mdjlog.info(pair_syndi_txt)
    del iff_crdt_data, iff_sbjt_data
    re = re4index(ac_no_list, crdt_data, cust_id_list, dataCat, sb2file, sbjt_data)
    crdt_data, sbjt_data, syndi_crdt_data, syndi_sbjt_data = re

    iocnow = crdt_data.to_dict('records')
    iocnow = iocnow + sbjt_data.to_dict('records')

    ac_count, cust_count = len(set(ac_no_list)), len(set(cust_id_list))
    psCustCount = len(list(set(psCustList)))
    mdjlog.info(f"{gsFacCount=}\t|\t{psCustCount=}")
    pairing_info(ac_count, acctGsCount, custPsCount, cust_count, gsFacCount, mdjlog, psCustCount, sb2file, dataCat)

    del crdt_data, fac_sgmnt, iff_fac_cols, iff_sbjt_cols, meta_data, sb2file, sbjt_data, sbjt_sgmnt, dataCat
    del targs
    return dataFacCount, syndi_data_list, syndicaxn_complt, iocnow, syndi_crdt_data, syndi_sbjt_data


def related_df(pri_df: pd.DataFrame, sec_args: tuple, mdjlog: Logger, meta_data) -> tuple:
    try:
        sec_df, sec_sgmnt, sec_cols, sec, rqrd_cols, ndx_col, svii = sec_args
        if sec:
            for col in [c for c in [r for r in rqrd_cols if r != ndx_col] if c in sec_df and c != ndx_col]:
                sec_df.drop(col, axis=1, inplace=True)
            sec_df = pd.merge(sec_df, pri_df[rqrd_cols], on=ndx_col, how='inner')
            sec_df = sec_df.reindex(columns=sec_cols)
            sec_df.insert(0, 'dpid', meta_data['dpid'])
            sec_df.insert(0, 'segment', sec_sgmnt)
            sec_df.drop_duplicates(inplace=True)
    except Exception as e:
        mdjlog.error(f"{svii}: {sec_sgmnt.upper()}'s Data was not provided hence: {e}")
    return sec, sec_df


def related_data_line(pri_count: int, df: pd.DataFrame, sec_count: int, data_list: list, logger: Logger) -> tuple:
    try:
        df_vals_list = df.values.tolist()
        for rw in df_vals_list:
            try:
                data_list.append(re_str(clean_a_line('|'.join(str(c) for c in rw), logger), logger))
                sec_count += 1
            except Exception as e:
                logger.error(f"{e=}\t|\t{rw=}")
        pri_count += 1
        std_out(f"{sec_count=}\t|\t{pri_count=}")
    except Exception as e:
        logger.error(f"{e=}")
    return pri_count, sec_count, data_list


def pairing_info(ac_count: int, acctGsCount: int, custPsCount: int, cust_count: int, gsFacCount: int,
                 mdjlog: Logger, psCustCount: int, sb2file: str, data_cat: str) -> None:
    mdjlog.info(f"Syndicate file {sb2file} will contain pairs from {ac_count:,} facilities and {cust_count:,} subjects")
    if 'com' not in data_cat:
        psCustCount = 0
    if gsFacCount > 0 and psCustCount > 0:
        if 'com' in data_cat:
            mdjlog.info(f"With {psCustCount:,} principal officers for {custPsCount:,} of {cust_count:,} subjects")
        mdjlog.info(f"And {gsFacCount:,} guarantors for {acctGsCount:,} of {ac_count:,} facilities")
    elif gsFacCount > 0 and psCustCount == 0:
        mdjlog.info(f"With {gsFacCount:,} guarantors for {acctGsCount:,} of {ac_count:,} facilities")
    elif gsFacCount == 0 and psCustCount > 0:
        mdjlog.info(f"With {psCustCount:,} principal officer(s) for {custPsCount:,} of {cust_count:,} subjects")
    else:
        if 'com' in data_cat:
            mdjlog.info(f"No principal officer for this syndication file {sb2file} set subject records")
        mdjlog.info(f"No guarantor for this syndication file {sb2file} set facility records")


def re4index(ac_no_list: list, crdt_data: pd.DataFrame, cust_id_list: list, dataCat: str, sb2file: str,
             sbjt_data: pd.DataFrame) -> tuple:
    syndi_crdt_data = crdt_data[crdt_data.account_no.isin(ac_no_list)]
    syndi_crdt_data.loc[:, 'sb2file'], syndi_crdt_data.loc[:, 'status'] = sb2file, 'Syndicated'
    syndi_crdt_data.loc[:, '_index'] = es_i
    syndi_crdt_data = syndi_crdt_data[['_index', '_id', 'sb2file', 'status']]

    syndi_sbjt_data = sbjt_data[sbjt_data.cust_id.isin(cust_id_list)]
    syndi_sbjt_data.loc[:, 'sb2file'], syndi_sbjt_data.loc[:, 'status'] = sb2file, 'Syndicated'
    syndi_sbjt_data.loc[:, '_index'] = es_i
    syndi_sbjt_data = syndi_sbjt_data[['_index', '_id', 'sb2file', 'status']]

    crdt_data = crdt_data[crdt_data.account_no.isin(ac_no_list)]
    sbjt_data = sbjt_data[sbjt_data.cust_id.isin(cust_id_list)]

    crdt_data.loc[:, '_index'], sbjt_data.loc[:, '_index'] = es_ii, es_ii
    crdt_data.loc[:, 'sb2file'], crdt_data.loc[:, 'status'] = sb2file, 'Syndicated'
    sbjt_data.loc[:, 'sb2file'], sbjt_data.loc[:, 'status'] = sb2file, 'Syndicated'
    crdt_data.loc[:, 'submission'] = 'corporate' if 'com' in dataCat else 'individual'

    crdt_data.loc[:, '_id'] = crdt_data['_id'].apply(lambda x: '-'.join(x.split('-')[:-1]))
    sbjt_data.loc[:, '_id'] = sbjt_data['_id'].apply(lambda x: '-'.join(x.split('-')[:-1]))

    return crdt_data, sbjt_data, syndi_crdt_data, syndi_sbjt_data


def fix_branch(mdjlog: Logger, df: pd.DataFrame):
    if 'branch_code' in df:
        try:
            df['branch_code'] = df.branch_code.apply(lambda x: str(x)[-4:] if len(str(x)) > 4 else str(x))
        except Exception as e:
            mdjlog.error(e)
            df['branch_code'] = '001'


def upd8segmnts(dp_name: str, iocnow, mdjlog: Logger, syndi_crdt_data: pd.DataFrame, syndi_sbjt_data: pd.DataFrame):
    try:
        upd8DFstatus(syndi_crdt_data, dp_name, es_i, 'submissions')
    except Exception as e:
        mdjlog.error(e)
    try:
        upd8DFstatus(syndi_sbjt_data, dp_name, es_i, 'submissions')
    except Exception as e:
        mdjlog.error(e)
    try:
        currindx(iocnow, mdjlog)
    except Exception as e:
        mdjlog.error(e)


def pmi_syndi_vars(datCat: str, grntr: bool, prnc: bool):
    fac_sgmnt, gs_cols, gs_sgmnt, iff_fac_cols, iff_sbjt_cols, ps_sgmnt, sbjt_sgmnt = nones(7)
    if datCat in ('com', 'mgcom'):
        fac_sgmnt, sbjt_sgmnt = 'MMCF', 'MMCS'
        iff_sbjt_cols = iff()['pmi']['comm']
        if prnc:
            ps_sgmnt = 'RMRS'
    if datCat in ('con', 'mgcon'):
        fac_sgmnt, sbjt_sgmnt = 'MNCF', 'MNCS'
        iff_sbjt_cols = iff()['pmi']['cons']
    iff_fac_cols = iff()['pmi']['fac']
    if grntr:
        gs_cols, gs_sgmnt = iff()['mfi'][gs[0]], 'MMGS' if datCat == 'com' else 'MNGS'
    return fac_sgmnt, gs_cols, gs_sgmnt, iff_fac_cols, iff_sbjt_cols, ps_sgmnt, sbjt_sgmnt


def mfi_syndi_vars(datCat: str, grntr: bool, prnc: bool):
    fac_sgmnt, gs_cols, gs_sgmnt, iff_fac_cols, iff_sbjt_cols, ps_sgmnt, sbjt_sgmnt = nones(7)
    if datCat in ('com', 'mfcom'):
        fac_sgmnt, sbjt_sgmnt = 'CMMF', 'CMMS'
        iff_sbjt_cols = iff()['mfi']['comm']
        if prnc:
            ps_sgmnt = 'CMRS'
    if datCat in ('con', 'mfcon'):
        fac_sgmnt, sbjt_sgmnt = 'CNMF', 'CNMS'
        iff_sbjt_cols = iff()['mfi']['cons']
    iff_fac_cols = iff()['mfi']['fac']
    if grntr:
        gs_cols, gs_sgmnt = iff()['mfi'][gs[0]], 'CMGS' if datCat == 'com' else 'CNGS'
    return fac_sgmnt, gs_cols, gs_sgmnt, iff_fac_cols, iff_sbjt_cols, ps_sgmnt, sbjt_sgmnt


def cmb_syndi_vars(datCat: str, grntr: bool, prnc: bool):
    fac_sgmnt, gs_cols, gs_sgmnt, iff_fac_cols, iff_sbjt_cols, ps_sgmnt, sbjt_sgmnt = nones(7)
    if datCat == 'com':
        fac_sgmnt, sbjt_sgmnt = 'CMCF', 'CMCS'
        iff_fac_cols, iff_sbjt_cols = iff()['cmb']['corpfac'], iff()['cmb']['corp']
        if prnc:
            ps_sgmnt = 'CMRS'
    if datCat == 'con':
        fac_sgmnt, sbjt_sgmnt = 'CNCF', 'CNCS'
        iff_fac_cols, iff_sbjt_cols = iff()['cmb']['ndvdlfac'], iff()['cmb']['ndvdl']
    if grntr:
        gs_cols, gs_sgmnt = iff()['cmb'][gs[0]], 'CMGS' if datCat == 'com' else 'CNGS'
    return fac_sgmnt, gs_cols, gs_sgmnt, iff_fac_cols, iff_sbjt_cols, ps_sgmnt, sbjt_sgmnt


def nones(n: int = 1) -> tuple:
    return eval("None, " * n)


def re_dupz(df: pd.DataFrame, ndx: int, mdjlog):
    ndx_nunique = df[ndx].nunique()
    dups = df.shape[0] - ndx_nunique
    if dups > 0:
        mdjlog.warn(f'dropping {dups} duplicate {ndx} from {df.shape[0]} to have {ndx_nunique} unique records')
        df.drop_duplicates(subset=[ndx], keep='first', inplace=True)
    else:
        mdjlog.info("no duplicates")


def normalize_dict(ivar) -> dict:
    d = ivar.to_dict('records') if isinstance(ivar, pd.DataFrame) else ivar.to_dict()
    d = d if isinstance(d, dict) else d[0]
    d = {i: d[i] for i in d if d[i] not in ('', None,)}
    d.update({i: d[i].item() for i in d if isinstance(d[i], np.int64)})
    return d


def currindx(iocnow: list, mdjlog: Logger):
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


def upd8DFstatus(df: pd.DataFrame, dp_name: str, es_i: str, es_t: str):
    mdjlog, slog = get_logger(dp_name), get_logger()
    bulk_upd8_data = []
    for idx in df.index:
        try:
            rwd = df.iloc[idx]
        except Exception as e:
            rwd = df.loc[idx]

        ndx_line = {'_index': es_i, '_op_type': 'update',
                    '_id': rwd['_id'] if isinstance(rwd['_id'], str) else rwd['_id'].iloc[0],
                    'doc': {'status': rwd['status'] if isinstance(rwd['status'], str) else rwd['status'].iloc[0],
                            'sb2file': rwd['sb2file'] if isinstance(rwd['sb2file'], str) else rwd['sb2file'].iloc[0], }}
        bulk_upd8_data.append(ndx_line)
        if len(bulk_upd8_data) == 7895:
            upd8a_batch(bulk_upd8_data, mdjlog)

    bulk_upd8_data = list(bulk_upd8_data) if not isinstance(bulk_upd8_data, list) else bulk_upd8_data
    if len(bulk_upd8_data) > 0:
        count_down(None, 1)
        upd8a_batch(bulk_upd8_data, mdjlog)
    mdjlog.info('done. ..')


def upd8a_batch(bulk_upd8_data: list, mdjlog: Logger):
    try:
        bulk_upd8_data = gener8(bulk_upd8_data)
        helpers.bulk(es, bulk_upd8_data)
        es.indices.refresh()
        bulk_upd8_data = []
    except Exception as e:
        mdjlog.error(e)
        count_down(None, 1)
        helpers.bulk(es, bulk_upd8_data)
        es.indices.refresh()
        bulk_upd8_data = []


def gener8(l: list):
    for e in l: yield e


def log_check(total: int, running: int, mdjlog: Logger):
    try:
        if total >= 5000:
            rv = not running % 5137
        elif 500 <= total < 5000:
            rv = not running % 1357
        elif total < 500:
            rv = not running % 135
        if total == running and not rv:
            rv = not total % running
        return rv
    except Exception as e:
        mdjlog.error(e)


def log_syndi_pro(crdt_shape: int, dataCount: int, mdjlog: Logger, sb2file: str, syndi_count: int, chunk_info) -> None:
    if log_check(crdt_shape, dataCount, mdjlog):
        try:
            msg = (f"PairinG data file {sb2file} & counting @#{dataCount} of {crdt_shape}"
                   + f" | total syndicated PAIRS are {syndi_count}\t|\t{chunk_info[9:]} @{time_t()}\t\t\n")
            std_out(msg)
        except Exception as e:
            mdjlog.error(e)


def upd8sjdt_recs(largs: tuple, sbdt_data: pd.DataFrame):
    loaded_batch, cust_rec_col = largs[:2]
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


def upd8crdt_recs(largs: tuple, crdt_data: pd.DataFrame):
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


@profile
def syndic8data(crdt_data: pd.DataFrame, sbjt_data: pd.DataFrame, meta_data, ctgry_dtls, datCat: str, dp_meta,
                b2u, chunk_mode=None, mdjlog=None):
    mdjlog, sb2files, tasks = mdjlog if mdjlog else get_logger(meta_data['dp_name']), [], []
    file_dtls = DataFiles.objects(cycle_ver=meta_data['cycle_ver'], dpid=meta_data['dpid'], status='Loaded').first()
    file_dtls = file_dtls if file_dtls else DataFiles.objects(
        cycle_ver=meta_data['cycle_ver'], dpid=meta_data['dpid']).first()
    # re_dupz(crdt_data, mdjlog)
    try:
        dp_name = meta_data['dp_name'].split('_')[0].lower()
        instCat = dp_meta[dp_name][2]
        institution_cat, submxn_pt = dp_meta[dp_name][2], dp_meta[dp_name][1]
        sgmnt = instCat + datCat if instCat not in ('cb',) else datCat
        d8reported = file_dtls['date_reported']
        dpid = meta_data['dpid']
        syndi_dir = xtrcxn_area + sep + meta_data['dp_name'].split('_')[0].upper() + sep
        mk_dir(syndi_dir)
        data_size, runnin_chunks_list = crdt_data.shape[0], []

        if data_size > chunk_size:
            chnk_args = (crdt_data, ctgry_dtls, d8reported, datCat, data_size, dp_meta, dpid, mdjlog, meta_data,
                         runnin_chunks_list, sbjt_data, sgmnt, submxn_pt, b2u, syndi_dir, chunk_mode)
            # p = mp.Process(target=syndi_chunk_pro, args=chnk_args, )
            # p.start()
            # multi_pro(syndi_chunk_pro, chnk_args)
            syndi_chunk_pro(*chnk_args)  # alt

        else:
            try:
                mdjlog.info(f"syndicating. .. \t{crdt_data.shape} credits with \t{sbjt_data.shape} subjects")
            except:
                mdjlog.info(f"syndicating. .. \t{crdt_data.shape} combined data")

            data_list_fn, data_list_hdr = syndi_params(ctgry_dtls, d8reported, dpid, mdjlog, sgmnt, submxn_pt)
            #
            chunk_info = f"\nQueuinG! #0 of 0 Counts: {crdt_data.shape[0]:,} of {crdt_data.shape[0]:,} of {data_size:,}"
            args = (crdt_data, sbjt_data, meta_data, dp_meta, datCat, data_list_hdr, data_list_fn, b2u, syndi_dir,
                    chunk_mode, chunk_info, )
            #
            # multi_pro(syndidata, [args])
            syndidata(args)  # alt
            #
        print('\n#YHWH')
        mdjlog.info(fig_str("#YHWH"))
        return

    except Exception as e:
        mdjlog.error("{}".format(e))


def multi_list_pro(args):
    logger = get_logger()
    # chunks = len(args)
    logger.info(f"About to move over {cores2u=}")  # { chunks=}
    pool = Pool(processes=cores2u)
    pool.map(syndidata, args)
    logger.info(f"Just moved over {cores2u=}")  # { chunks=}


@profile
def syndi_chunk_pro(crdt_data: pd.DataFrame, ctgry_dtls, d8reported, datCat: str, data_size: int, dp_meta, dpid: str,
                    mdjlog: Logger, meta_data, runnin_chunks_list, sbjt_data: pd.DataFrame, sgmnt: str, submxn_pt: str,
                    b2u, syndi_dir, chunk_mode):
    rounds, size_done = data_size // chunk_size + 1, 0
    rcount, size2use = rounds - 1, round(data_size / rounds)
    crdt_data_chunks = np.array_split(crdt_data, rounds)
    chunks_gene = chunk_pairs_pro(b2u, crdt_data_chunks, ctgry_dtls, d8reported, datCat, data_size, dp_meta, dpid,
                                  mdjlog, meta_data, rcount, runnin_chunks_list, sbjt_data, sgmnt, size2use, size_done,
                                  submxn_pt, syndi_dir)
    # pool = Pool(processes=cores2u)
    # pool.map(syndidata, runnin_chunks_list)

    p = mp.Process(target=multi_list_pro, args=[chunks_gene], )
    p.start()
    mdjlog.info(f"Handing Chunks OFF!")  # {len(runnin_chunks_list)}
    return


def chunk_pairs_pro(b2u, crdt_data_chunks, ctgry_dtls, d8reported, datCat, data_size, dp_meta, dpid, mdjlog, meta_data,
                    rcount, runnin_chunks_list, sbjt_data, sgmnt, size2use, size_done, submxn_pt, syndi_dir):
    for idx, crdt_data_chunk in enumerate(crdt_data_chunks):
        try:
            # if idx < 2:  # todo remove after debug
            ndx_column = 'cust_id' if meta_data['in_mod'] in ('cdt', 'udf',) else 'account_no'
            sbjt_df = sbjt_data[sbjt_data[ndx_column].isin(crdt_data_chunk[ndx_column])]
            # crdt_data_chunk = crdt_data_chunk[crdt_data_chunk[ndx_column].isin(sbjt_df[ndx_column])]  # todo remove
            mdjlog.info(f"""
            \nsyndicating CHUNK. .. \n\t{crdt_data_chunk.shape} credits with \n\t{sbjt_df.shape} subjects""")
            data_list_fn, data_list_hdr = syndi_params(ctgry_dtls, d8reported, dpid, mdjlog, sgmnt, submxn_pt)

            # runnin_chunks_list.append(
            chunk_info = f"\nQueuinG! #{idx} of {rcount} Counts: {size2use:,} of {size_done:,} of {data_size:,}"
            yield (crdt_data_chunk, sbjt_df, meta_data, dp_meta, datCat, data_list_hdr, data_list_fn, b2u, syndi_dir,
                 None, chunk_info, )
            # )
            size_done += size2use
            mdjlog.info(chunk_info)
            count_down(None, 1)
        except Exception as e:
            mdjlog.error(e)


def syndi_params(ctgry_dtls, d8reported, dpid: str, mdjlog: Logger, sgmnt, submxn_pt):
    process_date = datetime.now().strftime('%d-%b-%Y')
    process_time = datetime.now().strftime('%H%M%S')
    data_list_fn = '-'.join([dpid, ctgry_dtls[sgmnt][0].upper(), d8reported, process_time]) + '.dlt'
    data_list_fn = data_list_fn.upper()
    mdjlog.info(f"{ctgry_dtls[sgmnt][1] = }")
    data_list_hdr = '|'.join(['HDHD', dpid, submxn_pt, d8reported, process_time, process_date, ctgry_dtls[sgmnt][1]])
    mdjlog.info(f"about to . .. syndicate {data_list_fn}")
    return data_list_fn, data_list_hdr
