import os
from itertools import islice

import numpy as np
import openpyxl
import pandas as pd

from IoCEngine import drop_zone
from IoCEngine.celeryio import app
from IoCEngine.commons import mk_dp_x_dir, cs, ns
from IoCEngine.data_router import worksheet_datatype as confirm_data
from IoCEngine.logger import get_logger
from IoCEngine.utils.data2db import route_df
from IoCEngine.utils.file import DataFiles


def rw_file(dz, file, nums):
    mdjlog = get_logger(file.split('_')[0])
    try:
        bad_f, orgnl_l, now_orgnl_l = [], [], []
        mdjlog.info("Reading Data File {} for corrections".format(file))
        f = open(os.path.join(dz, file), "rb")
        orgnl_l = f.read().splitlines()
        f.close()
        
        bad_f.append(orgnl_l[0].decode("unicode_escape").strip())
        now_orgnl_l.append(orgnl_l[0].decode("unicode_escape").strip())
        
        for i, lo in enumerate(orgnl_l):
            if i > 0:
                lo_pop = orgnl_l.pop(i)
                try:
                    lo_pop = lo_pop.decode("unicode_escape")
                except Exception as e:
                    print(e)
                    mdjlog.error(lo_pop)
                try:
                    if int(len(lo_pop.strip().split('|'))) != int(nums[0]):
                        # lo_pop = lo_pop.translate(table,)
                        bad_f.append(lo_pop)
                    else:
                        now_orgnl_l.append(lo_pop)
                except Exception as e:
                    print(e)
                    print(lo_pop)
        
        dp_name = file.split('_')[0].upper()
        unprocessed = mk_dp_x_dir(dp_name.upper() + os.sep + 'unprocessed')
        badf = open(unprocessed + os.sep + file.replace('.', '_bad.'), 'w')
        mdjlog.info("writing bad file extracted from {}".format(file))
        # badf.write('\n'.join([l.decode().encode('utf-8').strip('\n') for l in bad_f]))
        for l in bad_f:
            l.strip('\n')
            try:
                badf.write('{}\n'.format(l))
            except Exception as e:
                mdjlog.error(l)
                mdjlog.error(e)
        badf.close()
        hdr = orgnl_l.pop(0)
        return pd.DataFrame([l.split('|') for l in now_orgnl_l], columns=hdr.decode('unicode_escape').split('|'))
    except Exception as e:
        mdjlog.error(e)


@app.task(name='xtrct_ff_data')
def xtrct_ff_data(file_meta, batch_no=None):
    file = file_meta['file_name']
    mdjlog = get_logger(file.split('_')[0])
    cont, expected, line, saw = True, [], [], []
    
    while cont:
        try:
            if not 'df' in locals():
                ff_large_file_mgr(file, file_meta)
            cont = False
            # if df.shape[0] > 0:
            #     # if df.shape[0] > 345789:
            #     #     chunks = np.array_split(df, 3)
            #     for c in df:
            #         return route_df((file_meta, file_meta['data_type'], c))
            #     else:
            #         return route_df((file_meta, file_meta['data_type'], c))
            # else:
            #     mdjlog.info('no data. ..')
            return
        except Exception as e:
            # error_bad_lines
            df = pd.read_csv(
                drop_zone + file, dtype=str, keep_default_na=True, sep='|', thousands=',', encoding="ISO-8859-1",
                error_bad_lines=False, warn_bad_lines=True)
            # df = handle_ff_xcpxn(e, expected, file, line, saw)
            df.drop_duplicates(inplace=True)
            return
    if len(line) != 0:
        mdjlog.info(line)
    ff_large_file_mgr(file, file_meta)
    # if df.shape[0] > 0:
    #     if df.shape[0] > 345789:
    #         chunks = np.array_split(df, 2)
    #         for df in chunks:
    #             return route_df((file_meta, file_meta['data_type'], df))
    #     else:
    #         return route_df((file_meta, file_meta['data_type'], df))
    # else:
    #     mdjlog.info('no data. ..')


def rez_dup(data_tpl, df):
    # datatype, mdjlogger = data_tpl['data_type'], get_logger(data_tpl['dp_name'])
    # if data_tpl['in_mod'] in ('cdt', 'udf',):
    #     sf = cs + ns
    #     ndx = 'cust_id' if datatype in sf else 'account_no'
    #     mdjlogger.info("{} {}".format(datatype, ndx))
    #     ndx_nunique = df[ndx].nunique()
    #     dups = df.shape[0] - ndx_nunique
    #     if dups > 0:
    #         mdjlogger.warn(
    #             'dropping {} duplicates from {} to have {} unique records'.format(dups, df.shape[0], ndx_nunique))
    #         df.drop_duplicates(subset=[ndx], keep='first', inplace=True)
    #     else:
    #         mdjlogger.info("no duplicates")
    # mdjlogger.info("checking counts in d2c {} | data type is {}".format(df.shape[0], data_tpl[1]))
    # return df
    pass


def ff_large_file_mgr(file, file_meta):
    # if os.path.getsize(drop_zone + file) >> 20 >= 1:  # todo get back to this for large flat files
    #     for i, df in enumerate(pd.read_csv(
    #             drop_zone + file, dtype=str, keep_default_na=True, sep='|', thousands=',', encoding="ISO-8859-1",
    #             chunksize=15 ** 3, error_bad_lines=False, warn_bad_lines=True)):
    #         route_df((file_meta, file_meta['data_type'], df))
    # else:
    df = pd.read_csv(drop_zone + file, dtype=str, keep_default_na=True, sep='|', thousands=',', encoding="ISO-8859-1",
                     error_bad_lines=False, warn_bad_lines=True)
    df.drop_duplicates(inplace=True)
    rez_dup(file_meta, df)
    route_df((file_meta, file_meta['data_type'], df))
    # return df


def handle_ff_xcpxn(e, expected, file, line, saw):
    # todo
    mdjlog = get_logger(file.split('_')[0])
    mdjlog.info("Data File {} has errors {}".format(file, e))
    emsg = str(e) if not hasattr(e, 'message') else e.message
    errortype = emsg.split('.')[0].strip()
    if errortype == 'Error tokenizing data':
        cerror = emsg.split(':')[1].strip().replace(',', '')
        nums = [n for n in cerror.split(' ') if str.isdigit(n)]
        expected.append(int(nums[0]))
        saw.append(int(nums[2].strip()))
        line.append(int(nums[1]) - 1)
        df = rw_file(drop_zone, file, nums)
    else:
        cerror = 'Unknown'
        print('Unknown Error - 222')
    return df


@app.task(name='xtrct_ws_data')
def xtrct_ws_data(file_meta: DataFiles):
    mdjlog = get_logger(file_meta['file_name'].split('_')[0])
    try:
        file = file_meta['file_name']
        mdjlog.info("Reading Data from WorkSheet from Data File {}".format(file))
        df = pd.read_excel(drop_zone + file, pd.ExcelFile(drop_zone + file).sheet_names[0], dtype=str)
        df.drop_duplicates(inplace=True)
        if df.shape[0] > 0:
            mdjlog.info("Data Read with STATS {}".format(df.shape))
            rez_dup(file_meta, df)
            route_df((file_meta, file_meta['data_type'], df))
        else:
            mdjlog.info('no data. ..')
    except Exception as e:
        mdjlog.error(e)


@app.task(name='xtrct_all_data')
def xtrct_all_data(file_meta: DataFiles):
    try:
        mdjlog = get_logger(file_meta['file_name'].split('_')[0].lower())
        try:
            file = file_meta['file_name']
            mdjlog.info("Reading ALL Data from SpeadSheets in Data File {}".format(file))
            xlwb = pd.ExcelFile(drop_zone + file)
            sh_names = xlwb.sheet_names
            mdjlog.info("data file worksheet names {}".format(sh_names))
            data_list = [confirm_data(name, file) for name in sh_names]
            data_list = [data_item for data_item in data_list if data_item is not None]
            mdjlog.info(data_list)
            for i, sgmnt in enumerate(data_list):
                ws = sgmnt[1]
                df = pd.read_excel(drop_zone + file, ws, dtype=str)
                df.drop_duplicates(inplace=True)
                mdjlog.info('exiting. .. routing {} : {} data'.format(df.shape, sgmnt))
                if df.shape[0] > 0:
                    mdjlog.info("Data Read from worksheet {} with STATS {}".format(ws, df.shape))
                    rez_dup(file_meta, df)
                    route_df((file_meta, sgmnt[0], df))
                else:
                    mdjlog.info('no data. ..')
            mdjlog.info('done. ..')
        except Exception as e:
            mdjlog.error(e)
    except Exception as e:
        mdjlog.error(e)

# legacy
# @app.task(name='xtrct_all_data')
# def xtrct_all_data(file_meta, lgcy):
#     try:
#         mdjlog = get_logger(file_meta['file_name'].split('_')[0].lower())
#         try:
#             file = file_meta['file_name']
#             mdjlog.info("Reading ALL Data from SpeadSheets in Data File {}".format(file))
#             xlwb = openpyxl.load_workbook(drop_zone + file, data_only=True)
#             sh_names = xlwb.get_sheet_names()
#             mdjlog.info("\ndata file worksheet names {}\n".format(sh_names))
#             data_list = [confirm_data(name, file) for name in sh_names]
#             data_list = [data_item for data_item in data_list if data_item is not None]
#             mdjlog.info(data_list)
#             for i, sgmnt in enumerate(data_list):
#                 ws = xlwb.get_sheet_by_name(data_list[i][1])
#                 datal = list(ws.values)
#                 cols = next(ws.values)[:]
#                 datai = (islice(r, 0, None) for r in datal)
#                 df = pd.DataFrame(datai, columns=cols, index=None)
#                 mdjlog.info('exiting. .. routing {} : {} data'.format(df.shape, sgmnt))
#                 if df.shape[0] > 0:
#                     mdjlog.info("Data Read from worksheet {} with STATS {}".format(data_list[i][1], df.shape))
#                     rez_dup(file_meta, df)
#                     route_df((file_meta, sgmnt[0], df))
#                 else:
#                     mdjlog.info('no data. ..')
#             mdjlog.info('exiting. ..')
#         except Exception as e:
#             mdjlog.error(e)
#     except Exception as e:
#         mdjlog.error(e)

# @app.task(name='xtrct_ws_data')
# def xtrct_ws_data(file_meta, batch_no=None):
#     mdjlog = get_logger(file_meta['file_name'].split('_')[0])
#     try:
#         file = file_meta['file_name']
#         mdjlog.info("Reading Data from WorkSheet from Data File {}".format(file))
#         xlwb = openpyxl.load_workbook(drop_zone + file, data_only=True)
#         sh_names = xlwb.get_sheet_names()
#         ws = xlwb.get_sheet_by_name(sh_names[0])
#         data = list(ws.values)
#         cols = next(ws.values)[:]  # todo override this
#         data = (islice(r, 0, None) for r in data[1:])
#         df = pd.DataFrame(data, columns=cols)
#         if df.shape[0] > 0:
#             mdjlog.info("Data Read with STATS {}".format(df.shape))
#             rez_dup(file_meta, df)
#             return route_df((file_meta, file_meta['data_type'], df))
#         else:
#             mdjlog.info('no data. ..')
#     except Exception as e:
#         mdjlog.error(e)
