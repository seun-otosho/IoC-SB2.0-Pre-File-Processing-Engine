import os
from itertools import islice

import openpyxl
import pandas as pd

from IoCEngine import drop_zone
from IoCEngine.celeryio import app
from IoCEngine.commons import mk_dp_x_dir
from IoCEngine.data_router import worksheet_datatype as confirm_data
from IoCEngine.logger import get_logger
from IoCEngine.utils.data2db import route_df


def rw_file(file, nums):
    mdjlog = get_logger(file.split('_')[0])
    try:
        bad_f, orgnl_l, now_orgnl_l = [], [], []
        mdjlog.info("Reading Data File {} for corrections".format(file))
        f = open(file, "rb")
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
        mdjlog.info("wrinting bad file extracted from {}".format(file))
        badf.write('\n'.join([l.strip('\n') for l in bad_f]))
        badf.close()
        hdr = orgnl_l.pop(0)
        return pd.DataFrame([l.split('|') for l in now_orgnl_l], columns=hdr.decode('unicode_escape').split('|'))
    except Exception as e:
        print(e)


@app.task(name='xtrct_ff_data')
def xtrct_ff_data(file_meta, batch_no=None):
    file = file_meta['file_name']
    mdjlog = get_logger(file.split('_')[0])
    cont, expected, line, saw = True, [], [], []

    while cont:
        try:
            if not 'df' in locals():
                df = pd.read_csv(
                    drop_zone + file, dtype=str, keep_default_na=True, sep='|', thousands=',', encoding="ISO-8859-1")
            cont = False
            if df.shape[0] > 0:
                return route_df((file_meta, file_meta['data_type'], df))
            else:
                mdjlog.info('no data. ..')

        except Exception as e:
            df = handle_ff_xcpxn(df, e, expected, file, line, saw)

    if len(line) != 0:
        mdjlog.info(line)

    if df.shape[0] > 0:
        return route_df((file_meta, file_meta['data_type'], df))
    else:
        mdjlog.info('no data. ..')


def handle_ff_xcpxn(df, e, expected, file, line, saw):
    #todo
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
        df = rw_file(drop_zone + file, nums)
    else:
        cerror = 'Unknown'
        print('Unknown Error - 222')
    return df


@app.task(name='xtrct_ws_data')
def xtrct_ws_data(file_meta, batch_no=None):
    mdjlog = get_logger(file_meta['file_name'].split('_')[0])
    try:
        file = file_meta['file_name']
        mdjlog.info("Reading Data from WorkSheet from Data File {}".format(file))
        xlwb = openpyxl.load_workbook(drop_zone + file, data_only=True)
        sh_names = xlwb.get_sheet_names()
        ws = xlwb.get_sheet_by_name(sh_names[0])
        data = list(ws.values)
        cols = next(ws.values)[:]  # todo override this
        data = (islice(r, 0, None) for r in data[1:])
        df = pd.DataFrame(data, columns=cols)
        if df.shape[0] > 0:
            mdjlog.info("Data Read with STATS {}".format(df.shape))
            return route_df((file_meta, file_meta['data_type'], df))
        else:
            mdjlog.info('no data. ..')
    except Exception as e:
        mdjlog.error(e)


@app.task(name='xtrct_all_data')
def xtrct_all_data(file_meta):
    try:
        mdjlog = get_logger(file_meta['file_name'].split('_')[0].lower())
        try:
            file = file_meta['file_name']
            mdjlog.info("Reading ALL Data from SpeadSheets in Data File {}".format(file))
            xlwb = openpyxl.load_workbook(drop_zone + file, data_only=True)
            sh_names = xlwb.get_sheet_names()
            mdjlog.info("\ndata file worksheet names {}\n".format(sh_names))
            data_list = [confirm_data(name, file) for name in sh_names]
            data_list = [data_item for data_item in data_list if data_item is not None]
            for i, sgmnt in enumerate(data_list):
                ws = xlwb.get_sheet_by_name(data_list[i][1])
                datal = list(ws.values)
                cols = next(ws.values)[:]
                datai = (islice(r, 0, None) for r in datal)
                df = pd.DataFrame(datai, columns=cols, index=None)
                mdjlog.info('exiting. .. routing {} : {} data'.format(df.shape, sgmnt))
                if df.shape[0] > 0:
                    mdjlog.info("Data Read from worksheet {} with STATS {}".format(data_list[i][1], df.shape))
                    route_df((file_meta, sgmnt[0], df))
                else:
                    mdjlog.info('no data. ..')
        except Exception as e:
            mdjlog.error(e)
    except Exception as e:
        mdjlog.error(e)
