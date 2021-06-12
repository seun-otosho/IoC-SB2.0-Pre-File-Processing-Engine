import os

import datetime
import openpyxl

import pandas as pd

from itertools import islice

from IoCEngine import drop_zone
from IoCEngine.data_router import worksheet_datatype as confirm_data
from IoCEngine.logger import get_logger as logger
from IoCEngine.utils.file_reader import xtrct_all_data
from IoCEngine.utils.file import xtrct_file_details


def init1file(file, i=None, c=None):
    file, mdjlog, start = file, logger(file.split('_')[0]), datetime.datetime.now()
    mdjlog.info('file: {} of {}, {} : {}'.format(i, c, file, len(file.split('_'))))
    if len(file.split('_')) >= 2 \
            and '~' not in file and '#' not in file \
            and '(' not in file and ')' not in file:  # pick only process related files
        file_dtls = xtrct_file_details(file)
        # file_id = dict_file(file, file_dtls, start, mdjlog)
        # fp_router(file_id)
        return file_dtls


drop_zone = drop_zone + os.sep + 'unupdated' + os.sep
for f in os.listdir(drop_zone):
    data_dtls = {}
    file_details = init1file(f, 1, 1)
    data_dtls['dp_name'], data_dtls['in_mod'], data_dtls['file_ext'] = file_details[0], file_details[1], file_details[2]
    data_dtls.update({'file_name': f})
    
    mdjlog = logger(data_dtls['dp_name'])
    
    xlwb = openpyxl.load_workbook(drop_zone + f, data_only=True)
    sh_names = xlwb.get_sheet_names()
    mdjlog.info("\ndata file worksheet names {}\n".format(sh_names))
    data_list = [confirm_data(name, f) for name in sh_names]
    data_list = [data_item for data_item in data_list if data_item is not None]
    mdjlog.info(data_list)
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
