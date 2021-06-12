import multiprocessing

import pandas as pd
import sys
import threading
import time
import zipfile

from datetime import datetime
from inspect import stack
from logging import Formatter, getLogger, INFO, StreamHandler
from logging.handlers import TimedRotatingFileHandler
from os import getpid, listdir, makedirs, pardir, path
from os.path import abspath, exists, join
from random import randint
from sqlalchemy import create_engine

level = INFO
curfilePath = abspath(__file__)
curDir = drop_zone = abspath(
    join(curfilePath, pardir))  # this will return current directory in which python file resides.
log_dir = curDir + path.sep + 'logs' + path.sep
nlv = "\r\n"

ioc_tns = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.1.16)(PORT = 1521)) '
ioc_tns += '(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = crcpdb01) ) )'
ioc_con = create_engine("oracle+cx_oracle://ioc:IoC@" + ioc_tns)

sb2pub_tns = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.1.20)(PORT = 1521)) '
sb2pub_tns += '(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = sb2prod) ) )'
sb2pub_con = create_engine("oracle+cx_oracle://IoC:IoC@" + sb2pub_tns)

loggers = {}


def get_logger(logger_name=None, level=level):
    global loggers
    # try:
    if loggers.get(logger_name):
        return loggers.get(logger_name)
    else:
        logger_name = stack()[1][3].replace('<', '').replace('>', '') if not logger_name else logger_name
        l = getLogger(logger_name)
        l.propagate = False
        formatter = Formatter(
            # '%(processName)s : %(process)s | %(threadName)s : %(thread)s:\n'
            '%(process)s - %(thread)s @ '
            '%(asctime)s {%(name)s:%(lineno)10d  - %(funcName)23s()} %(levelname)s - %(message)s')
        # '[%(asctime)s] - {%(name)s:%(lineno)d  - %(funcName)20s()} - %(levelname)s - %(message)s')
        # fileHandler = TimedRotatingFileHandler(log_dir + '%s.log' % logger_name, mode='a')
        if not exists(log_dir): makedirs(log_dir)
        if l.handlers:
            l.handlers = []
        fileHandler = TimedRotatingFileHandler(log_dir + '%s.log' % logger_name)
        fileHandler.setFormatter(formatter)
        streamHandler = StreamHandler()
        streamHandler.setFormatter(formatter)

        l.setLevel(level)
        l.addHandler(fileHandler)
        l.addHandler(streamHandler)
        loggers.update(dict(name=logger_name))

        return l


def watch():
    before = dict([(f, None) for f in listdir(drop_zone)])
    mdjlog.debug("before: " + ", ".join(before) + nlv)
    while 1:
        after = dict([(f, None) for f in listdir(drop_zone)])
        added = [f for f in after if not f in before]
        removed = [f for f in before if not f in after]
        mdjlog.debug("Removed: " + ", ".join(removed) + nlv)
        if removed:
            std_out("Removed: " + ", ".join(removed), nlv)
        mdjlog.debug("Added: " + ", ".join(removed) + nlv)
        if added:
            std_out("Added: " + ", ".join(added), nlv)
            for file in added:
                count_down()
                yield file
                count_down()
        count_down()
        before = after
    count_down()


def parse_log_file(zip_log):
    zip_ref = zipfile.ZipFile(zip_log, 'r')
    file = zip_ref.namelist()[0]
    stage = file.split('_')[1]
    for i, line in enumerate(zip_ref.open(file).readlines()):
        line = line.decode()
        if i == 3:
            fn = line.split('/')[2].split(' ')[0]
            fid = line.split('/')[1].split(':')[1]
            dp_name = line[line.find('Institution') + 17:]
            print(dp_name, fid, fn)
        if i == 4:
            category = ' '.join(line.split()[:2])
            try:
                process_date = datetime.strptime(line[line.find('Date') + 5:].strip(), '%m/%d/%Y %I:%M:%S %p')
            except Exception as e:
                mdjlog.error(e)
            print(process_date, category)

    eldata = pd.read_csv(zip_ref.open(file), dtype=str, sep='|', header=6, skipinitialspace=True)
    # , encoding="ISO-8859-1"
    eldata.columns = eldata.columns.str.strip()
    eldata.fillna('', inplace=True)
    eldata['file_name'], eldata['file_id'], eldata['dp_name'], eldata['category'] = fn, fid, dp_name, category
    eldata['process_date'], eldata['stage'] = process_date, stage
    mdjlog.info('Will now write data from {} to DB. Thank you!'.format(file))
    return eldata


def std_out(strv, nlv=None):
    if nlv: sys.stdout.write(nlv)
    sys.stdout.write("\r")
    sys.stdout.write(strv)
    if nlv: sys.stdout.write(nlv)
    sys.stdout.flush()


def count_down(dir=None, down_count=None, msg=None, ):
    iW8 = randint(4, 10)
    for remaining in range(iW8, 0, -1):
        if dir:
            std_out(
                "Please drop new files in this location {} to be processed in {:2d} seconds.".format(dir, remaining))
        elif down_count and not msg:
            std_out("Please wait we will continue in {:2d} seconds.".format(remaining))
        elif msg:
            std_out("Please wait we will {} in {:2d} seconds.".format(msg, remaining, ))
        time.sleep(1)


def summarize_data(dtls_df):
    # write dtls_df to db
    dtls_df['S.NO'] = dtls_df['S.NO'].apply(lambda x: int(x) if str(x).strip().isdigit() else None)
    dtls_df_obj = dtls_df.select_dtypes(['object'])
    dtls_df[dtls_df_obj.columns] = dtls_df_obj.apply(lambda x: x.str.strip())
    # summary_df = dtls_df[
    #     ['file_id', 'dp_name', 'category', 'CREDIT FACILITY ACCOUNT NUMBER', 'SEVERITY', 'ERROR DESCRIPTION']
    # ].groupby(['SEVERITY', 'ERROR DESCRIPTION']).agg('count')
    # summary_df.rename(columns={'CREDIT FACILITY ACCOUNT NUMBER': 'No of Records'}, inplace=True)
    dtls_df.rename(columns={
        'S.NO': 'S_NO', 'CREDIT FACILITY ACCOUNT NUMBER': 'account_no', 'DATA PROVIDER BRANCH ID': 'branch_code'
    }, inplace=True)
    dtls_df.dropna(inplace=True)
    dtls_df.to_sql('ioc_sb2exception_logs', ioc_con, index=False, if_exists='append')
    mdjlog.info("Data written to DB!. Thank you again.")
    # print(summary_df)
    # return summary_df
    count_down()


def the_main_pro():
    # mdjlog.info(IoCstr)
    while True:
        mdjlog.info(IoCstr)
        # data_prvdrs_re = dp_meta_data()
        mdjlog.info("watching while True. ..")
        count_down()
        for watched in watch():
            try:
                summarize_data(parse_log_file(watched))
            except Exception as e:
                mdjlog.error(e)
            count_down()


IoCstr = """
#!/IJN:
###               ##
 #       ##      #  #
 #      #  #     #
 #      #  #     #  #
###  #   ##   #   ##   #
"""

mdjlog = get_logger('log_miner')
if __name__ == "__main__":
    the_main_pro()
