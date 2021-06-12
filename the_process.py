# coding: utf-8


import datetime
import os
import subprocess
import sys

from subprocess import call, PIPE, run

from IoCEngine import drop_zone
from IoCEngine.celeryio import app
from IoCEngine.commons import count_down, std_out
from IoCEngine.logger import get_logger as logger
from IoCEngine.utils.file import dict_file, xtrct_file_details, DataFiles
from IoCEngine.utils.sb2 import dp_meta_data
from jarvis import route_file, route_filed_data


def messager(msg, msgr=None):
    # global mdjlog
    rslt = run("figlet -w 256 {}".format(msg), shell=True, stdout=PIPE)
    # if not msgr:
    #     msgr = mdjlog
    # msgr.info(f"\n\r{rslt.stdout.decode('utf-8')}")
    return "\n\r{}".format(rslt.stdout.decode('utf-8'))


if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding('utf-8')

nlv = "\r\n"

mdjlog = logger('jarvis')


# celery.config_from_object('config.pilot')


def right_now():
    return datetime.datetime.now().strftime("%A, %d, %B, %Y %H:%M:%S")


def watch():
    before = dict([(f, None) for f in os.listdir(drop_zone)])
    mdjlog.debug("before: " + ", ".join(before) + nlv)
    while 1:
        after = dict([(f, None) for f in os.listdir(drop_zone)])
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
        before = after

        count_down(drop_zone)


def initialize_files(the_files):
    file = the_files[0] if isinstance(the_files, list) else the_files
    mdjlog = logger(the_files[0].split('_')[0]) if isinstance(the_files, list) else logger(the_files.split('_')[0])
    try:
        if not isinstance(file, list):
            init1file(file)
        else:
            for i, file in enumerate(the_files):
                mdjlog = logger(file.split('_')[0])
                init1file(file, i)
    except Exception as e:
        mdjlog.error(e)
        mdjlog.critical("Data file name '{}', is wrong!".format(file))


def init1file(file, i=None):
    file, mdjlog = file, logger(file.split('_')[0])
    mdjlog.info('{}, {} : {}'.format(i, file, len(file.split('_'))))
    if len(file.split('_')) >= 4 \
            and '~' not in file and '#' not in file \
            and '(' not in file and ')' not in file:  # pick only process related files
        file_dtls = xtrct_file_details(file)
        file_id = dict_file(file, file_dtls, mdjlog)
        fp_router(file_id)
        # fp_router.delay(file_id)


def get_dpid(fl_dtls, mdjlog, plf=None):
    mdjlog = logger(fl_dtls['dp_name'])
    """Get the Data Provider ID of the institution whose data file is been processed"""
    data_prvdrs_re = dp_meta_data()
    mmbr = fl_dtls['dp_name']
    if not mmbr in data_prvdrs_re.keys():
        mdjlog.debug('invalid data provider provided')
        fl_dtls['dpid'] = None
        return fl_dtls
    try:
        mdjlog.debug("data_prvdr_id: {}".format(data_prvdrs_re[mmbr][0]))
        fl_dtls['dpid'] = data_prvdrs_re[mmbr][0]
        try:
            if data_prvdrs_re[mmbr][3]: fl_dtls['dayfirst'] = data_prvdrs_re[mmbr][3]['dayfirst']
        except Exception as e:
            mdjlog.debug(e)
        return fl_dtls
    except Exception as e:
        mdjlog.debug(e)


@app.task(name='fp_router')
def fp_router(file_id):
    try:
        mdjlog.info('\n' + '*' * 123)
        fl_dtls = DataFiles.objects.get(id=file_id)
        # first of get data prvdr id
        fl_dtls = get_dpid(fl_dtls, mdjlog)
        try:
            dpid = fl_dtls['dpid'] if fl_dtls['dpid'] else None
        except:
            mdjlog.error('\tPlease maintain institution details for: {}'.format(fl_dtls['dp_name']))
        if dpid:
            fl_dtls.update(dpid=fl_dtls['dpid'])
            route_file(fl_dtls)
            route_filed_data(fl_dtls['dpid'])
        else:
            mdjlog.error('\tPlease maintain institution details for: {}'.format(fl_dtls['dp_name']))
            # end and report for file or file group
    except Exception as e:
        mdjlog.critical(e)
        mdjlog.critical("\n\n\tData File Name Error\n\n")


IoCstr = """
#!/IJN:
###               ##
 #       ##      #  #
 #      #  #     #
 #      #  #     #  #
###  #   ##   #   ##   #
"""


def the_main_pro():
    # data_prvdrs_re = dp_meta_data()
    # mdjlog.info(IoCstr)
    while True:
        mdjlog.info(messager("'#'!iJn"))
        mdjlog.info(messager("IoC . iX"))
        # data_prvdrs_re = dp_meta_data()
        mdjlog.info("watching while True. ..")
        for watched in watch():
            initialize_files(watched)
            mdjlog.info(messager("IoC . iX"))
            count_down()


if __name__ == "__main__":
    the_main_pro()
