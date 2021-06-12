from __future__ import absolute_import

import multiprocessing
import os
import os.path
import random
import string
import sys
import time
from datetime import datetime
from random import randint

from IoCEngine import xtrcxn_area
from IoCEngine.logger import get_logger
from IoCEngine.models import CtgryCode, DataProvider

# db = sqlite3.connect('staging.db')

# from commons import mk_dir

# mpcores = multiprocessing.cpu_count() - 1


def right_now():
    return datetime.now().strftime("%A, %d, %B, %Y %H:%M:%S")


def time_t():
    return datetime.now().strftime("%H%M")

def ryna():
    return datetime.now().strftime("%y%b%d%H%M")


def mk_dir(my_dir_path, plof=None):
    try:
        my_dir_path = my_dir_path[:-1] if my_dir_path.endswith('/') else my_dir_path
        head, tail = os.path.split(my_dir_path)
        mdjlog = get_logger(tail.lower())
        try:
            if not os.path.exists(my_dir_path):
                try:
                    os.makedirs(my_dir_path)
                    # mdjlog.info(my_dir_path)
                except Exception as e:  # Guard against race condition
                    # mdjlog.error(e)
                    pass
            else:
                dir_cdtime = datetime.fromtimestamp(os.stat(my_dir_path).st_ctime).date()
                dir_mdtime = datetime.fromtimestamp(os.stat(my_dir_path).st_mtime).date()
                dir_dtime = dir_mdtime if dir_mdtime < dir_cdtime else dir_cdtime
                if dir_dtime < datetime.today().date():
                    # rename dir .strftime("%Y%b%d%a")
                    try:
                        # temp_dir = my_dir_path + dir_dtime.strftime("%Y%b%d%a")
                        # shutil.move(my_dir_path, temp_dir)
                        # shutil.move(temp_dir, my_dir_path + os.sep + dir_dtime.strftime("%Y%b%d%a"))
                        back_up_dir = my_dir_path + os.sep + dir_dtime.strftime("%Y%b%d%a")
                        os.makedirs(back_up_dir)
                        [os.rename(os.path.join(my_dir_path, fname), os.path.join(back_up_dir, fname)) for fname in
                         os.listdir(my_dir_path) if os.path.isfile(os.path.join(my_dir_path, fname))]

                        time.sleep(16)
                        os.makedirs(os.path.dirname(my_dir_path))
                        mdjlog.info(my_dir_path)
                    except Exception as e:
                        # mdjlog.error(e)
                        pass

        except Exception as e:
            mdjlog.error(e)
    except Exception as e:
        mdjlog.error(e)


def id_generator(chars=string.ascii_uppercase + string.digits, size=5, ):
    chars = chars.replace('Z012', '').replace('5', '').replace('I', '').replace('O', '').replace('S', '')
    return ''.join(random.choice(chars) for _ in range(size))


def getID():
    return id_generator()


def mk_dp_x_dir(dp_name):
    mk_dir(xtrcxn_area)
    xtrcxn_zone = xtrcxn_area
    xtrcxn_zone += os.path.sep + dp_name + os.path.sep if os.sep not in dp_name else dp_name
    mk_dir(xtrcxn_zone)
    return xtrcxn_zone


def count_down(dir=None, down_count=None, ):
    iW8, spcr = randint(1, 4), '\t' * 14
    for remaining in range(iW8, 0, -1):
        if dir:
            std_out(
                "{}Please drop files in {} to be processed in {:4d} seconds.".format(spcr, dir, remaining))
        elif down_count:
            std_out("Please wait we will continue in {:4d} seconds.".format(remaining))
        time.sleep(1)


def std_out(strv, nlv=None):
    if nlv: sys.stdout.write(nlv)
    sys.stdout.write("\r")
    sys.stdout.write(strv)
    if nlv: sys.stdout.write(nlv)
    sys.stdout.flush()


all_all_modes = ('all', 'allcomm', 'allcons', 'allcorp', 'allndvdl',)
all_corp_modes = ('allcomm', 'allcorp',)
all_ndvdl_modes = ('allcons', 'allndvdl',)
cdt_udf_modes = ('cdt', 'udf',)
iff_sb2_modes = ('cmb', 'fandl', 'iff', 'mfi', 'pmi',)
cs, fs, ns = ('comm', 'corp',), ('commfac', 'consfac', 'corpfac', 'fac', 'ndvdlfac',), ('cons', 'ndvdl',)
cf, nf, sf = ('commfac', 'corpfac', 'fac',), ('consfac', 'ndvdlfac', 'fac',), ('fac',)
gs, pos = ('grntr',), ('prnc',)
sngl_sbjt_in_modes = cs + fs + ns
sngl_crdt_in_modes = cf + nf + sf

sgmnt_def = {
    'allcorp': 'corporate',
    'corp': 'corporate',
    'allcorp': 'facility',
    'corpfac': 'facility',
    'fac': 'facility',
    'ndvdlfac': 'facility',
    'allndvdl': 'individual',
    'ndvdl': 'individual'
}

submission_type_dict = {
    'com': 'corporate',
    'comm': 'corporate',
    'commfac': 'corporate',
    'con': 'individual',
    'cons': 'individual',
    'consfac': 'individual',
    'corp': 'corporate',
    'corpfac': 'corporate',
    'fac': 'combined',
    'ndvdl': 'individual',
    'ndvdlfac': 'individual',
    'grntr': 'combined',
    'prnc': 'corporate',
}

data_type_dict = {
    'com': 'subject',
    'comm': 'subject',
    'commfac': 'facility',
    'con': 'subject',
    'cons': 'subject',
    'consfac': 'facility',
    'corp': 'subject',
    'corpfac': 'facility',
    'fac': 'facility',
    'ndvdl': 'subject',
    'ndvdlfac': 'facility',
    'grntr': 'guarantor',
    'prnc': 'officer',
}


def sb2ctgry_file_type_codes(code_name=None):
    if code_name:
        ctgry_code = CtgryCode.query.filter_by(code_name=code_name).first()
        return {ctgry_code.code_name: (ctgry_code.header_code, ctgry_code.file_type_code)}
    else:
        ctgry_codes = CtgryCode.query.all()
        return {ctgry_code.code_name: (ctgry_code.header_code, ctgry_code.file_type_code) for ctgry_code in ctgry_codes}


# ctgry_dtls = sb2ctgry_file_type_codes()


def g_meta(datCat, dp_name, load3Dbatch):
    dp_meta = dp_meta_data(load3Dbatch['dp_name'])
    instCat = dp_meta[dp_name][2]
    sgmnt = instCat + datCat if instCat not in ('cb',) else datCat
    ctgry_dtls = sb2ctgry_file_type_codes(sgmnt)
    return ctgry_dtls, dp_meta


def dp_meta_data(dp_code_name=None):
    if dp_code_name and dp_code_name.isalpha():
        data_prvdr = DataProvider.query.filter_by(code_name=dp_code_name).first()
        return {
            data_prvdr.code_name: (
                data_prvdr.dpid, data_prvdr.sbmxn_pt, data_prvdr.ctgry, data_prvdr.day_first)
        }
    if dp_code_name and dp_code_name.isalnum() and not dp_code_name.isalpha():
        data_prvdr = DataProvider.query.filter_by(dpid=dp_code_name).first()
        return {
            data_prvdr.code_name: (
                data_prvdr.dpid, data_prvdr.sbmxn_pt, data_prvdr.ctgry, data_prvdr.day_first)
        }
    if not dp_code_name:
        data_prvdrs = DataProvider.query.all()
        return {
            data_prvdr.code_name: (
                data_prvdr.dpid, data_prvdr.sbmxn_pt, data_prvdr.ctgry, data_prvdr.day_first) for
            data_prvdr in data_prvdrs
        }
