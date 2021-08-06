from __future__ import absolute_import

import multiprocessing as mp
import random
import string
import sys
# import time
from datetime import datetime
from functools import wraps
from logging import Logger
from os import sep, makedirs, rename, listdir, stat
from os.path import split, exists, join, isfile, dirname
from random import randint
from time import perf_counter, sleep

import pyfiglet

from IoCEngine import xtrcxn_area
from IoCEngine.logger import get_logger
from IoCEngine.models import CtgryCode, DataProvider

logger = get_logger()
# db = sqlite3.connect('staging.db')

# from commons import mk_dir

# mpcores = multiprocessing.cpu_count() - 1
from figs import font2u


def right_now():
    return datetime.now().strftime("%A, %d, %B, %Y %H:%M:%S")


def time_t():
    return datetime.now().strftime("%H%M")


def ryna():
    return datetime.now().strftime("%y%b%d%H%M")


def mk_dir(my_dir_path, plof=None):
    try:
        my_dir_path = my_dir_path[:-1] if my_dir_path.endswith('/') else my_dir_path
        head, tail = split(my_dir_path)
        mdjlog = get_logger(tail.lower())
        try:
            if not exists(my_dir_path):
                try:
                    makedirs(my_dir_path)
                    # mdjlog.info(my_dir_path)
                except Exception as e:  # Guard against race condition
                    # mdjlog.error(e)
                    pass
            else:
                dir_cdtime = datetime.fromtimestamp(stat(my_dir_path).st_ctime).date()
                dir_mdtime = datetime.fromtimestamp(stat(my_dir_path).st_mtime).date()
                dir_dtime = dir_mdtime if dir_mdtime < dir_cdtime else dir_cdtime
                if dir_dtime < datetime.today().date():
                    try:
                        back_up_dir = my_dir_path + sep + dir_dtime.strftime("%Y%b%d%a")
                        makedirs(back_up_dir)
                        [rename(join(my_dir_path, fname), join(back_up_dir, fname)) for fname in
                         listdir(my_dir_path) if isfile(join(my_dir_path, fname))]

                        sleep(16)
                        makedirs(dirname(my_dir_path))
                        mdjlog.info(my_dir_path)
                    except Exception as e:
                        mdjlog.error(e)
                        # pass

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
    xtrcxn_zone += sep + dp_name + sep if sep not in dp_name else dp_name
    mk_dir(xtrcxn_zone)
    return xtrcxn_zone


def count_down(dir: str = None, down_count: int = None, ):
    iW8, spcr = randint(1, 4), '\t' * 14
    for remaining in range(iW8, 0, -1):
        d, s = '.' * remaining + ' ' * 5, 'seconds' if remaining > 1 else 'second'
        if dir:
            std_out(f"{spcr}Drop your file(s) in {dir} to be processed in {remaining} {s}. {d}")
        elif down_count:
            std_out(f"Hold on, we move in {remaining} {s}. {d}")
        sleep(1)


def std_out(strv: str, nlv=None):
    if nlv:
        sys.stdout.write(nlv)
    sys.stdout.write("\r")
    sys.stdout.write(strv)
    if nlv:
        sys.stdout.write(nlv)
    sys.stdout.flush()


all_all_modes = ('all', 'allcomm', 'allcons', 'allcorp', 'allndvdl',)
all_corp_modes = ('allcomm', 'allcorp',)
all_ndvdl_modes = ('allcons', 'allndvdl',)
cdt_udf_modes = ('cdt', 'udf',)
iff_sb2_modes = ('cmb', 'fandl', 'iff', 'mfi', 'pmi',)
cs, fs, ns = ('comm', 'corp',), ('commfac', 'consfac', 'corpfac', 'fac', 'ndvdlfac',), ('cons', 'ndvdl',)
cf, nf, sf = ('commfac', 'corpfac', 'fac',), ('consfac', 'ndvdlfac', 'fac',), ('fac',)
gs, ps = ('grntr',), ('prnc',)
sngl_sbjt_in_modes = cs + fs + ns
sngl_crdt_in_modes = cf + nf + sf

sgmnt_def = {
    'allcorp': 'corporate',
    'corp': 'corporate',
    # 'allcorp': 'facility',
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
    'prnc': 'principal',
}

data_type_dict = {
    'com': 'subject',
    'comm': 'subject',
    'corp': 'subject',

    'con': 'subject',
    'cons': 'subject',
    'ndvdl': 'subject',

    'commfac': 'facility',
    'corpfac': 'facility',
    'fac': 'facility',
    'consfac': 'facility',
    'ndvdlfac': 'facility',

    'grntr': 'guarantor',
    'prnc': 'officer',
}


def sb2ctgry_file_type_codes(code_name=None):
    if code_name is None:
        ctgry_codes = CtgryCode.query.all()
        return {ctgry_code.code_name: (ctgry_code.header_code, ctgry_code.file_type_code) for ctgry_code in ctgry_codes}
    else:
        ctgry_code = CtgryCode.query.filter_by(code_name=code_name).first()
        return {ctgry_code.code_name: (ctgry_code.header_code, ctgry_code.file_type_code)}


# ctgry_dtls = sb2ctgry_file_type_codes()


def g_meta(datCat, dp_name, load3Dbatch):
    logger = get_logger(dp_name)
    try:
        dp_meta = dp_meta_data(load3Dbatch['dp_name'])
        instCat = dp_meta[dp_name][2]
        sgmnt = instCat + datCat if instCat not in ('cb',) else datCat
        ctgry_dtls = sb2ctgry_file_type_codes(sgmnt)
    except Exception as e:
        logger.error(e)
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


def re_ndx_flds(df, ndx_flds, fillvar=None):
    for fld in [f for f in ndx_flds if f not in df]:
        df.loc[:, fld] = fillvar if fillvar else None


def fig_str(str2fig: str = None):
    str2fig = """Internet
of Credit
,   IoC""" if not str2fig else str2fig
    fig_font = font2u()
    figletstr = pyfiglet.figlet_format(str2fig, font=fig_font)
    return f"""
#!/IJN
{fig_font}
{figletstr}
"""


class dict_dotter(dict):
    """dot.notation access to dictionary attributes"""

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as k:
            raise AttributeError(k)

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __repr__(self):
        return '<Dict Dotter ' + dict.__repr__(self) + '>'


def m_or_s(completed=None, start=None):
    ts = (completed - start).total_seconds() if completed and start else completed
    if round(ts) > 3600:
        h = 'hours' if ts >= 7200 else 'hour'
        return f"{round(ts) // 3600} {h} and {round(ts % 3600)} minutes"
    if round(ts) > 128:
        m = 'minutes' if ts >= 120 else 'minute'
        return f"{round(ts) // 60} {m} and {round(ts) % 60} seconds"
    return f"{round(ts)} seconds" if round(ts) > 0 else f"{round(ts * 1000)} milliseconds"


def profile(fn):
    logger = get_logger()
    @wraps(fn)
    def inner(*args, **kwargs):
        global logger
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items() if not isinstance(v, Logger))
        try:
            logger = [a for a in args if isinstance(a, Logger)][0]
        except Exception as e:
            try:
                logger = [v for k, v in kwargs.items() if isinstance(v, Logger)][0]
            except Exception as e:
                pass
        # logger.info(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = perf_counter()
        retval = fn(*args, **kwargs)
        # logger.info(fig_str("#288"))
        # mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)
        elapsed = perf_counter() - t
        elapsed = m_or_s(elapsed)
        logger.info(f'Profiling::{fn.__name__}({fn_kwargs_str})::~>Time {elapsed}')

        # Measure memory
        # logger.info(f'Profiling::{fn.__name__}({fn_kwargs_str})::~>Memory {max(mem) - min(mem)}')
        return retval

    return inner


def init():
    global curr_dp


def multi_pro(func, args):
    logger = get_logger()
    try:
        p = mp.Process(target=func, args=args, )
        p.start()
    except Exception as e:
        logger.warn(e)
    return True