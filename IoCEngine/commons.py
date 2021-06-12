from __future__ import absolute_import

import logging
import multiprocessing
import os
import random
import string
import sys
import time
from datetime import datetime
from random import randint

from IoCEngine import xtrcxn_area
from IoCEngine.logger import get_logger

# db = sqlite3.connect('staging.db')


from os import makedirs
import os.path


# from commons import mk_dir

mpcores = multiprocessing.cpu_count() - 1

def right_now():
    return datetime.now().strftime("%A, %d, %B, %Y %H:%M:%S")


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
                    mdjlog.info(my_dir_path)
                except Exception as e:  # Guard against race condition
                    mdjlog.error(e)
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
                        mdjlog.error(e)

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


def count_down(dir=None, down_count=None,):
    iW8 = randint(1, 4)
    for remaining in range(iW8, 0, -1):
        if dir:
            std_out(
                "Please drop new files in this location {} to be processed in {:2d} seconds.".format(dir, remaining))
        elif down_count:
            std_out("Please wait we will continue in {:2d} seconds.".format(remaining))
        time.sleep(1)


def std_out(strv, nlv=None):
    if nlv: sys.stdout.write(nlv)
    sys.stdout.write("\r")
    sys.stdout.write(strv)
    if nlv: sys.stdout.write(nlv)
    sys.stdout.flush()