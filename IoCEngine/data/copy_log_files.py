
import os, time

from shutil import copy2, move

from IoCEngine.commons import count_down
from IoCEngine.utils.sb2 import dp_meta_data as data_prvdrs_re

dir_path = os.path.dirname(os.path.realpath(__file__))


def process_ftp_files(rtrnd):
    for pth in rtrnd:
        print('\nprocessing path :\n\t{}'.format(pth))
        if os.path.isfile(pth):
            head, tail = os.path.split(pth)
            dpid = head.split('\\')[6]
            mmbr = mmbr_name(dpid)
            print('\nfile: {}\ndpid: {}\nname: {}'.format(tail, dpid, mmbr))
            copy_sbmttd_file(pth, dir_path + os.sep + '_'.join((mmbr, tail)))
            print('#' * 123)


# use dpid to get institution short name
def mmbr_name(dpid):
    for mmbr in data_prvdrs_re():
        if data_prvdrs_re[mmbr][0] == dpid:
            return mmbr
        return dpid


def copy_sbmttd_file(fp, dest):
    msg = 'copy {}\nto {}'.format(fp, dest)
    # count_down(None, 345, msg)
    copy2(fp, dest)
