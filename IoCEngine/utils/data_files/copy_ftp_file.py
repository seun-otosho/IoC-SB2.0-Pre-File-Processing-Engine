import os
from datetime import datetime
from shutil import copy2

from IoCEngine.commons import count_down, dp_meta_data

dir_path = os.path.dirname(os.path.realpath(__file__))
data_prvdrs_re = dp_meta_data()


# use dpid to get institution short name
def mmbr_name(dpid):
    for mmbr in data_prvdrs_re:
        if data_prvdrs_re[mmbr][0] == dpid:
            return mmbr
        return dpid


def copy_sbmttd_file(fp, dest):
    msg = 'copying {} to {}'.format(fp, dest)
    print(msg)
    count_down(msg, 123)
    copy2(fp, dest)


def process_ftp_files(rtrnd):
    for pth in rtrnd:
        print('\nprocessing path :\n\t{}'.format(pth))
        if os.path.isfile(pth):
            head, tail = os.path.split(pth)
            dpid = head.split('\\')[6]
            mmbr = mmbr_name(dpid)
            print('\nfile: {}\ndpid: {}\nname: {}'.format(tail, dpid, mmbr))

            with open("Data Submission as at {}.txt".format(str(datetime.now().date())), "a") as myfile:
                myfile.write("|".join((dpid, mmbr, tail, str(datetime.now()))))

            copy_sbmttd_file(pth, dir_path + os.sep + '_'.join((mmbr, tail)))
            print('#' * 123)
