# coding: utf-8
import re
from datetime import datetime

from IoCEngine.commons import get_logger

mmddyyyy = "^((([1-9])|([0][1-9])|([1][0-2]))\/(([1-9])|([0-2][1-9])|([1-3][0-1]))\/\d{4})$"
mm_dd_yyyy = "^((([1-9])|([0][1-9])|([1][0-2]))\-(([1-9])|([0-2][1-9])|([1-3][0-1]))\-\d{4})$"
dd_mmm_yy = "^((([1-9])|([0-2][1-9])|([1-3][0-1]))\-(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\-\d{2})$"
dd_mmm_yyyy = "^((([1-9])|([0-2][1-9])|([1-3][0-1]))\-(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\-\d{4})$"
ddmmyyyy = "^((([1-9])|([0-2][1-9])|([1-3][0-1]))\/(([1-9])|([0][1-9])|([1][0-2]))\/\d{4})$"
y4mmdd = '^\d{4}\-(([1-9])|([0][1-9])|([1][0-2]))\-(([1-9])|([0-2][1-9])|([1-3][0-1]))$'
y4m2 = '^\d{4}(([1-9])|([0][1-9])|([1][0-2]))(([1-9])|([0-2][1-9])|([1-3][0-1]))$'
dd_mm_yyyy = '^(([1-9])|([0-2][1-9])|([1-3][0-1]))\-((([1-9])|([0][1-9])|([1][0-2]))\-\d{4})$'
mmddyyyy = '^((([1-9])|([0][1-9])|([1][0-2]))\-(([1-9])|([0-2][1-9])|([1-3][0-1]))\-\d{4})$'
y4mmdd_ts = '^\d{4}\-(([1-9])|([0][1-9])|([1][0-2]))\-(([1-9])|([0-2][1-9])|([1-3][0-1]))\s\d{2}:\d{2}:\d{2}$'
mmm_dd_yyyy = '^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\-(([1-9])|([0-2][1-9])|([1-3][0-1]))\-(\d{4})$'


def transform_date(x):
    mdjlog = get_logger(__name__)
    xr = None
    try:
        if isinstance(x, datetime):
            xr = x.strftime('%d-%b-%Y')

        try:
            if d8mtch(dd_mmm_yyyy, str(x).strip()):
                xr = x
        except Exception as e:
            mdjlog.error('{} | {}'.format(x, e))

        try:
            if d8mtch(y4mmdd, str(x).strip()):
                xr = x.strftime('%d-%b-%Y')
        except Exception as e:
            mdjlog.error('{} | {}'.format(x, e))

        try:
            if d8mtch(y4mmdd_ts, str(x).strip().strip()):
                xr = datetime.strptime(str(x).strip(), "%Y-%m-%d %H:%M:%S").strftime('%d-%b-%Y')
        except Exception as e:
            mdjlog.error('{} | {}'.format(x, e))

        try:
            if d8mtch(y4m2, str(x).strip()):
                xr = datetime.strptime(str(x).strip(), "%Y%m%d").strftime('%d-%b-%Y')
        except ValueError as e:
            mdjlog.error('{} | {}'.format(x, e))

        try:
            if d8mtch(ddmmyyyy, str(x).strip()):
                xr = datetime.strptime(str(x).strip(), "%d/%m/%Y").strftime('%d-%b-%Y')
        except ValueError as e:
            mdjlog.error('{} | {} buh will maneuver'.format(x, e))

            try:
                if d8mtch(mmddyyyy, str(x).strip()):
                    xr = datetime.strptime(str(x).strip(), "%m/%d/%Y").strftime('%d-%b-%Y')
            except Exception as e:
                mdjlog.error('{} | {}'.format(x, e))

        try:
            if d8mtch(dd_mm_yyyy, str(x).strip()):
                xr = datetime.strptime(str(x).strip(), "%d-%m-%Y").strftime('%d-%b-%Y')
        except Exception as e:
            mdjlog.error('{} | {}'.format(x, e))

            try:
                if d8mtch(mm_dd_yyyy, str(x).strip()):
                    xr = datetime.strptime(str(x).strip(), "%m-%d-%Y").strftime('%d-%b-%Y')
            except Exception as e:
                mdjlog.error('{} | {}'.format(x, e))

        return xr
    except Exception as e:
        mdjlog.error('{} | {}'.format(x, e))
    if not xr:
        return None


def d8mtch(ptrn, strng):
    pattern = re.compile(ptrn)
    if pattern.match(strng.lower().strip()):
        return True
    return False


def transform_date_v0(x):
    mdjlog = get_logger(__name__)
    try:
        if isinstance(x, datetime):
            return x.strftime('%d-%b-%Y')

        if d8mtch(dd_mmm_yyyy, str(x).strip()):
            return x

        if d8mtch(y4mmdd, str(x).strip()):
            return x.strftime('%d-%b-%Y')

        if d8mtch(y4mmdd_ts, str(x).strip().strip()):
            return datetime.strptime(str(x).strip(), "%Y-%m-%d %H:%M:%S").strftime('%d-%b-%Y')

        if d8mtch(ddmmyyyy, str(x).strip()):
            return datetime.strptime(str(x).strip(), "%d/%m/%Y").strftime('%d-%b-%Y')

        if d8mtch(mmm_dd_yyyy, str(x).strip()):
            return datetime.strptime(str(x).strip(), "%b-%d-%Y").strftime('%d-%b-%Y')

        if d8mtch(dd_mm_yyyy, str(x).strip()):
            return datetime.strptime(str(x).strip(), "%d-%m-%Y").strftime('%d-%b-%Y')

        """        if d8mtch(mmddyyyy, str(x).strip()):
            return datetime.strptime(str(x).strip(), "%m-%d-%Y").strftime('%d-%b-%Y')"""

    except Exception as e:
        mdjlog.error('{} | {}'.format(x, e))
        return ''
