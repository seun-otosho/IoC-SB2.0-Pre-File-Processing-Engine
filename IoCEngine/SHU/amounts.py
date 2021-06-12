from numbers import Real

from IoCEngine.commons import get_logger


def round_numbers_iono(num):
    try:
        if isinstance(num, float):
            return abs(int(num))
        if isinstance(num, int):
            return abs(num)
        if ',' in str(num):
            return abs(int(float(num.replace(',', ''))))
    except Exception as e:
        print('\nerror: {} in round function for value: {}\nreturning unchanged'.format(e, num))
        return num


def round_amt(num):
    mdjlog = get_logger(__name__)
    xr = None
    if num and (
                str(num).replace(' ', '').replace('_', '').replace('-', '').replace(',', '').replace('.', '').isdigit()
            or str(num).isdigit()
    ):
        if ' ' in str(num):
            num = str(num).replace(' ', '')
        try:
            if ',' in str(num):
                xr = int(abs(round(float(num.replace(',', '')))))
            if type(num) == 'float':
                xr = int(abs(round(float(num))))
            if isinstance(num, float):  # '.' in str(num):
                xr = int(abs(round(float(num))))
            if isinstance(num, int):
                xr = int(abs(round(num)))
        except Exception as e:
            mdjlog.error('{}'.format(e))
    elif int(num) == 0:
        return 0
    else:
        mdjlog.debug('{}'.format('returning zero'))
        return 0
    return xr


def is_any_real_no(x):
    if isinstance(x, Real):
        return True
    elif isinstance(x, float):
        return True
    elif isinstance(x, int):
        return True
    else:
        return False


def normal_numbers(num):
    mdjlog = get_logger(__name__)
    if num:
        if (
            str(num).replace(' ', '').replace('_', '').replace('-', '').replace(',', '').replace('.',
                                                                                                 '').isdigit()
                or str(num).isdigit()
        ):
            num = str(num).replace(' ', '').replace(
                '_', '').replace('-', '').replace(',', '')
            num = float(num)  # if '.' in str(num) else int(num)

            if str(num) == '':
                return 0.0
            try:

                if isinstance(num, (float, int, Real)):
                    return float(num)
                """
                if ',' in str(num) and '.' in str(num):
                    num = num.replace(',', '')
                    try:
                        return abs(round(float(num)))
                    except:
                        return abs(round(int(num)))
                """

                try:
                    if float(num) == 0:
                        return 0.0
                except:
                    if int(num) == 0:
                        return 0.0

            except Exception as e:
                mdjlog.error('{}'.format(e))
        else:
            if (num.isalnum() and not num.isalpha()) or num:
                # just return the first token that is a number
                num_list = str(num).split()
                nnum, num_count = None, len(num_list)
                for inum in num_list:
                    if inum.isdigit():
                        nnum = float(inum)
                        if 'over' in num or 'above' in num:
                            nnum = float(nnum) + 1
                    num_count -= 1
                    if num_count == 1 and nnum is None:
                        nnum = 0.0
                return nnum

    else:
        mdjlog.debug(
            "not sure what's going on. .. {}\n".format('returning zero'))
        return 0.0



def round_numbers(num):
    mdjlog = get_logger(__name__)
    if num and (
                str(num).replace(' ', '').replace('_', '').replace('-', '').replace(',', '').replace('.', '').isdigit()
            or str(num).isdigit()
    ):
        num = str(num).replace(' ', '').replace('_', '').replace('-', '').replace(',', '')
        num = float(num) if '.' in str(num) else int(num)

        if ' ' in str(num):
            num = str(num).replace(' ', '')
        try:

            if isinstance(num, (float, int, Real)):
                return abs(round(num))
            """
            if ',' in str(num) and '.' in str(num):
                num = num.replace(',', '')
                try:
                    return abs(round(float(num)))
                except:
                    return abs(round(int(num)))
            """

            try:
                if float(num) == 0:
                    return 0
            except:
                if int(num) == 0:
                    return 0

        except Exception as e:
            mdjlog.error('{}'.format(e))
    else:
        mdjlog.debug("not sure what's going on. .. {}\n".format('returning zero'))
        return 0


"""

def round_numbers(num):
    mdjlog = get_logger(__name__)
    # xr = None
    if num and (
                str(num).replace(' ', '').replace('_', '').replace('-', '').replace(',', '').replace('.', '').isdigit()
            or str(num).isdigit()
    ):
        if ' ' in str(num):
            num = str(num).replace(' ', '')
        try:
            if ',' in str(num) and '.' in str(num):
                return int(abs(round(float(num.replace(',', '')))))  # xr =
            if type(num) is 'float' or '.' in str(num):
                return int(abs(round(float(num))))  # xr =
            if isinstance(num, float):  # '.' in str(num):
                return int(abs(round(float(num))))  # xr =
            if isinstance(num, int) or isinstance(int(num), int):
                return int(abs(round(num)))  # xr =
        except Exception as e:
            mdjlog.error('{}'.format(e))
    elif int(num) == 0:
        return 0
    else:
        mdjlog.debug('{}'.format('returning zero'))
        return 0
        # return xr

def rounda(num):
    mdjlog = get_logger(__name__)
    if num and str(num).replace(' ', '').replace('_', '').replace('-', '').replace('.', '').isdigit():
        try:
            if ',' in str(num):
                rounded = int(abs(round(float(num.replace(',', '')))))
                return rounded
            if type(num) is 'float':
                rounded = int(abs(round(float(num))))
                return rounded
            if '.' in str(num):
                rounded = int(abs(round(float(num))))
                return rounded
            return num
        except Exception as e:
            mdjlog.error('{}'.format(e))
    else:
        mdjlog.debug('{}'.format('returning zero'))
        return 0

"""
