def check_days(day):
    if day:
        try:
            ret_num = abs(int(day))
        except:
            try:
                ret_num = abs(int(float(day)))
            except:
                ret_num = 0
        if ret_num >= 1000:
            ret_num = 999
        return ret_num
    else:
        return 0
