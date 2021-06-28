from IoCEngine.commons import cs, dict_dotter, fs, ns
from IoCEngine.utils.data_modes import min_mndtry
from IoCEngine.SHU.d8s import to_date
from IoCEngine.utils.file import get_d8rprt3D, pym_db
from IoCEngine.logger import get_logger

severity_levels = {
    'alert': 1,
    'field': 2,
    'segment': 3,
    'structure': 4,
}


def xcpxn_rex(facdf, subdf, sgmnt, btch):
    mandatory_fields(facdf, subdf, sgmnt, btch)
    over_dues(btch, facdf)
    date_reported = get_d8rprt3D(btch.cycle_ver)
    facility_dates(btch, facdf, date_reported)


def mandatory_fields(facdf, subdf, sgmnt, btch):
    mdjlog = get_logger(btch['dp_name'])
    for iSgmnt in btch['segments']:
        try:
            cat = 'Mandatory Field Error'
            if iSgmnt in cs + ns and sgmnt in ('corporate', 'individual',):
                for field in min_mndtry()[iSgmnt]:
                    if field not in subdf:
                        error = ('{} Record was not provided with required field{}.'.format(sgmnt.title(), field))
                        log_df_xcpxns(btch, subdf, (field,), (cat, error,), True)
                    else:
                        xcpxn_df = subdf[subdf[field].isnull()]
                        if not xcpxn_df.empty:
                            error = (
                                "{} Record was provided with required field{}, but it's empty.".format(sgmnt.title(),
                                                                                                       field))
                            log_df_xcpxns(btch, subdf, (field,), (cat, error,), True)
            elif iSgmnt in fs and sgmnt in ('facility',):
                for field in min_mndtry()[iSgmnt]:
                    if field not in facdf:
                        error = ('{} Record was not provided with required field{}.'.format(sgmnt.title(), field))
                        log_df_xcpxns(btch, facdf, (field,), (cat, error,), True)
                        # db_log_xcpxns(btch,None, None,(cat, error,), True)
                    else:
                        xcpxn_df = facdf[facdf[field].isnull()]
                        if not xcpxn_df.empty:
                            error = (
                                "{} Record was provided with required field{}, but it's empty.".format(sgmnt.title(),
                                                                                                       field))
                            log_df_xcpxns(btch, subdf, (field,), (cat, error,), True)
        except Exception as e:
            mdjlog.error(e)


def bvn(subdf, sgmnt, btch):
    pass


def biz_reg_no(subdf, sgmnt, btch):
    pass


def over_dues(btch, facdf):
    mdjlog = get_logger(btch['dp_name'])
    try:
        # df['overdue_amt'] = df.overdue_amt.apply()
        overdue_fields = ['overdue_amt', 'overdue_days']
        principal_overdues(btch, facdf, overdue_fields)

        if btch['in_mod'] not in ('cdt',) and btch['out_mod'] in ('cmb', 'fandl', 'phed',):
            # should be validated for interest overdue amount and days
            interest_overdues(btch, facdf, overdue_fields)
    except Exception as e:
        mdjlog.error(e)


def principal_overdues(btch, facdf, overdue_fields):
    mdjlog = get_logger(btch['dp_name'])
    try:
        all_null = (facdf.overdue_amt.isnull() & facdf.overdue_days.isnull())
        facdf.loc[all_null, 'overdue_amt'] = 0
        facdf.loc[all_null, 'overdue_days'] = 0
        excpxn_df = facdf[(~facdf.overdue_amt.isnull() & ~facdf.overdue_days.isnull())]
        xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', ('Principal Overdue Amount and Days should both have values.'
                                                           ' One cannot be 0 while the other has value.')
        if not excpxn_df.empty:
            # log exceptions for dp, cycle_ver, cust_id, account_no, fields, exception
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'overdue_amt', 'overdue_days']] if btch['in_mod'] in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'overdue_amt', 'overdue_days']]
            df_dict = df.to_dict('records')
            for d in df_dict:
                rec = dict_dotter(d)
                if pssbl_number(rec):
                    if (int(float(rec.overdue_amt)) == 0 and int(float(rec.overdue_days)) > 0) or (
                                    int(float(rec.overdue_amt)) > 0 and int(float(rec.overdue_days)) == 0):
                        dd = {k: str(v) for k, v in d.items() if
                              k in ('dp_name', 'dpid', 'account_no', 'cust_id', 'overdue_amt', 'overdue_days',)}
                        dd['cycle_ver'], dd['fields'], dd['level'] = btch['cycle_ver'], overdue_fields, 4
                        db_log_xcpxns(btch, dd, rec, (xcpxn_cat, xcpxn_desc,))
    except Exception as e:
        mdjlog.error(e)


def pssbl_number(rec):
    try:
        isnumeric_ = (str(rec.overdue_amt).replace(' ', '').replace(',', '').replace('.', '').isnumeric() and str(
            rec.overdue_days).replace(' ', '').replace(',', '').replace('.', '').isnumeric())
        return isnumeric_
    except:
        isnumeric_ = (
            str(rec.int_overdue_amt).replace(' ', '').replace(',', '').replace('.', '').isnumeric() and str(
                rec.int_overdue_days).replace(' ', '').replace(',', '').replace('.', '').isnumeric())
        return isnumeric_


def interest_overdues(btch, facdf, overdue_fields):
    mdjlog = get_logger(btch['dp_name'])
    try:
        all_int_null = (facdf.int_overdue_amt.isnull() & facdf.int_overdue_days.isnull())
        facdf.loc[all_int_null, 'int_overdue_amt'] = 0
        facdf.loc[all_int_null, 'int_overdue_days'] = 0
        excpxn_df = facdf[(~facdf.int_overdue_amt.isnull() & ~facdf.int_overdue_days.isnull())]
        xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', ('Interest Overdue Amount and Days should both have values.'
                                                           ' One cannot be 0 while the other has value.')
        if not excpxn_df.empty:
            # log exceptions for dp, cycle_ver, cust_id, account_no, fields, exception
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'int_overdue_amt', 'int_overdue_days']
            ] if btch['in_mod'] in ('cdt',) else excpxn_df[
                ['dp_name', 'account_no', 'int_overdue_amt', 'int_overdue_days']]
            df_dict = df.to_dict('records')
            for d in df_dict:
                rec = dict_dotter(d)
                if pssbl_number(rec):
                    if (int(rec.int_overdue_amt) == 0 and int(rec.int_overdue_days) > 0) or (
                                    int(rec.int_overdue_amt) > 0 and int(rec.int_overdue_days) == 0):
                        dd = {k: str(v) for k, v in d.items() if
                              k in ('dp_name', 'dpid', 'account_no', 'cust_id', 'int_overdue_amt', 'int_overdue_days',)}
                        dd['level'] = 4
                        db_log_xcpxns(btch, dd, overdue_fields, rec, (xcpxn_cat, xcpxn_desc,))
    except Exception as e:
        mdjlog.error(e)


def facility_dates(btch, facdf, date_reported):
    mdjlog = get_logger(btch['dp_name'])

    try:
        facdf['approved_date'] = facdf.d8_approved.apply(to_date)
    except Exception as e:
        mdjlog.warning(e)
    try:
        facdf['date_acct_clsd'] = facdf.acct_clsd_date.apply(to_date)
    except Exception as e:
        mdjlog.warning(e)
    try:
        facdf['first_disbursed_date'] = facdf.d8_disbursed.apply(to_date)
    except Exception as e:
        mdjlog.warning(e)
    try:
        facdf['date_restructured'] = facdf.amend_date.apply(to_date)
    except Exception as e:
        mdjlog.warning(e)
    try:
        facdf['date_planned_close'] = facdf.maturity_date.apply(to_date)
    except Exception as e:
        mdjlog.warning(e)

    try:
        facdf['last_payment_date'] = facdf.last_paid_date.apply(to_date)
    except Exception as e:
        mdjlog.warning(e)
    #
    # resolve column for validation
    # try:
    #     facdf['last_payment_date'] = facdf.last_paid_date.apply(to_date)
    # except Exception as e:
    #     mdjlog.error(e)
    # resolve column for validation
    #
    in_mod = btch['in_mod']
    # with date reported
    #
    try:  # date acct closed <= current date / date reported / file creation date
        gt_date_reported = facdf['date_acct_clsd'] > date_reported
        excpxn_df = facdf[gt_date_reported]
        if not excpxn_df.empty:
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'date_acct_clsd']] if in_mod in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'date_acct_clsd']]
            xcpxn_cat, xcpxn_desc = 'Field Value Error', (
                "Facility's closed date can not be later than data submission date.")
            xcpxn_fields = ('dp_name', 'dpid', 'cust_id', 'account_no', 'date_acct_clsd',)
            fx_xcpxn_df(btch, df, (xcpxn_cat, xcpxn_desc,), xcpxn_fields)
    except Exception as e:
        mdjlog.warning(e)

    try:  # date loan first disbursed <= current date / date reported / file creation date
        gt_date_reported = facdf['first_disbursed_date'] > date_reported
        excpxn_df = facdf[gt_date_reported]
        if not excpxn_df.empty:
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'd8_disbursed']] if in_mod in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'd8_disbursed']]
            xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', (
                "Facility's first disbursement date can not be later than data submission date.")
            xcpxn_fields = ('dp_name', 'dpid', 'cust_id', 'account_no', 'd8_disbursed')
            fx_xcpxn_df(btch, df, (xcpxn_cat, xcpxn_desc,), xcpxn_fields)
    except Exception as e:
        mdjlog.warning(e)

    try:  # last payment date <= current date / date reported / file creation date
        gt_date_reported = facdf['last_payment_date'] > date_reported
        excpxn_df = facdf[gt_date_reported]
        if not excpxn_df.empty:
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'last_paid_date']] if in_mod in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'last_paid_date']]
            xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', (
                "Facility's principal last payment date can not be later than data submission date.")
            xcpxn_fields = ('dp_name', 'dpid', 'cust_id', 'account_no', 'last_paid_date',)
            fx_xcpxn_df(btch, df, (xcpxn_cat, xcpxn_desc,), xcpxn_fields)
    except Exception as e:
        mdjlog.warning(e)

    # interest payment date <= current date / date reported / file creation date
    # try:
    #     gt_date_reported = facdf['last_payment_date'] > date_reported
    #     excpxn_df = facdf[gt_date_reported]
    #     if not excpxn_df.empty:
    #         df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'date_acct_clsd']] if btch['in_mod'] in (
    #             'cdt',) else excpxn_df[['dp_name', 'account_no', 'date_acct_clsd']]
    #         xcpxn_desc = "Facility's interest last payment date can not be later than data submission date."
    #         xcpxn_fields = ['last_paid_date']
    #         fx_xcpxn_df(btch, df, xcpxn_desc, xcpxn_fields)
    # except Exception as e:
    #     mdjlog.error(e)

    try:  # approved date <= current date / date reported / file creation date
        gt_date_reported = facdf['approved_date'] > date_reported
        excpxn_df = facdf[gt_date_reported]
        if not excpxn_df.empty:
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'd8_approved']] if in_mod in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'd8_approved']]
            xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', (
                "Facility's principal approved date can not be later than data submission date.")
            xcpxn_fields = ('dp_name', 'dpid', 'cust_id', 'account_no', 'd8_approved',)
            fx_xcpxn_df(btch, df, (xcpxn_cat, xcpxn_desc,), xcpxn_fields)
    except Exception as e:
        mdjlog.warning(e)

    #
    # inter fields
    #
    try:  # date loan first disbursed <= date acct closed
        d8closed_gt_d8dsbrsd = facdf['first_disbursed_date'] > facdf['date_acct_clsd']
        excpxn_df = facdf[d8closed_gt_d8dsbrsd]
        if not excpxn_df.empty:
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'd8_disbursed', 'date_acct_clsd']] if in_mod in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'd8_disbursed', 'date_acct_clsd']]
            xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', (
                "Facility's first dibursed date can not be later than closed date.")
            xcpxn_fields = ('dp_name', 'dpid', 'cust_id', 'account_no', 'd8_disbursed', 'date_acct_clsd',)
            fx_xcpxn_df(btch, df, (xcpxn_cat, xcpxn_desc,), xcpxn_fields)
    except Exception as e:
        mdjlog.warning(e)

    try:  # date loan first disbursed <= last payment date
        d8closed_gt_d8lastpay = facdf['first_disbursed_date'] > facdf['last_payment_date']
        excpxn_df = facdf[d8closed_gt_d8lastpay]
        if not excpxn_df.empty:
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'd8_disbursed', 'last_paid_date']] if in_mod in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'd8_disbursed', 'last_paid_date']]
            xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', (
                "Facility's first disbursed date can not be later than last payment date.")
            xcpxn_fields = ('dp_name', 'dpid', 'cust_id', 'account_no', 'd8_disbursed', 'last_paid_date',)
            fx_xcpxn_df(btch, df, (xcpxn_cat, xcpxn_desc,), xcpxn_fields)
    except Exception as e:
        mdjlog.warning(e)

    try:  # date loan first disbursed <= date planned close
        d8closed_gt_d8lastpay = facdf['first_disbursed_date'] > facdf['date_planned_close']
        excpxn_df = facdf[d8closed_gt_d8lastpay]
        if not excpxn_df.empty:
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'd8_disbursed', 'maturity_date']] if in_mod in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'd8_disbursed', 'maturity_date']]
            xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', (
                "Facility's first dibursed date can not be later than maturity date.")
            xcpxn_fields = ('dp_name', 'dpid', 'cust_id', 'account_no', 'd8_disbursed', 'maturity_date',)
            fx_xcpxn_df(btch, df, (xcpxn_cat, xcpxn_desc,), xcpxn_fields)
    except Exception as e:
        mdjlog.warning(e)

    try:  # date loan first disbursed <= approved date
        d8closed_gt_d8lastpay = facdf['first_disbursed_date'] < facdf['approved_date']
        excpxn_df = facdf[d8closed_gt_d8lastpay]
        if not excpxn_df.empty:
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'd8_disbursed', 'd8_approved']] if in_mod in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'd8_disbursed', 'd8_approved']]
            xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', (
                "Facility's first dibursed date should be later than or same as approved date.")
            xcpxn_fields = ('dp_name', 'dpid', 'cust_id', 'account_no', 'd8_disbursed', 'd8_approved',)
            fx_xcpxn_df(btch, df, (xcpxn_cat, xcpxn_desc,), xcpxn_fields)
    except Exception as e:
        mdjlog.warning(e)

    try:  # date loan restructured <= date acct closed
        d8closed_gt_d8lastpay = facdf['date_restructured'] > facdf['date_acct_clsd']
        excpxn_df = facdf[d8closed_gt_d8lastpay]
        if not excpxn_df.empty:
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'amend_date', 'acct_clsd_date']] if in_mod in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'amend_date', 'acct_clsd_date']]
            xcpxn_cat, xcpxn_desc = 'Dependent Fields Error', (
                "Facility's restructured date should be earlier than or same as closed date.")
            xcpxn_fields = ('dp_name', 'dpid', 'cust_id', 'account_no', 'amend_date', 'acct_clsd_date',)
            fx_xcpxn_df(btch, df, (xcpxn_cat, xcpxn_desc,), xcpxn_fields)
    except Exception as e:
        mdjlog.warning(e)


def fx_xcpxn_df(btch, df, xcpxn, xcpxn_fields):
    df_dict = df.to_dict('records')
    for d in df_dict:
        rec = dict_dotter(d)
        dd = {k: str(v) for k, v in d.items()}
        dd['cycle_ver'], dd['fields'], dd['level'] = btch['cycle_ver'], ', '.join(xcpxn_fields), 4
        db_log_xcpxns(btch, dd, rec, xcpxn, None, xcpxn_fields)


def db_log_sbjt_xcpxns(btch, dd, rec, xcpxn_desc):
    mdjlog = get_logger(btch['dp_name'])
    try:
        sd = {'dp_name': rec.dp_name, 'cust_id': rec.cust_id, 'account_no': rec.account_no,
              'cycle_ver': btch['cycle_ver'], 'exception': xcpxn_desc, } if not btch['in_mod'] in ('cdt',) else {
            'dp_name': rec.dp_name, 'account_no': rec.account_no, 'cycle_ver': btch['cycle_ver'],
            'exception': xcpxn_desc, }

        pym_db['data_exceptions'].update(sd, {'$set': dd}, upsert=True)
    except Exception as e:
        mdjlog.error(e)


def db_log_xcpxns(btch, dd, rec, xcpxns, subjt=None, fields=None):
    mdjlog, xcpxn_cat, xcpxn_desc = get_logger(btch['dp_name']), xcpxns[0], xcpxns[1]
    try:
        if subjt:
            sd = {'dpid': btch['dpid'], 'dp_name': rec.dp_name, 'cust_id': rec.cust_id, 'account_no': rec.account_no,
                  'cycle_ver': btch['cycle_ver'], 'category': xcpxn_cat, 'exception': xcpxn_desc,
                  } if not btch['in_mod'] in ('cdt',) else {
                'dpid': btch['dpid'], 'dp_name': rec.dp_name, 'account_no': rec.account_no,
                'cycle_ver': btch['cycle_ver'], 'category': xcpxn_cat, 'exception': xcpxn_desc, }
        else:
            sd = {'dpid': btch['dpid'], 'dp_name': rec.dp_name, 'cust_id': rec.cust_id, 'account_no': rec.account_no,
                  'cycle_ver': btch['cycle_ver'], 'exception': xcpxn_desc, } if btch['in_mod'] in ('cdt',) else {
                'dpid': btch['dpid'], 'dp_name': rec.dp_name, 'account_no': rec.account_no,
                'cycle_ver': btch['cycle_ver'], 'category': xcpxn_cat, 'exception': xcpxn_desc, }
        ds = {f: dd[f] for f in fields if f in dd.keys()}
        ds.update(sd)
        pym_db['data_exceptions'].update(sd, {'$set': ds}, upsert=True)
    except Exception as e:
        mdjlog.error(e)


def log_df_xcpxns(btch, df, fields, xcpxns, subjt=None):
    mdjlog = get_logger(btch['dp_name'])
    try:
        df.dropna(axis='columns', how='all', inplace=True)
        df_dict = df.to_dict('records')
        for d in df_dict:
            rec = dict_dotter(d)
            dd = {k: str(d[k]) for k in d.keys() if not str(d[k]).strip() == ''}
            fj = ', '.join(fields) if fields else None
            dd['cycle_ver'], dd['fields'], dd['level'] = btch['cycle_ver'], fj, 4
            db_log_xcpxns(btch, dd, rec, xcpxns, subjt, fields)
    except Exception as e:
        mdjlog.error(e)


"""

dp_name = ''
dpid = ''
cyle_ver
cust_id = ''
account_no
fields = []
exception = ''

"""
