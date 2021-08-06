from commons import dict_dotter
from IoCEngine.logger import get_logger
from IoCEngine.utils.file import pym_db

severity_levels = {
    'alert': 1,
    'field': 2,
    'segment': 3,
    'structure': 4,
}

"""

level 4


all segments
    facility branch == subject branch


date acct closed <= current date / date reported / file creation date


last payment date <= current date / date reported / file creation date


date loan restructured <= date acct closed
date loan first disbursed <= date acct closed
date loan first disbursed <= last payment date
date loan first disbursed <= date planned close
date loan first disbursed <= current date / date reported / file creation date

date loan first disbursed >= approved date

maturity date >= approved date

approved date <= current date / date reported / file creation date


interest payment date <= current date / date reported / file creation date
interest payment date <= date acct closed
interest payment date > approved date
interest payment date > date loan first disbursed

principal payment date <= current date / date reported / file creation date
principal payment date <= date acct closed
principal payment date > approved date
principal payment date > date loan first disbursed


date of birth < current date / date reported / file creation date
date of birth < last payment date
date of birth < date loan first disbursed
date of birth < date loan restructured
date of birth < last payment date
date of birth < date planned close

date of birth < principal payment date
date of birth < interest payment date
date of birth < date loan first disbursed
date of birth < date loan restructured
date of birth < approved date


consent status null default is 001 not provided
    consent status == '003'
        consent from date and consent to data
        
        consent to date >= current date / date reported and vice versa
        consent to date >= consent from date and vice versa
        consent to date < current date


principal and interest
    overdue days and overdue days > 0
    overdue amt and overdue amt > 0
    
    overdue amt and overdue days > 0
    overdue days and overdue amt > 0


loan type == 'credit card'
    primary card number


first name
last name
name or (first name and last name)




"""


def xcpxn_rex(facdf, subdf, sgmnt, btch):
    over_dues(btch, facdf)


"""
level 3


"""


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
        xcpxn_desc = 'principal overdue amount and days values are inconsistent'
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
                        dd = {k: str(v) for k, v in d.items()}
                        dd['cycle_ver'], dd['fields'], dd['level'] = btch['cycle_ver'], overdue_fields, 4
                        db_log_xcpxns(btch, dd, rec, xcpxn_desc)
    except Exception as e:
        mdjlog.error(e)


def pssbl_number(rec):
    return (str(rec.overdue_amt).replace(' ', '').replace(',', '').replace('.', '').isnumeric() and str(
        rec.overdue_days).replace(' ', '').replace(',', '').replace('.', '').isnumeric())


def interest_overdues(btch, facdf, overdue_fields):
    mdjlog = get_logger(btch['dp_name'])
    try:
        all_int_null = (facdf.int_overdue_amt.isnull() & facdf.int_overdue_days.isnull())
        facdf.loc[all_int_null, 'int_overdue_amt'] = 0
        facdf.loc[all_int_null, 'int_overdue_days'] = 0
        excpxn_df = facdf[(~facdf.int_overdue_amt.isnull() & ~facdf.int_overdue_days.isnull())]
        xcpxn_desc = 'interest overdue amount and days values are inconsistent'
        if not excpxn_df.empty:
            # log exceptions for dp, cycle_ver, cust_id, account_no, fields, exception
            df = excpxn_df[['dp_name', 'cust_id', 'account_no', 'overdue_amt', 'overdue_days']] if btch['in_mod'] in (
                'cdt',) else excpxn_df[['dp_name', 'account_no', 'overdue_amt', 'overdue_days']]
            df_dict = df.to_dict('records')
            for d in df_dict:
                rec = dict_dotter(d)
                if pssbl_number(rec):
                    if (int(rec.int_overdue_amt) == 0 and int(rec.int_overdue_days) > 0) or (
                                    int(rec.int_overdue_amt) > 0 and int(rec.int_overdue_days) == 0):
                        dd = {k: str(v) for k, v in d.items()}
                        dd['level'] = 4
                        db_log_xcpxns(btch, dd, overdue_fields, rec, xcpxn_desc)
    except Exception as e:
        mdjlog.error(e)


def db_log_xcpxns(btch, dd, rec, xcpxn_desc):
    mdjlog = get_logger(btch['dp_name'])
    try:
        sd = {'dp_name': rec.dp_name, 'cust_id': rec.cust_id, 'account_no': rec.account_no,
              'cycle_ver': btch['cycle_ver'], 'exception': xcpxn_desc, } if btch['in_mod'] in ('cdt',) else {
            'dp_name': rec.dp_name, 'account_no': rec.account_no, 'cycle_ver': btch['cycle_ver'],
            'exception': xcpxn_desc, }

        pym_db['exceptions'].update(sd, {'$set': dd}, upsert=True)
    except Exception as e:
        mdjlog.error(e)

