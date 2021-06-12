import IoCEngine.SHU.catalogues as ctlg
from IoCEngine.SHU.amounts import is_any_real_no, normal_numbers, round_numbers
from IoCEngine.SHU.d8s import to_date, transform_date
from IoCEngine.SHU.numbers import check_days
from IoCEngine.commons import get_logger, right_now

from IoCEngine.utils.data_modes import amnt_fields, date_fields

acct_stat_dict = ctlg.acct_stat_ctlg()
asset_class_dict = ctlg.asset_class_ctlg()
comm_brw_typ_dict = ctlg.comm_brw_typ_ctlg()
cons_brw_typ_dict = ctlg.cons_brw_typ_ctlg()
consent_stat_dict = ctlg.cnsnt_stat_ctlg()
country_dict = ctlg.country_ctlg()
currency_dict = ctlg.currency_ctlg()
employ_stat_dict = ctlg.employ_stat_ctlg()
gender_dict = ctlg.gender_ctlg()
grntee_cov = ctlg.guarantee_cov()
loan_typ_dict = ctlg.loan_typ_ctlg()
legal_stat_dict = ctlg.legal_stat_ctlg()
mrtl_stat_dict = ctlg.marital_stat_ctlg()
occpaxn_dict = ctlg.occpaxn_ctlg()
repay_freq_dict = ctlg.repay_freq_ctlg()
state_dict = ctlg.state_ctlg()
secure_stat_dict = ctlg.sec_stat_ctlg()
biz_sect_dict = ctlg.biz_sect_ctlg()
legal_const_dict = ctlg.legal_const_ctlg()
phed_biz_units = ctlg.disco_biz_units()

sme_dict = {'Y': '001', 'N': '002'}


def normalize_amts(file_dict, cxcf_data):
    log_msg, mdjlog = '', get_logger(file_dict['dp_name'])

    for fld in amnt_fields():
        if fld in cxcf_data:
            try:
                cxcf_data[fld] = cxcf_data[fld].apply(normal_numbers)
            except Exception as e:
                mdjlog.warn(f'{fld=}\t|\t{e}')
    
    # try:
    #     log_msg += ' approved_amt |'
    #     # cxcf_data['approved_amt'] = cxcf_data.approved_amt.apply(normal_numbers)
    #     cxcf_data['approved_amt'] = cxcf_data.approved_amt.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' disbursed_amt |'
    #     cxcf_data['disbursed_amt'] = cxcf_data.disbursed_amt.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' int_overdue_amt |'
    #     cxcf_data['int_overdue_amt'] = cxcf_data.int_overdue_amt.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' outstanding_amt |'
    #     cxcf_data['outstanding_amt'] = cxcf_data.outstanding_amt.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' instal_amt |'
    #     cxcf_data['instal_amt'] = cxcf_data.instal_amt.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' int_last_paid_amt |'
    #     cxcf_data['int_last_paid_amt'] = cxcf_data.int_last_paid_amt.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' last_paid_amt |'
    #     cxcf_data['last_paid_amt'] = cxcf_data.last_paid_amt.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' overdue_amt |'
    #     cxcf_data['overdue_amt'] = cxcf_data.overdue_amt.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    
    try:
        log_msg = ' overdue_days |'
        cxcf_data['overdue_days'] = cxcf_data.overdue_days.apply(normal_numbers)
        # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    except Exception as e:
        mdjlog.warn('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    
    # try:
    #     if 'wrttn_off_amt' in cxcf_data:
    #         log_msg += ' wrttn_off_amt |'
    #         cxcf_data['wrttn_off_amt'] = cxcf_data.wrttn_off_amt.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     if 'disbursed_amt2date' in cxcf_data:
    #         log_msg += ' disbursed_amt2date |'
    #         cxcf_data['disbursed_amt2date'] = cxcf_data.disbursed_amt2date.apply(normal_numbers)
    #     # lambda x: normal_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    
    if file_dict['in_mod'] in ('cdt',) and file_dict['out_mod'] not in ('mfi',):
        cxcf_data['int_overdue_amt'] = 0.0
        cxcf_data['int_overdue_days'] = 0.0
        mdjlog.info('fix3D int_overdue_amt & int_overdue_days fields not provided in CDT Data File Submissions')
    #
    return cxcf_data


def fac_vals(file_dict, cxcf_data):
    mdjlog = get_logger(file_dict['dp_name'])
    mdjlog.info(cxcf_data.shape)
    log_msg = ''
    
    if cxcf_data is not None and not cxcf_data.empty:
        mdjlog.info("Facility transformation began @ {} with {}".format(right_now(), cxcf_data.shape))
        # count_down(None, 5)
        
        cxcf_data = trnsfrm_fac_d8s(file_dict, cxcf_data)
        
        try:
            cxcf_data['cust_id'] = cxcf_data.cust_id.apply(
                lambda x: str(x).replace('.0', '') if str(x).endswith('.0') else str(x))
        except Exception as e:
            pass
        
        try:
            cxcf_data['account_no'] = cxcf_data.account_no.apply(
                lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
        except Exception as e:
            pass
        
        if file_dict['in_mod'] == 'phed':
            try:
                log_msg = ' branch_code |'
                cxcf_data['branch_code'] = cxcf_data.branch_code.apply(lambda x: phed_biz_units.get(x.strip(), ''))
            except Exception as e:
                mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
            try:
                log_msg = ' prev_branch_id |'
                cxcf_data['prev_branch_id'] = cxcf_data.branch_code.apply(lambda x: phed_biz_units.get(x.strip(), ''))
            except Exception as e:
                mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
            
            try:
                log_msg = ' prev_acct_no |'
                cxcf_data['prev_acct_no'] = ''
            except Exception as e:
                mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' acct_stat |'
            cxcf_data['acct_stat'] = cxcf_data.acct_stat.apply(lambda x: acct_stat_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' currency |'
            cxcf_data['currency'] = cxcf_data.currency.apply(lambda x: currency_dict.get(str(x).strip().upper(), 'NGN'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        if file_dict['dp_name'] == 'accion':
            cxcf_data.loc[(cxcf_data['overdue_amt'] == 0) & (cxcf_data['overdue_days'] == 0), 'asset_class'] = '001'
            cxcf_data.loc[(cxcf_data['overdue_amt'] > 0) & (cxcf_data['overdue_days'] < 31) & (
                cxcf_data['asset_class'].isnull()), 'asset_class'] = '002'
            cxcf_data.loc[(cxcf_data['overdue_amt'] > 30) & (cxcf_data['overdue_days'] < 61) & (
                cxcf_data['asset_class'].isnull()), 'asset_class'] = '003'
            cxcf_data.loc[(cxcf_data['overdue_amt'] > 60) & (cxcf_data['overdue_days'] < 91) & (
                cxcf_data['asset_class'].isnull()), 'asset_class'] = '004'
            cxcf_data.loc[(cxcf_data['overdue_days'] > 91) & (cxcf_data['asset_class'].isnull()), 'asset_class'] = '005'
        
        try:
            log_msg = ' int_overdue_days |'
            cxcf_data['int_overdue_days'] = cxcf_data.int_overdue_days.apply(check_days)
                # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        try:
            log_msg = ' overdue_days |'
            cxcf_data['overdue_days'] = cxcf_data.overdue_days.apply(check_days)
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' asset_class |'
            cxcf_data['asset_class'] = cxcf_data.asset_class.apply(
                lambda x: asset_class_dict.get(str(x).strip().upper(), ''))
            # cxcf_data['asset_class'] = cxcf_data.asset_class.apply(lambda x: str(x).upper())
            # .apply(lambda x: asset_class_dict.get(x, ''))
            
            if file_dict['in_mod'] == 'phed':
                # cxcf_data['overdue_days'] = cxcf_data.overdue_days.apply(check_days)
                cxcf_data['overdue_amt'] = cxcf_data['overdue_amt'].apply(lambda x: 0).where(
                    ((cxcf_data['asset_class'] == '001') & (cxcf_data['overdue_days'] == 0)), cxcf_data['overdue_amt'])
                # below should also work
                # cxcf_data.loc[
                #     (cxcf_data['asset_class'] == '001') & (cxcf_data['overdue_days'] == 0), 'overdue_amt'] = 0
        
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' consent_stat |'
            # cxcf_data.consent_stat = cxcf_data.consent_stat
            # .apply(lambda x: str(x).upper()).replace(consent_stat_dict, inplace=True)
            cxcf_data['consent_stat'] = cxcf_data.consent_stat.apply(
                lambda x: consent_stat_dict.get(str(x).strip().upper(), '004'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
            if 'consent' not in cxcf_data:
                cxcf_data['consent_stat'] = '004'
        
        try:
            log_msg = ' facility_type |'
            if file_dict['in_mod'] == 'phed':
                cxcf_data['facility_type'] = '032'
            else:
                loan_typ_dict = ctlg.loan_typ_ctlg(file_dict['out_mod']) if file_dict[
                    'out_mod'] else ctlg.loan_typ_ctlg()
                cxcf_data['facility_type'] = cxcf_data.facility_type.apply(
                    lambda x: loan_typ_dict.get(str(x).strip().upper(), '999'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' facility_purpose |'
            if file_dict['in_mod'] == 'phed':
                cxcf_data.loc[cxcf_data.facility_purpose.isnull(), 'facility_purpose'] = '999'
                # cxcf_data.facility_purpose.apply(round_numbers)
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' repay_freq |'
            cxcf_data['repay_freq'] = cxcf_data.repay_freq.apply(
                lambda x: repay_freq_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' secure_stat |'
            cxcf_data['secure_stat'] = cxcf_data.secure_stat.apply(
                lambda x: secure_stat_dict.get(str(x).strip().upper(), '003'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
            cxcf_data['secure_stat'] = '003'
        
        try:
            log_msg = ' ownership |'
            cxcf_data['ownership'] = '001'
        except Exception as e:
            mdjlog.warn('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' grnt_cov |'
            if 'grnt_cov' in cxcf_data:
                cxcf_data['grnt_cov'] = cxcf_data.grnt_cov.apply(lambda x: str(x).strip()).apply(
                    lambda x: grntee_cov.get(x.upper(), '002'))
            else:
                cxcf_data['grnt_cov'] = '002'
        except Exception as e:
            mdjlog.error('log_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' legal_stat |'
            cxcf_data['legal_stat'] = '002'
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' trxn_typ_cod |'
            cxcf_data['trxn_typ_cod'] = '001'
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        cxcf_data = trnsfm_amts(file_dict, cxcf_data)
        
        mdjlog.info(f"Facility Data shape: {cxcf_data.shape=}")
        # count_down(None, 5)
        return cxcf_data


def trnsfm_amts(file_dict, cxcf_data):
    mdjlog = get_logger(file_dict['dp_name'])
    for fld in amnt_fields():
        if fld in cxcf_data:
            try:
                cxcf_data[fld] = cxcf_data[fld].apply(round_numbers)
            except Exception as e:
                mdjlog.warn(f'{fld=}\t|\t{e=}')
    
    # try:
    #     log_msg += ' approved_amt |'
    #     # cxcf_data['approved_amt'] = cxcf_data.approved_amt.apply(round_numbers)
    #     cxcf_data['approved_amt'] = cxcf_data.approved_amt.apply(round_numbers)
    #     # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' disbursed_amt |'
    #     cxcf_data['disbursed_amt'] = cxcf_data.disbursed_amt.apply(round_numbers)
    #     # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' int_overdue_amt |'
    #     cxcf_data['int_overdue_amt'] = cxcf_data.int_overdue_amt.apply(round_numbers)
    #     # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' outstanding_amt |'
    #     cxcf_data['outstanding_amt'] = cxcf_data.outstanding_amt.apply(round_numbers)
    #     # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' instal_amt |'
    #     cxcf_data['instal_amt'] = cxcf_data.instal_amt.apply(round_numbers)
    #     # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' int_last_paid_amt |'
    #     cxcf_data['int_last_paid_amt'] = cxcf_data.int_last_paid_amt.apply(round_numbers)
    #     # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' last_paid_amt |'
    #     cxcf_data['last_paid_amt'] = cxcf_data.last_paid_amt.apply(round_numbers)
    #     # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' overdue_amt |'
    #     cxcf_data['overdue_amt'] = cxcf_data.overdue_amt.apply(round_numbers)
    #     # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' wrttn_off_amt |'
    #     cxcf_data['wrttn_off_amt'] = cxcf_data.wrttn_off_amt.apply(round_numbers)
    #     # lambda x: round_numbers(x) if is_any_real_no(x) else 0)
    # except Exception as e:
    #     mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    
    if file_dict['in_mod'] in ('cdt', 'udf',) and file_dict['out_mod'] not in ('mfi',):
        cxcf_data['int_overdue_amt'] = 0
        cxcf_data['int_overdue_days'] = 0
        mdjlog.info('fix3D int_overdue_amt & int_overdue_days fields not provided in CDT Data File Submissions')
    #
    return cxcf_data


def trnsfrm_fac_d8s(file_dict, cxcf_data):
    mdjlog = get_logger(file_dict['dp_name'])
    for fld in date_fields():
        if fld in cxcf_data:
            nrmlz_d8fld(cxcf_data, fld, mdjlog)

    # try:
    #     log_msg += ' maturity_date |'
    #     cxcf_data['maturity_date'] = cxcf_data.maturity_date.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' litigxn_date |'
    #     cxcf_data['litigxn_date'] = cxcf_data.litigxn_date.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' int_last_paid_date |'
    #     cxcf_data['int_last_paid_date'] = cxcf_data.int_last_paid_date.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' last_paid_date |'
    #     cxcf_data['last_paid_date'] = cxcf_data.last_paid_date.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' acct_clsd_date |'
    #     cxcf_data['acct_clsd_date'] = cxcf_data.acct_clsd_date.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' d8_acct_stat |'
    #     cxcf_data['d8_acct_stat'] = cxcf_data.d8_acct_stat.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' d8_disbursed |'
    #     cxcf_data['d8_disbursed'] = cxcf_data.d8_disbursed.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' consent_d8from |'
    #     cxcf_data['consent_d8from'] = cxcf_data.consent_d8from.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' consent_d8to |'
    #     cxcf_data['consent_d8to'] = cxcf_data.consent_d8to.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' last_paid_date |'
    #     cxcf_data['last_paid_date'] = cxcf_data.last_paid_date.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' d8_approved |'
    #     # cxcf_data.d8_approved = cxcf_data.d8_approved.apply(transform_date)
    #     cxcf_data['d8_approved'] = cxcf_data.d8_approved.apply(transform_date).where((
    #         cxcf_data['d8_approved'].notnull()), cxcf_data.d8_approved.apply(transform_date))
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' amend_date |'
    #     cxcf_data['amend_date'] = cxcf_data.amend_date.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #

    return cxcf_data


def trnsfrm_corp_d8s(file_dict, cmcs_data):
    mdjlog = get_logger(file_dict['dp_name'])

    for fld in date_fields():
        if fld in cmcs_data:
            nrmlz_d8fld(cmcs_data, fld, mdjlog)

    # try:
    #     log_msg += ' incorp_date |'
    #     cmcs_data['incorp_date'] = cmcs_data.incorp_date.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' id_code1d8xpry |'
    #     cmcs_data['id_code1d8xpry'] = cmcs_data.id_code1d8xpry.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' id_code2d8xpry |'
    #     cmcs_data['id_code2d8xpry'] = cmcs_data.id_code2d8xpry.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #

    return cmcs_data


def trnsfrm_ndvdl_d8s(file_dict, cncs_data):
    mdjlog = get_logger(file_dict['dp_name'])

    for fld in date_fields():
        if fld in cncs_data:
            nrmlz_d8fld(cncs_data, fld, mdjlog)

    # try:
    #     log_msg += ' birth_date |'
    #     cncs_data['birth_date'] = cncs_data.birth_date.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' i_pass_expiry |'
    #     cncs_data['i_pass_expiry'] = cncs_data.i_pass_expiry.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' bvn_d8xpry |'
    #     cncs_data['bvn_d8xpry'] = cncs_data.bvn_d8xpry.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' biz_d8reg |'
    #     cncs_data['biz_d8reg'] = cncs_data.biz_d8reg.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #
    # try:
    #     log_msg += ' id_code2d8xpry |'
    #     cncs_data['id_code2d8xpry'] = cncs_data.id_code2d8xpry.apply(transform_date)
    # except Exception as e:
    #     mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
    #

    return cncs_data


def corp_vals(file_dict, cmcs_data):
    mdjlog = get_logger(file_dict['dp_name'])
    log_msg = ''
    if cmcs_data is not None and not cmcs_data.empty:
        mdjlog.info(f"Corporate Subject transformation began @ {right_now()=} with {cmcs_data.shape=}")
        # count_down(None, 5)
        
        cmcs_data = trnsfrm_corp_d8s(file_dict, cmcs_data)
        
        try:
            cmcs_data['cust_id'] = cmcs_data.cust_id.apply(
                lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
        except Exception as e:
            pass
        
        try:
            cmcs_data['biz_reg_no'] = cmcs_data.biz_reg_no.apply(
                lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
        except Exception as e:
            pass
        
        try:
            cmcs_data['account_no'] = cmcs_data.account_no.apply(
                lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
        except Exception as e:
            pass
        
        if file_dict['in_mod'] == 'phed':
            try:
                cmcs_data['branch_code'] = cmcs_data.branch_code.apply(
                    lambda x: phed_biz_units.get(x.strip(), ''))  # phed_biz_units[x]
            except Exception as e:
                mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
            
            try:
                log_msg = ' cust_id |'
                cmcs_data['cust_id'] = cmcs_data.account_no.where(
                    (cmcs_data.cust_id.str.strip() == 'NOMETER'), cmcs_data.cust_id)
            except Exception as e:
                mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
            
            try:
                cmcs_data['prev_branch_id'] = cmcs_data.branch_code.apply(
                    lambda x: phed_biz_units.get(x.strip(), ''))
            except Exception as e:
                mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' phone_no |'
            cmcs_data['phone_no'] = cmcs_data.phone_no.apply(
                lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
            # .apply(lambda x: str(int(x)) if x else '')
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' biz_corp_type |'
            cmcs_data['biz_corp_type'] = cmcs_data.biz_corp_type.apply(
                lambda x: legal_const_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' biz_category |'
            cmcs_data['biz_category'] = cmcs_data.biz_category.apply(lambda x: str(x).strip().upper()).apply(
                lambda x: biz_sect_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' is_sme |'
            cmcs_data['is_sme'] = cmcs_data.is_sme.apply(lambda x: sme_dict.get(str(x).upper(), '002'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' pri_addr_typ |'
            cmcs_data.loc[cmcs_data.pri_addr_line1.str.len() > 0, 'pri_addr_typ'] = '001'
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' pri_addr_state |'
            cmcs_data['pri_addr_state'] = cmcs_data.pri_addr_state.apply(
                lambda x: state_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' pri_addr_country |'
            cmcs_data['pri_addr_country'] = cmcs_data.pri_addr_country.apply(
                lambda x: country_dict.get(str(x).strip().upper(), 'NG'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' sec_addr_typ |'
            cmcs_data.loc[cmcs_data.sec_addr_line1.notnull(), 'sec_addr_typ'] = '002'
            cmcs_data.loc[~cmcs_data.sec_addr_line1.notnull(), 'sec_addr_typ'] = ''
            # cmcs_data['sec_addr_typ'] = '002'.where((cmcs_data.sec_addr_line1.str.len() > 0), '')
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' sec_addr_state |'
            cmcs_data['sec_addr_state'] = cmcs_data.sec_addr_state.apply(
                lambda x: state_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' sec_addr_country |'
            cmcs_data['sec_addr_country'] = cmcs_data.sec_addr_country.apply(
                lambda x: country_dict.get(str(x).strip().upper(), 'NG'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        mdjlog.info("Corporate Data shape: {}".format(cmcs_data.shape))
        # count_down(None, 5)
        return cmcs_data


def ndvdl_vals(file_dict, cncs_data):
    mdjlog = get_logger(file_dict['dp_name'])
    log_msg = ''
    if cncs_data is not None and not cncs_data.empty:
        mdjlog.info("Individual Subject transformation began @ {} with {}".format(right_now(), cncs_data.shape))
        # count_down(None, 5)
        
        cncs_data = trnsfrm_ndvdl_d8s(file_dict, cncs_data)
        
        # if str(new_cons_sbjt_rw['first_name']).strip() == '' and str(new_cons_sbjt_rw['last_name']).strip() != '':
        #     new_cons_sbjt_rw['full_name'] = str(new_cons_sbjt_rw['last_name']).strip()
        #     new_cons_sbjt_rw['last_name'] = ''
        cncs_data.fillna('', inplace=True)
        # try:
        #     if 'last_name' in cncs_data:
        #         cncs_data['last_name'] = cncs_data.last_name.apply(lambda x: str(x).strip())
        # except Exception as e:
        #     mdjlog.warn(e)
        # try:
        #     if 'first_name' in cncs_data:
        #         cncs_data['first_name'] = cncs_data.first_name.apply(lambda x: str(x).strip())
        # except Exception as e:
        #     mdjlog.warn(e)
        # if ('first_name' not in cncs_data or cncs_data.first_name.empty) and 'last_name' in cncs_data and 'full_name' not in cncs_data:
        if file_dict['dp_name'] == 'unity':
            try:
                cncs_data['full_name'] = cncs_data.last_name.where(
                	(cncs_data.last_name.str.strip() != '') & (cncs_data.first_name.str.strip() == ''))
                cncs_data['e_str'] = ''
                cncs_data['last_name'] = cncs_data.e_str.where(
                	(cncs_data.last_name.str.strip() != '') & (cncs_data.first_name.str.strip() == ''))
                del cncs_data['e_str']
            except Exception as e:
                mdjlog.error(e)
                cncs_data['full_name'] = cncs_data.last_name  # .where( (cncs_data.last_name.str.strip() != '') )
                # cncs_data['e_str'] = ''
                cncs_data['last_name'] = ''
                # del cncs_data['e_str']

        # cncs_data.loc[(cncs_data['first_name'] == '') & (~cncs_data['last_name'] == ''),
        #               'full_name'] = cncs_data['last_name']
        # cncs_data.loc[(~cncs_data['full_name'] == ''), 'last_name'] = ''
        
        try:
            cncs_data['cust_id'] = cncs_data.cust_id.apply(
                lambda x: str(x).replace('.0', '') if str(x).endswith('.0') else str(x))
        except Exception as e:
            pass
        
        try:
            cncs_data['account_no'] = cncs_data.account_no.apply(
                lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
        except Exception as e:
            pass
        
        try:
            log_msg = ' bvn |'
            cncs_data['bvn'] = cncs_data.bvn.apply(lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' work_phone |'
            cncs_data['work_phone'] = cncs_data.work_phone.apply(
                lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' home_phone |'
            cncs_data['home_phone'] = cncs_data.home_phone.apply(
                lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' mobile_no |'
            cncs_data['mobile_no'] = cncs_data.mobile_no.apply(
                lambda x: str(x).replace('.0', '') if '.0' in str(x) else str(x))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' nationality |'
            cncs_data['nationality'] = cncs_data.nationality.apply(
                lambda x: country_dict.get(str(x).strip().upper(), 'NG'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' gender |'
            cncs_data['gender'] = cncs_data.gender.apply(lambda x: str(x).upper()).apply(
                lambda x: gender_dict.get(x.strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' mrtl_stat |'
            cncs_data['mrtl_stat'] = cncs_data.mrtl_stat.apply(lambda x: mrtl_stat_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' ownership |'
            cncs_data['ownership'] = '001'  # cncs_data.ownership.apply(lambda x: str(x).upper()).apply(
            # lambda x: cons_brw_typ_dict.get(x, ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' employ_stat |'
            cncs_data['employ_stat'] = cncs_data.employ_stat.apply(
                lambda x: employ_stat_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' occpaxn |'
            cncs_data['occpaxn'] = cncs_data.occpaxn.apply(lambda x: occpaxn_dict.get(str(x).strip().upper(), '999'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' biz_category |'
            cncs_data['biz_category'] = cncs_data.biz_category.apply(
                lambda x: biz_sect_dict.get(str(x).strip().upper(), '999'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' biz_sector |'
            cncs_data['biz_sector'] = cncs_data.biz_sector.apply(
                lambda x: biz_sect_dict.get(str(x).strip().upper(), '999'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' pri_addr_state |'
            cncs_data['pri_addr_state'] = cncs_data.pri_addr_state.apply(
                lambda x: state_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' pri_addr_typ |'
            cncs_data.loc[cncs_data.pri_addr_line1.str.len() > 0, 'pri_addr_typ'] = '001'
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' pri_addr_country |'
            cncs_data['pri_addr_country'] = cncs_data.pri_addr_country.apply(
                lambda x: country_dict.get(str(x).strip().upper(), 'NG'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' sec_addr_typ |'
            cncs_data.loc[cncs_data.sec_addr_line1.notnull(), 'sec_addr_typ'] = '002'
            cncs_data.loc[~cncs_data.sec_addr_line1.notnull(), 'sec_addr_typ'] = ''
            # cncs_data['sec_addr_typ'] = '002'.where((cncs_data.sec_addr_line1.str.len() > 0), '')
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' sec_addr_state |'
            cncs_data['sec_addr_state'] = cncs_data.sec_addr_state.apply(
                lambda x: state_dict.get(str(x).strip().upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' sec_addr_country |'
            cncs_data['sec_addr_country'] = cncs_data.sec_addr_country.apply(
                lambda x: country_dict.get(str(x).upper(), 'NG'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' employr_addr_state |'
            cncs_data['employr_addr_state'] = cncs_data.employr_addr_state.apply(
                lambda x: state_dict.get(str(x).upper(), ''))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        try:
            log_msg = ' employr_addr_country |'
            cncs_data['employr_addr_country'] = cncs_data.employr_addr_country.apply(
                lambda x: country_dict.get(str(x).upper(), 'NG'))
        except Exception as e:
            mdjlog.error('\nlog_msg: {}\n\nerror:\t{}\n\n'.format(log_msg, e))
        
        mdjlog.info("Individual Data shape: {}".format(cncs_data.shape))
        # count_down(None, 5)
        return cncs_data


def fields2date(file_dict, df):
    mdjlog = get_logger(file_dict['dp_name'])

    for fld in date_fields():
        if fld in df:
            nrmlz_d8fld(df, fld, mdjlog)
    
    # if 'litigxn_date' in df:
    #     try:
    #         log_msg += ' litigxn_date |'
    #         df['litigxn_date'] = df.litigxn_date.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'int_last_paid_date' in df:
    #     try:
    #         log_msg += ' int_last_paid_date |'
    #         df['int_last_paid_date'] = df.int_last_paid_date.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'last_paid_date' in df:
    #     try:
    #         log_msg += ' last_paid_date |'
    #         df['last_paid_date'] = df.last_paid_date.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'acct_clsd_date' in df:
    #     try:
    #         log_msg += ' acct_clsd_date |'
    #         df['acct_clsd_date'] = df.acct_clsd_date.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'd8_acct_stat' in df:
    #     try:
    #         log_msg += ' d8_acct_stat |'
    #         df['d8_acct_stat'] = df.d8_acct_stat.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'd8_disbursed' in df:
    #     try:
    #         log_msg += ' d8_disbursed |'
    #         df['d8_disbursed'] = df.d8_disbursed.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'consent_d8from' in df:
    #     try:
    #         log_msg += ' consent_d8from |'
    #         df['consent_d8from'] = df.consent_d8from.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'consent_d8to' in df:
    #     try:
    #         log_msg += ' consent_d8to |'
    #         df['consent_d8to'] = df.consent_d8to.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'd8_approved' in df:
    #     try:
    #         log_msg += ' d8_approved |'
    #         # df.d8_approved = df.d8_approved.apply( to_date)
    #         df['d8_approved'] = df.d8_approved.apply(to_date).where((
    #             df['d8_approved'].notnull()), df.d8_approved.apply(to_date))
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'amend_date' in df:
    #     try:
    #         log_msg += ' amend_date |'
    #         df['amend_date'] = df.amend_date.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'incorp_date' in df:
    #     try:
    #         log_msg += ' incorp_date |'
    #         df['incorp_date'] = df.incorp_date.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'id_code1d8xpry' in df:
    #     try:
    #         log_msg += ' id_code1d8xpry |'
    #         df['id_code1d8xpry'] = df.id_code1d8xpry.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'id_code2d8xpry' in df:
    #     try:
    #         log_msg += ' id_code2d8xpry |'
    #         df['id_code2d8xpry'] = df.id_code2d8xpry.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'birth_date' in df:
    #     try:
    #         log_msg += ' birth_date |'
    #         df['birth_date'] = df.birth_date.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'i_pass_expiry' in df:
    #     try:
    #         log_msg += ' i_pass_expiry |'
    #         df['i_pass_expiry'] = df.i_pass_expiry.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'bvn_d8xpry' in df:
    #     try:
    #         log_msg += ' bvn_d8xpry |'
    #         df['bvn_d8xpry'] = df.bvn_d8xpry.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    #
    # if 'biz_d8reg' in df:
    #     try:
    #         log_msg += ' biz_d8reg |'
    #         df['biz_d8reg'] = df.biz_d8reg.apply(to_date)
    #     except Exception as e:
    #         mdjlog.warning('\nlog_msg: {}\n\nwarning:\t{}\n\n'.format(log_msg, e))
    
    return df


def nrmlz_d8fld(df, fld, mdjlog):
    try:
        df[fld] = df[fld].apply(to_date)
    except Exception as e:
        mdjlog.warn(f'{fld=}\t|\t{e=}')
