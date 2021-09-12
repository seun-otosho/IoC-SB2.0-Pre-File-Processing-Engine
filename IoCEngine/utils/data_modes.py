# coding: utf-8


def cdt() -> dict:
    d = {
        'fac': (
            'cust_id', 'account_no', 'acct_stat', 'd8_acct_stat', 'd8_disbursed', 'approved_amt', 'disbursed_amt',
            'outstanding_bal', 'instal_amt', 'currency', 'overdue_days', 'overdue_amt', 'facility_type', 'tenor',
            'repay_freq', 'last_paid_date', 'last_paid_amt', 'maturity_date', 'asset_class', 'legal_stat',
            'litigxn_date', 'consent_stat', 'secure_stat', 'collateral_type', 'collateral_details',
            'prev_acct_no', 'prev_name', 'prev_cust_id', 'prev_branch_id'
        ),
        # local common data format for commercial subject
        'comm': (
            'biz_reg_no', 'biz_name', 'biz_corp_type', 'biz_category', 'incorp_date', 'cust_id', 'branch_code',
            'pri_addr_line1', 'pri_addr_line2', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_country',
            'e_mail_addr', 'sec_addr_line1', 'sec_addr_line2', 'sec_addr_city_lga', 'sec_addr_state',
            'sec_addr_country', 'tax_id', 'phone_no'
        ),
        # local common data format for consumer subject
        'cons': (
            'cust_id', 'branch_code', 'last_name', 'first_name', 'middle_name', 'birth_date', 'national_id_no',
            'drivin_license_no', 'bvn', 'i_pass_no', 'gender', 'nationality', 'mrtl_stat', 'mobile_no',
            'pri_addr_line1', 'pri_addr_line2', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_country',
            'employ_stat', 'occpaxn', 'biz_category', 'biz_sector', 'ownership', 'other_id', 'tax_id', 'pic_file_path',
            'e_mail_addr', 'employr_name', 'employr_addr_line1', 'employr_addr_line2', 'employr_addr_city',
            'employr_addr_state', 'employr_addr_country', 'title', 'birth_place', 'work_phone', 'home_phone',
            'sec_addr_line1', 'sec_addr_line2', 'sec_addr_city_lga', 'sec_addr_state', 'sec_addr_country',
            'spouse_surname', 'spouse_first_name', 'spouse_middle_name'
        ),
        'grntr': (
            'account_no', 'loan_stat', 'grntr_type',
            'biz_name', 'biz_reg_no',
            'last_name', 'first_name', 'middle_name', 'birth_incorp_date', 'gender',
            'national_id_no', 'i_pass_no', 'drivin_license_no', 'bvn', 'other_id',
            'pri_addr_line1', 'pri_addr_line2', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_country',
            'phone_no', 'e_mail_addr',
        ),
        'prnc': (
            'cust_id',

            'last_name', 'first_name', 'middle_name', 'birth_date', 'gender',
            'pri_addr_line1', 'pri_addr_line2', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_country',
            'national_id_no', 'drivin_license_no', 'bvn', 'i_pass_no',
            'phone_no', 'e_mail_addr',
            'psxn_in_biz',

            'last_name1', 'first_name1', 'middle_name1', 'birth_date1', 'gender1',
            'pri_addr_line11', 'pri_addr_line21', 'pri_addr_city_lga1', 'pri_addr_state1', 'pri_addr_country1',
            'national_id_no1', 'drivin_license_no1', 'bvn1', 'i_pass_no1',
            'phone_no1', 'e_mail_addr1',
            'psxn_in_biz1',

            'phone_no2', 'sec_addr_line', 'sec_addr_city_lga', 'sec_addr_state', 'tax_id', 'pic_file_path',
        ),
    }
    d.update({'corp': d['comm'], 'corpfac': d['fac'], 'ndvdl': d['cons'], 'ndvdlfac': d['fac'], })
    return d


def prevs():
    return 'prev_acct_no', 'prev_name', 'prev_cust_id', 'prev_branch_id',


def iff() -> dict:
    d = {
        'cmb': {
            'corp': (
                'branch_code', 'account_no', 'cust_id', 'prev_cust_id', 'biz_reg_no', 'incorp_date', 'prev_biz_reg_no',
                'incorp_cert', 'soc_reg_no', 'tax_id', 'iss_auth1', 'id_code1', 'id_code1d8xpry', 'iss_auth2',
                'id_code2', 'id_code2d8xpry', 'is_sme', 'biz_corp_type', 'biz_name', 'shrt_biz_name',
                'prev_reg_biz_name', 'prev_shrt_biz_name', 'biz_category', 'biz_category2', 'biz_category3',
                'pri_addr_typ', 'pri_addr_line1', 'pri_addr_line2', 'pri_addr_line3', 'pri_addr_city_lga',
                'pri_addr_state', 'pri_addr_post_code', 'pri_addr_country', 'sec_addr_typ', 'sec_addr_line1',
                'sec_addr_line2', 'sec_addr_line3', 'sec_addr_city_lga', 'sec_addr_state', 'sec_addr_post_code',
                'sec_addr_country', 'last_fin_yr_turnover', 'ttl_asset_val', 'no_of_emplye', 'area_code', 'phone_no',
                'fax_no', 'e_mail_addr', 'url'),
            'corpfac': (
                'branch_code', 'prev_branch_id', 'account_no', 'prev_acct_no', 'consent_stat', 'consent_d8from',
                'consent_d8to', 'facility_type', 'facility_purpose', 'ownership', 'disbursed_amt', 'd8_approved',
                'currency', 'int_type', 'int_rate', 'd8_disbursed', 'maturity_date', 'int_repay_freq', 'repay_freq',
                'int_instal_count', 'instal_count', 'int_instal_amt', 'instal_amt', 'highest_crdt_amt',
                'outstanding_bal', 'int_last_paid_amt', 'last_paid_amt', 'int_last_paid_date', 'last_paid_date',
                'int_overdue_days', 'overdue_days', 'int_overdue_amt', 'overdue_amt', 'int_outstanding_amt',
                'outstanding_amt', 'int_outstanding_amt_count', 'outstanding_amt_count', 'asset_class', 'acct_stat',
                'amend_date', 'wrttn_off_amt', 'wrttn_off_rsn', 'acct_clsd_date', 'acct_cls_rsn', 'secure_stat',
                'grnt_cov', 'legal_stat', 'trxn_typ_cod', 'dspt_id'),
            'ndvdl': (
                'branch_code', 'account_no', 'cust_id', 'prev_cust_id', 'nationality', 'national_id_no', 'i_pass_no',
                'i_pass_expiry', 'drivin_license_no', 'tax_id', 'iss_auth1', 'bvn', 'bvn_d8xpry', 'iss_auth2',
                'id_code2', 'id_code2d8xpry', 'last_name', 'first_name', 'middle_name', 'full_name', 'alias',
                'prev_name', 'fathers_name', 'mothers_maiden_name', 'pri_addr_typ', 'pri_addr_line1',
                'pri_addr_line2', 'pri_addr_line3', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_post_code',
                'pri_addr_country', 'sec_addr_typ', 'sec_addr_line1', 'sec_addr_line2', 'sec_addr_line3',
                'sec_addr_city_lga', 'sec_addr_state', 'sec_addr_post_code', 'sec_addr_country', 'employ_stat',
                'occpaxn', 'employr_name', 'employr_addr_line1', 'emply_posxn', 'annl_sal', 'mnths_w_crrnt_emplyr',
                'biz_name', 'biz_reg_no', 'biz_d8reg', 'area_code', 'work_phone', 'mobile_no', 'fax_no', 'e_mail_addr',
                'url', 'birth_date', 'birth_place', 'gender', 'mrtl_stat', 'spouse_surname'),
            'ndvdlfac': (
                'branch_code', 'prev_branch_id', 'account_no', 'prev_acct_no', 'consent_stat', 'consent_d8from',
                'consent_d8to', 'facility_type', 'facility_purpose', 'ownership', 'disbursed_amt', 'd8_approved',
                'currency', 'int_type', 'int_rate', 'd8_disbursed', 'maturity_date', 'int_repay_freq', 'repay_freq',
                'int_instal_count', 'instal_count', 'int_instal_amt', 'instal_amt', 'highest_crdt_amt',
                'outstanding_bal', 'int_last_paid_amt', 'last_paid_amt', 'int_last_paid_date', 'last_paid_date',
                'int_overdue_days', 'overdue_days', 'int_overdue_amt', 'overdue_amt', 'int_outstanding_amt',
                'outstanding_amt', 'int_outstanding_amt_count', 'outstanding_amt_count', 'asset_class',
                'acct_stat', 'amend_date', 'wrttn_off_amt', 'wrttn_off_rsn', 'acct_clsd_date', 'acct_cls_rsn',
                'secure_stat', 'pri_card_no', 'grnt_cov', 'legal_stat', 'trxn_typ_cod', 'dspt_id'),
            'grntr': (
                'branch_code', 'account_no',

                'grntr_cust_id', 'grntr_prev_cust_id', 'grntr_type', 'pri_addr_country',

                'national_id_no', 'i_pass_no', 'i_pass_expiry', 'drivin_license_no', 'tax_id',

                'biz_reg_no', 'incorp_date', 'prev_biz_reg_no', 'incorp_cert', 'soc_reg_no', 'biz_tax_id',

                'iss_auth1', 'bvn', 'id_code1d8xpry', 'iss_auth2', 'id_code2', 'id_code2d8xpry',

                'last_name', 'middle_name', 'first_name', 'full_name', 'prev_name', 'birth_date', 'gender',
                'mrtl_stat',

                'biz_name', 'shrt_biz_name', 'prev_reg_biz_name', 'biz_corp_type', 'biz_category', 'biz_category2',
                'biz_category3',

                'pri_addr_typ', 'pri_addr_line1', 'pri_addr_line2', 'pri_addr_line3', 'pri_addr_city_lga',
                'pri_addr_state', 'pri_addr_post_code', 'pri_addr_country',
                'sec_addr_typ', 'sec_addr_line1', 'sec_addr_line2', 'sec_addr_line3', 'sec_addr_city_lga',
                'sec_addr_state', 'sec_addr_post_code', 'sec_addr_country',

                'area_code', 'phone_no', 'fax_no', 'e_mail_addr', 'url'
            ),
        },
        'mfi': {
            'comm': (
                'branch_code', 'account_no', 'cust_id', 'prev_cust_id', 'reg_biz_name', 'biz_reg_no', 'incorp_date',
                'prev_biz_reg_no',
                'incorp_cert', 'trustee_reg_no', 'tax_id', 'iss_auth1', 'id_code1', 'id_code1d8xpry', 'iss_auth2',
                'id_code2',
                'id_code2d8xpry', 'biz_corp_type', 'biz_name', 'shrt_biz_name', 'prev_reg_biz_name',
                'prev_shrt_biz_name',
                'biz_category', 'biz_category2', 'biz_category3', 'pri_addr_typ', 'pri_addr_line1', 'pri_addr_line2',
                'pri_addr_line3', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_post_code', 'pri_addr_country',
                'sec_addr_typ',
                'sec_addr_line1', 'sec_addr_line2', 'sec_addr_line3', 'sec_addr_city_lga', 'sec_addr_state',
                'sec_addr_post_code',
                'sec_addr_country', 'last_fin_yr_turnover', 'ttl_asset_val', 'no_of_emplye', 'area_code', 'phone_no',
                'fax_no',
                'e_mail_addr', 'url'),
            'fac': (
                'branch_code', 'prev_branch_id', 'account_no', 'prev_acct_no', 'consent_stat', 'consent_d8from',
                'consent_d8to', 'facility_type', 'comments', 'ownership', 'disbursed_amt', 'currency', 'int_rate',
                'd8_disbursed', 'maturity_date', 'repay_freq', 'instal_amt', 'outstanding_bal', 'last_paid_amt',
                'last_paid_date', 'overdue_days', 'overdue_amt', 'asset_class', 'acct_stat', 'd8_amended',
                'written_off_amt', 'wrttn_off_rsn', 'acct_clsd_date', 'acct_cls_rsn', 'trxn_typ_cod', 'dspt_id',
                'grnt_cov', 'secure_stat'),
            'cons': (
                'branch_code', 'account_no', 'cust_id', 'prev_cust_id', 'nationality', 'national_id_no', 'i_pass_no',
                'i_pass_expiry', 'drivin_license_no', 'tax_id', 'iss_auth1', 'bvn', 'bvn_d8xpry', 'iss_auth2',
                'id_code2', 'id_code2d8xpry', 'iss_auth3', 'id_code3', 'id_code3d8xpry', 'iss_auth4', 'id_code4',
                'id_code4d8xpry',
                'last_name',
                'first_name', 'middle_name', 'full_name', 'prev_name', 'mothers_maiden_name', 'pri_addr_typ',
                'pri_addr_line1',
                'pri_addr_line2', 'pri_addr_line3', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_post_code',
                'pri_addr_country',
                'sec_addr_typ', 'sec_addr_line1', 'sec_addr_line2', 'sec_addr_line3', 'sec_addr_city_lga',
                'sec_addr_state',
                'sec_addr_post_code', 'sec_addr_country', 'biz_name', 'biz_reg_no', 'biz_d8reg', 'biz_category',
                'market_association_name', 'market_association_no', 'market_association_addr', 'market_leader_name',
                'monthly_income', 'total_productive_assets', 'area_code', 'phone_no', 'mobile_no', 'e_mail_addr', 'url',
                'birth_date', 'birth_place', 'gender'),
            'grntr': (
                'branch_code', 'account_no',

                'grntr_cust_id', 'grntr_prev_cust_id', 'grntr_type', 'pri_addr_country',

                'national_id_no', 'i_pass_no', 'i_pass_expiry', 'drivin_license_no', 'tax_id',

                # 'iss_auth1', 'bvn', 'id_code1d8xpry', 'iss_auth2', 'id_code2', 'id_code2d8xpry',

                'iss_auth1', 'bvn', 'id_code1d8xpry', 'iss_auth2', 'id_code2', 'id_code2d8xpry',
                'iss_auth3', 'bvn', 'id_code3d8xpry', 'iss_auth4', 'id_code4', 'id_code4d8xpry',

                'biz_reg_no', 'incorp_date', 'prev_biz_reg_no', 'incorp_cert', 'soc_reg_no', 'biz_tax_id',

                'last_name', 'middle_name', 'first_name', 'full_name', 'prev_name', 'birth_incorp_date', 'gender',
                'mrtl_stat',

                'biz_name', 'shrt_biz_name', 'prev_reg_biz_name', 'biz_corp_type', 'market_association_name',
                'market_association_no', 'biz_category', 'biz_category2', 'biz_category3',

                'pri_addr_typ', 'pri_addr_line1', 'pri_addr_line2', 'pri_addr_line3', 'pri_addr_city_lga',
                'pri_addr_state', 'pri_addr_post_code', 'pri_addr_country',
                'sec_addr_typ', 'sec_addr_line1', 'sec_addr_line2', 'sec_addr_line3', 'sec_addr_city_lga',
                'sec_addr_state', 'sec_addr_post_code', 'sec_addr_country',

                'area_code', 'phone_no', 'fax_no', 'e_mail_addr', 'url'),
        },
        'pmi': {
            'comm': (
                'branch_code', 'account_no', 'cust_id', 'prev_cust_id', 'biz_reg_no', 'incorp_date', 'prev_biz_reg_no',
                'incorp_cert', 'trustee_reg_no', 'tax_id', 'iss_auth1', 'id_code1', 'id_code1d8xpry', 'iss_auth2',
                'id_code2',
                'id_code2d8xpry', 'is_sme', 'biz_corp_type', 'biz_name', 'shrt_biz_name', 'prev_reg_biz_name',
                'prev_shrt_biz_name',
                'biz_category', 'biz_category2', 'biz_category3', 'pri_addr_typ', 'pri_addr_line1', 'pri_addr_line2',
                'pri_addr_line3', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_post_code', 'pri_addr_country',
                'sec_addr_typ',
                'sec_addr_line1', 'sec_addr_line2', 'sec_addr_line3', 'sec_addr_city_lga', 'sec_addr_state',
                'sec_addr_post_code',
                'sec_addr_country', 'last_fin_yr_turnover', 'ttl_asset_val', 'no_of_emplye', 'area_code', 'phone_no',
                'fax_no', 'e_mail_addr', 'url',
            ),
            'fac': (
                'branch_code', 'prev_branch_id', 'account_no', 'prev_acct_no', 'consent_stat', 'consent_d8from',
                'consent_d8to',
                'facility_type', 'facility_purpose', 'ownership', 'disbursed_amt', 'equity_amt', 'd8_approved',
                'currency',
                'int_type', 'int_rate', 'd8_disbursed', 'disbursed_amt2date', 'maturity_date', 'int_repay_freq',
                'repay_freq',
                'int_instal_amt', 'instal_amt', 'highest_crdt_amt', 'outstanding_bal', 'int_last_paid_amt',
                'last_paid_amt',
                'int_last_paid_date', 'last_paid_date', 'int_overdue_days', 'overdue_days', 'int_overdue_amt',
                'overdue_amt',
                'int_outstanding_amt', 'outstanding_amt', 'int_outstanding_amt_count', 'outstanding_amt_count',
                'asset_class',
                'acct_stat', 'd8_amended', 'written_off_amt', 'wrttn_off_rsn', 'acct_clsd_date', 'acct_cls_rsn',
                'penalty',
                'secure_stat', 'grnt_cov', 'legal_stat', 'trxn_typ_cod', 'dspt_id',
            ),
        },
        'prnc': (
            'branch_code', 'account_no', 'cust_id', 'prev_cust_id',
            'prnc_cust_id', 'prnc_prev_cust_id', 'prnc_type',

            'biz_corp_type', 'psxn_in_biz',

            'pri_addr_country', 'national_id_no', 'i_pass_no', 'i_pass_expiry', 'drivin_license_no', 'tax_id',

            'biz_reg_no', 'incorp_date', 'prev_biz_reg_no', 'incorp_cert', 'soc_reg_no', 'biz_tax_id',

            'iss_auth1', 'bvn', 'id_code1d8xpry', 'iss_auth2', 'id_code2', 'id_code2d8xpry',

            'last_name', 'middle_name', 'first_name', 'full_name', 'prev_name', 'birth_date', 'gender',
            'mrtl_stat',

            'biz_name', 'shrt_biz_name', 'prev_reg_biz_name', 'biz_category', 'biz_category2', 'biz_category3',

            'ctrl_pctg',

            'pri_addr_typ', 'pri_addr_line1', 'pri_addr_line2', 'pri_addr_line3', 'pri_addr_city_lga',
            'pri_addr_state', 'pri_addr_post_code', 'pri_addr_country',
            'sec_addr_typ', 'sec_addr_line1', 'sec_addr_line2', 'sec_addr_line3', 'sec_addr_city_lga',
            'sec_addr_state', 'sec_addr_post_code', 'sec_addr_country',

            'area_code', 'phone_no', 'fax_no', 'e_mail_addr', 'url'
        ),
    }
    d['mfi']['corp'], d['mfi']['ndvdl'] = d['mfi']['comm'], d['mfi']['cons']
    d['mfi']['commfac'] = d['mfi']['consfac'] = d['mfi']['corpfac'] = d['mfi']['ndvdlfac'] = d['mfi']['fac']
    d['pmi']['grntr'] = d['cmb']['grntr']
    d['pmi']['cons'] = d['cmb']['ndvdl']
    # d['pmi']['prnc'] = d['mfi']['prnc'] = d['cmb']['prnc']
    return d


def c0mf() -> tuple:
    return (
        'account_no', 'cust_id', 'last_name', 'first_name', 'middle_name', 'gender', 'mrtl_stat', 'full_name',
        ' outstanding_bal', ' overdue_amt', 'overdue_days', 'maturity_date', 'currency', 'asset_class', 'acct_stat',
        'customer_type', ' approved_amt', 'birth_date', 'bvn', 'biz_reg_no', 'pri_addr_line1', 'pri_addr_line2',
        'e_mail_addr', 'mobile_no',
    )


def visa():
    return c0mf()


c1mf = fnco = (
    'national_id_no', 'i_pass_no', 'drivin_license_no', 'pencom_no', 'other id number', 'gender', 'birth_place',
    'branch_code', 'account_no', 'last_name', 'title', 'first_name', 'middle_name', 'pri_addr_line1', 'pri_addr_line2',
    'blnk0', 'pri_addr_city_lga', 'blnk1', 'owner tenant', 'sec_addr_line1', 'blnk2', 'sec_addr_line2', 'blnk3',
    'blnk4', 'sec_addr_line3', 'blnk5', 'blnk6', 'blnk7', 'sec_addr_city_lga', 'blnk8', 'ownership', 'blnk9',
    ' account type code ', 'type of account', 'blnk10', 'acct_open_date', 'deferred payment date', 'last_paid_date',
    'opening balance', 'current balance', 'current bal indicator', 'overdue_amt', 'instal_amt', 'months in arrears',
    'status code', 'repay_freq', 'tenor', 'status date', 'cust_id', 'currency', 'phone_no', 'mobile_no', 'work_phone',
    'employr_name', 'income', 'income frequency', 'occupation', 'own_count', 'maturity_date', 'expected pay date',
    'last payment received', 'employr_addr_line1', 'employr_addr_line2', 'employr_addr_line3', 'e_mail_addr',
    'nationality', 'mrtl_stat', 'dependants', 'grntr_name', 'grntr_addr_line1', 'grntr_addr_line2', 'grntr_addr_line3',
    'grntr_phone_no'
)


def fandl() -> dict:
    idict = {
        'corp': iff()['cmb']['corp'],
        'ndvdl': iff()['cmb']['ndvdl'],
        'corpfac': (
            'branch_code', 'prev_branch_id', 'account_no', 'prev_acct_no', 'consent_stat', 'consent_d8from',
            'consent_d8to', 'facility_type', 'facility_purpose', 'ownership', 'disbursed_amt', 'd8_approved',
            'currency', 'int_rate', 'd8_disbursed', 'maturity_date', 'repay_freq', 'instal_count', 'outstanding_bal',
            'last_paid_amt', 'last_paid_date', 'overdue_days', 'overdue_amt', 'outstanding_amt_count', 'asset_class',
            'acct_stat', 'amend_date', 'wrttn_off_amt', 'wrttn_off_rsn', 'acct_clsd_date', 'acct_cls_rsn',
            'secure_stat', 'grnt_cov', 'legal_stat', 'trxn_typ_cod', 'dspt_id'
        ),
    }
    idict.update({'fac': idict['corpfac'], 'ndvdlfac': idict['corpfac'], })
    return idict


def phed() -> dict:
    return {
        'corpfac': (
            'branch_code', 'prev_branch_id', 'account_no', 'prev_acct_no', 'consent_stat_blnk', 'consent_stat',
            'consent_d8from', 'consent_d8to', 'facility_type', 'facility_purpose', 'ownership', 'disbursed_amt',
            'd8_approved', 'currency', 'int_rate', 'd8_approved', 'maturity_date', 'repay_freq', 'instal_count',
            'outstanding_bal', 'last_paid_amt', 'last_paid_date', 'overdue_days', 'overdue_amt', 'instal_missd_count',
            'asset_class', 'acct_stat', 'tariff'
        ),
        'corp': (
            'branch_code', 'prev_branch_id', 'account_no', 'cust_id', 'prev_acct_no', 'prev_cust_id', 'biz_reg_no',
            'date of registration', 'prev_biz_reg_no', 'incorp_cert', 'trustee_reg_no', 'tax_id', 'iss_auth1',
            'id_code1', 'id_code1d8xpry', 'iss_auth2', 'id_code2', 'is_sme', 'biz_name', 'shrt_biz_name',
            'prev_reg_biz_name', 'prev_shrt_biz_name', 'pri_addr_line1', 'pri_addr_line2', 'pri_addr_line3',
            'pri_addr_country', 'phone_no', 'fax_no', 'e_mail_addr', 'url', 'tariff'
        ),
    }


def min_mndtry() -> dict:
    d = {
        'comm': (
            'branch_code', 'account_no', 'cust_id', 'biz_name', 'pri_addr_typ', 'pri_addr_line1', 'pri_addr_country',),
        'ndvdl': (
            'branch_code', 'account_no', 'cust_id', 'nationality', 'pri_addr_typ', 'pri_addr_line1', 'pri_addr_country',
            'birth_date', 'gender'),
        'ndvdlfac': (
            'branch_code', 'account_no', 'consent_stat', 'facility_type', 'ownership', 'disbursed_amt', 'currency',
            'maturity_date', 'outstanding_bal', 'overdue_days', 'overdue_amt', 'asset_class', 'acct_stat',
            'secure_stat', 'grnt_cov', 'trxn_typ_cod',),
    }
    d['corp'], d['cons'] = d['comm'], d['ndvdl']
    d['commfac'] = d['consfac'] = d['corpfac'] = d['fac'] = d['ndvdlfac']
    return d


def amnt_fields() -> tuple:
    f = ('approved_amt', 'disbursed_amt', 'disbursed_amt2date', 'outstanding_bal', 'instal_amt', 'annl_sal',
         'int_instal_amt', 'instal_amt', 'highest_crdt_amt', 'int_last_paid_amt', 'last_paid_amt', 'int_overdue_amt',
         'overdue_amt', 'int_outstanding_amt', 'outstanding_amt', 'wrttn_off_amt',)
    return f


def number_fields() -> tuple:
    # return amnt_fields() + ('cycle_ver', 'int_rate',)
    return 'cycle_ver', 'int_rate', 'int_overdue_days', 'overdue_days',


def date_fields() -> tuple:
    f = ('maturity_date', 'litigxn_date', 'int_last_paid_date', 'last_paid_date', 'acct_clsd_date', 'd8_acct_stat',
         'd8_disbursed', 'consent_d8from', 'consent_d8to', 'd8_approved', 'amend_date', 'incorp_date', 'id_code1d8xpry',
         'id_code2d8xpry', 'birth_date', 'i_pass_expiry', 'bvn_d8xpry', 'biz_d8reg', 'birth_incorp_date', 'birth_date1',
         )
    return f


def dates_and_number_fields() -> tuple:
    return date_fields() + number_fields()


def cols2cat():
    return ('acct_stat', 'asset_class', 'area_code', 'branch_code', 'consent_stat', 'currency', 'cycle_ver', 'dpid',
            'employ_stat', 'facility_type', 'facility_purpose', 'gender', 'grnt_cov', 'grntr_type', 'is_sme',
            'legal_stat', 'mrtl_stat', 'nationality', 'occpaxn', 'ownership', 'prev_branch_id', 'repay_freq',
            'secure_stat', 'trxn_typ_cod',)


def prnc_cols():
    ps0 = ['cust_id', 'last_name', 'first_name', 'middle_name', 'birth_date', 'gender',
           'pri_addr_line1', 'pri_addr_line2', 'pri_addr_city_lga', 'pri_addr_state', 'pri_addr_country',
           'national_id_no', 'drivin_license_no', 'bvn', 'i_pass_no', 'phone_no', 'e_mail_addr', 'psxn_in_biz', ]
    ps1 = ['cust_id', 'last_name1', 'first_name1', 'middle_name1', 'birth_date1', 'gender1',
           'pri_addr_line11', 'pri_addr_line21', 'pri_addr_city_lga1', 'pri_addr_state1', 'pri_addr_country1',
           'national_id_no1', 'drivin_license_no1', 'bvn1', 'i_pass_no1', 'phone_no1', 'e_mail_addr1', 'psxn_in_biz1', ]
    psa = ['phone_no2', 'sec_addr_line', 'sec_addr_city_lga', 'sec_addr_state', 'tax_id', 'pic_file_path', ]
    xtr_cols = ['batch_no', 'cycle_ver', 'dpid', 'status', 'dp_name', 'data_file', ]
    return ps0, ps1, psa, xtr_cols
