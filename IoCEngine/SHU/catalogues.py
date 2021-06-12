import pandas as pd

from sqlalchemy import create_engine

from IoCEngine.config.pilot import SQLALCHEMY_DATABASE_URI as constr

sb2fixes_engine = create_engine(constr)


def country_ctlg():
    country_sql = ''' SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                      FROM aa_udf_catalogues WHERE catalogue_name = 'COUNTRY' '''
    country_df = pd.read_sql(country_sql, sb2fixes_engine)
    country_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in country_df.iterrows()
    }
    country_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in country_df.iterrows()
        }
    )
    country_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in country_df.iterrows()
        }
    )
    country_dict['NGN'] = 'NG'
    return country_dict


def state_ctlg():
    state_sql = ''' SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                    FROM aa_udf_catalogues WHERE catalogue_name = 'STATE' '''
    state_df = pd.read_sql(state_sql, sb2fixes_engine)
    state_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in state_df.iterrows()
    }
    state_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in state_df.iterrows()
        }
    )
    state_dict.update(
        {
            row['udf_cat_value'] + ' STATE': row['crc_cat_code'] for index, row in state_df.iterrows()
        }
    )
    state_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in state_df.iterrows()
        }
    )
    state_dict['ABUJA'] = '015'
    state_dict['FCT'] = '015'
    state_dict['AKWA IBOM'] = '003'
    return state_dict


def biz_sect_ctlg():
    biz_sect_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                        FROM aa_udf_catalogues WHERE segment = 'CMCS' AND field_code = 'BS' '''
    biz_sect_df = pd.read_sql(biz_sect_sql, sb2fixes_engine)
    biz_sect_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in biz_sect_df.iterrows()
    }
    biz_sect_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in biz_sect_df.iterrows()
        }
    )
    biz_sect_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in biz_sect_df.iterrows()
        }
    )
    return biz_sect_dict


def comm_biz_sect_ctlg():
    biz_sect_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                        FROM aa_udf_catalogues WHERE segment = 'CMCS' AND field_code = 'BS' '''
    biz_sect_df = pd.read_sql(biz_sect_sql, sb2fixes_engine)
    biz_sect_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in biz_sect_df.iterrows()
    }
    biz_sect_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in biz_sect_df.iterrows()
        }
    )
    biz_sect_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in biz_sect_df.iterrows()
        }
    )
    return biz_sect_dict


def asset_class_ctlg():
    asset_class_sql = ''' SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                          FROM aa_udf_catalogues WHERE field_code = 'AC' '''
    asset_class_df = pd.read_sql(asset_class_sql, sb2fixes_engine)
    asset_class_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in asset_class_df.iterrows()
    }
    asset_class_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in asset_class_df.iterrows()
        }
    )
    asset_class_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in asset_class_df.iterrows()
        }
    )
    asset_class_dict.update(
        {
            int(row['crc_cat_code']): row['crc_cat_code'] for index, row in asset_class_df.iterrows()
        }
    )
    return asset_class_dict


def acct_stat_ctlg():
    acct_stat_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                        FROM aa_udf_catalogues WHERE field_code = 'AS' '''
    acct_stat_df = pd.read_sql(acct_stat_sql, sb2fixes_engine)
    acct_stat_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in acct_stat_df.iterrows()
    }
    acct_stat_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in acct_stat_df.iterrows()
        }
    )
    acct_stat_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in acct_stat_df.iterrows()
        }
    )
    return acct_stat_dict


def cnsnt_stat_ctlg():
    cnsnt_stat_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                        FROM aa_udf_catalogues WHERE field_code = 'CS' '''
    cnsnt_stat_df = pd.read_sql(cnsnt_stat_sql, sb2fixes_engine)
    cnsnt_stat_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in cnsnt_stat_df.iterrows()
    }
    cnsnt_stat_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in cnsnt_stat_df.iterrows()
        }
    )
    cnsnt_stat_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in cnsnt_stat_df.iterrows()
        }
    )
    return cnsnt_stat_dict


def legal_stat_ctlg():
    legal_stat_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                        FROM aa_udf_catalogues WHERE field_code = 'LS' '''
    legal_stat_df = pd.read_sql(legal_stat_sql, sb2fixes_engine)
    legal_stat_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in legal_stat_df.iterrows()
    }
    legal_stat_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in legal_stat_df.iterrows()
        }
    )
    legal_stat_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in legal_stat_df.iterrows()
        }
    )
    return legal_stat_dict


def loan_typ_ctlg(out_mod=None):
    if out_mod == 'mfi':
        loan_typ_sql = ("SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code "
                        " FROM aa_udf_catalogues WHERE field_code = 'LT' and segment = 'CXMF' ")
    elif out_mod == 'pmi':
        loan_typ_sql = (" SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code "
                        " FROM aa_udf_catalogues WHERE field_code = 'LT' and segment = 'MXCF' ")
    else:
        loan_typ_sql = (" SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code "
                        " FROM aa_udf_catalogues WHERE field_code = 'LT' and segment = 'CXCF' ")

    loan_typ_df = pd.read_sql(loan_typ_sql, sb2fixes_engine)
    loan_typ_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in loan_typ_df.iterrows()
    }
    loan_typ_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in loan_typ_df.iterrows()
        }
    )
    loan_typ_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in loan_typ_df.iterrows()
        }
    )
    return loan_typ_dict


def repay_freq_ctlg():
    repay_freq_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                        FROM aa_udf_catalogues WHERE field_code = 'RF' '''
    repay_freq_df = pd.read_sql(repay_freq_sql, sb2fixes_engine)
    repay_freq_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in repay_freq_df.iterrows()
    }
    repay_freq_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in repay_freq_df.iterrows()
        }
    )
    repay_freq_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in repay_freq_df.iterrows()
        }
    )
    return repay_freq_dict


def sec_stat_ctlg():
    sec_stat_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code 
                      FROM aa_udf_catalogues WHERE field_code = 'SS' '''
    sec_stat_df = pd.read_sql(sec_stat_sql, sb2fixes_engine)
    sec_stat_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in sec_stat_df.iterrows()
    }
    sec_stat_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in sec_stat_df.iterrows()
        }
    )
    sec_stat_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in sec_stat_df.iterrows()
        }
    )
    return sec_stat_dict


def legal_const_ctlg():
    legal_const_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                            FROM aa_udf_catalogues WHERE field_code = 'ZT' and segment = 'CMCS' '''
    legal_const_df = pd.read_sql(legal_const_sql, sb2fixes_engine)
    legal_const_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in legal_const_df.iterrows()
    }
    legal_const_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in legal_const_df.iterrows()
        }
    )
    legal_const_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in legal_const_df.iterrows()
        }
    )
    return legal_const_dict


def comm_brw_typ_ctlg():
    comm_brw_typ_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                          FROM aa_udf_catalogues WHERE field_code = 'BT' and segment = 'CMCS' '''
    comm_brw_typ_df = pd.read_sql(comm_brw_typ_sql, sb2fixes_engine)
    comm_brw_typ_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in comm_brw_typ_df.iterrows()
    }
    comm_brw_typ_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in comm_brw_typ_df.iterrows()
        }
    )
    comm_brw_typ_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in comm_brw_typ_df.iterrows()
        }
    )
    return comm_brw_typ_dict


def cons_brw_typ_ctlg():
    cons_brw_typ_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                          FROM aa_udf_catalogues WHERE field_code = 'BT' and segment = 'CNCS' '''
    cons_brw_typ_df = pd.read_sql(cons_brw_typ_sql, sb2fixes_engine)
    cons_brw_typ_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in cons_brw_typ_df.iterrows()
    }
    cons_brw_typ_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in cons_brw_typ_df.iterrows()
        }
    )
    cons_brw_typ_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in cons_brw_typ_df.iterrows()
        }
    )
    return cons_brw_typ_dict


def employ_stat_ctlg():
    employ_stat_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                          FROM aa_udf_catalogues WHERE field_code = 'ES' '''
    employ_stat_df = pd.read_sql(employ_stat_sql, sb2fixes_engine)
    employ_stat_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in employ_stat_df.iterrows()
    }
    employ_stat_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in employ_stat_df.iterrows()
        }
    )
    employ_stat_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in employ_stat_df.iterrows()
        }
    )
    return employ_stat_dict


def gender_ctlg():
    gender_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                    FROM aa_udf_catalogues WHERE field_code = 'GD' '''
    gender_df = pd.read_sql(gender_sql, sb2fixes_engine)
    gender_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in gender_df.iterrows()
    }
    gender_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in gender_df.iterrows()
        }
    )
    gender_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in gender_df.iterrows()
        }
    )
    # gender_dict.update(
    #     {
    #         int(row['crc_cat_value']): row['crc_cat_code'] for index, row in gender_df.iterrows() if
    #     str(row['crc_cat_code']).isdigit()
    #     }
    # )
    return gender_dict


def marital_stat_ctlg():
    marital_stat_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                          FROM aa_udf_catalogues WHERE field_code = 'MS' '''
    marital_stat_df = pd.read_sql(marital_stat_sql, sb2fixes_engine)
    marital_stat_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in marital_stat_df.iterrows()
    }
    marital_stat_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in marital_stat_df.iterrows()
        }
    )
    marital_stat_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in marital_stat_df.iterrows()
        }
    )
    return marital_stat_dict


def occpaxn_ctlg():
    occpaxn_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                      FROM aa_udf_catalogues WHERE field_code = 'OC' '''
    occpaxn_df = pd.read_sql(occpaxn_sql, sb2fixes_engine)
    occpaxn_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in occpaxn_df.iterrows()
    }
    occpaxn_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in occpaxn_df.iterrows()
        }
    )
    occpaxn_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in occpaxn_df.iterrows()
        }
    )
    return occpaxn_dict


def currency_ctlg():
    currency_sql = '''SELECT udf_cat_code, udf_cat_value, crc_cat_value, crc_cat_code
                      FROM aa_udf_catalogues WHERE field_code = 'CU' AND catalogue_name = 'CURRENCY' '''
    currency_df = pd.read_sql(currency_sql, sb2fixes_engine)
    currency_dict = {
        row['udf_cat_code']: row['crc_cat_code'] for index, row in currency_df.iterrows()
    }
    currency_dict.update(
        {
            row['udf_cat_value']: row['crc_cat_code'] for index, row in currency_df.iterrows()
        }
    )
    currency_dict.update(
        {
            row['crc_cat_value']: row['crc_cat_code'] for index, row in currency_df.iterrows()
        }
    )
    return currency_dict


def guarantee_cov():
    return {
        '1': '001',
        '001': '001',
        'Guaranteed': '001',
        '2': '002',
        '002': '002',
        'Not Guaranteed': '002',
        '3': '003',
        '003': '003',
        'Government Backed Guarantees': '003',
    }


def disco_biz_units():
    return {'Garden City Main': '001',
            'Garden City East': '002',
            'Garden City New': '003',
            'Garden City Central': '004',
            'Garden City Industrial': '005',
            'Paradise City North': '006',
            'Glory City Main': '007',
            'Paradise City Main': '008',
            'Promise City South': '009',
            'Promise City Main': '010',
            #
            'AhoadaMD': 1000,
            'BorokiriMD': 1001,
            'CalabarMD': 1002,
            'DiobuMD': 1003,
            'EketMD': 1004,
            'IgwurutaMD': 1005,
            'IkomMD': 1006,
            'Ikot-EkpeneMD': 1007,
            'OgojaMD': 1008,
            'OnneMD': 1009,
            'OyigboMD': 1010,
            'RumuodomayaMD': 1011,
            'RumuolaMD': 1012,
            'RumuolaSC1': 1013,
            'TransamadiMD': 1014,
            'UyoMD': 1015,
            'YenagoaMD': 1016
            }


def utility_ass_cls():
    return {
        'Performing': '001',
        'Not Performing': '004',
    }
