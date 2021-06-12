# coding: utf-8


from IoCEngine.commons import get_logger

mdjlog = get_logger(__name__)


def minify(df):
    mdjlog.info(df.memory_usage(deep=True))
    df.fillna('', inplace=True)
    cols = df.columns
    if 'biz_corp_type' in cols:
        df['biz_corp_type'] = df.biz_corp_type.astype('category')

    if 'biz_category' in cols:
        df['biz_category'] = df.biz_category.astype('category')

    if 'pri_addr_state' in cols:
        df['pri_addr_state'] = df.pri_addr_state.astype('category')

    if 'pri_addr_country' in cols:
        df['pri_addr_country'] = df.pri_addr_country.astype('category')

    if 'sec_addr_state' in cols:
        df['sec_addr_state'] = df.sec_addr_state.astype('category')

    if 'biz_category2' in cols:
        df['biz_category2'] = df.biz_category2.astype('category')

    if 'biz_category3' in cols:
        df['biz_category3'] = df.biz_category3.astype('category')

    #

    if 'acct_stat' in cols:
        df['acct_stat'] = df.acct_stat.astype('category')

    if 'currency' in cols:
        df['currency'] = df.currency.astype('category')

    if 'facility_type' in cols:
        df['facility_type'] = df.facility_type.astype('category')

    if 'facility_purpose' in cols:
        df['facility_purpose'] = df.facility_purpose.astype('category')

    if 'repay_freq' in cols:
        df['repay_freq'] = df.repay_freq.astype('category')

    if 'asset_class' in cols:
        df['asset_class'] = df.asset_class.astype('category')

    if 'legal_stat' in cols:
        df['legal_stat'] = df.legal_stat.astype('category')

    if 'consent_stat' in cols:
        df['consent_stat'] = df.consent_stat.astype('category')

    if 'secure_stat' in cols:
        df['secure_stat'] = df.secure_stat.astype('category')

    if 'collateral_type' in cols:
        df['collateral_type'] = df.collateral_type.astype('category')

    #

    if 'gender' in cols:
        df['gender'] = df.gender.astype('category')

    if 'nationality' in cols:
        df['nationality'] = df.nationality.astype('category')

    if 'mrtl_stat' in cols:
        df['mrtl_stat'] = df.mrtl_stat.astype('category')

    if 'employ_stat' in cols:
        df['employ_stat'] = df.employ_stat.astype('category')

    if 'ownership' in cols:
        df['ownership'] = df.ownership.astype('category')

    if 'employr_addr_state' in cols:
        df['employr_addr_state'] = df.employr_addr_state.astype('category')

    if 'employr_addr_country' in cols:
        df['employr_addr_country'] = df.employr_addr_country.astype('category')

    if 'title' in cols:
        df['title'] = df.title.astype('category')

    #

    if 'is_sme' in cols:
        df['is_sme'] = df.is_sme.astype('category')

    #

    if 'wrttn_off_rsn' in cols:
        df['wrttn_off_rsn'] = df.wrttn_off_rsn.astype('category')

    if 'acct_cls_rsn' in cols:
        df['acct_cls_rsn'] = df.acct_cls_rsn.astype('category')

    if 'grnt_cov' in cols:
        df['grnt_cov'] = df.grnt_cov.astype('category')

    if 'trxn_typ_cod' in cols:
        df['trxn_typ_cod'] = df.trxn_typ_cod.astype('category')

    #

    if 'employ_stat' in cols:
        df['employ_stat'] = df.employ_stat.astype('category')

    #

    mdjlog.info(df.memory_usage(deep=True))
