import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

base_file_name = datetime.now().strftime('%y%m%d')[-5:]
scores_tns = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.1.16)(PORT = 1521)) '
scores_tns += '(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = crcpdb01) ) )'
scores_con = create_engine("oracle+cx_oracle://scores:scores@" + scores_tns)


def others_qry(count):
    sql = """
SELECT
  hdr_sub.merged_ruid,
  replace(hdr_sub.header_block, 'Header|',
          'Header|' || TO_CHAR(SYSDATE, 'YMMDDHH24MI') || '-'
  )                                      hdr_blk,
  hdr_sub.subject_header sbjt_blk,
  nvl(nq_blk.enquiries_block, 'INQC|||') nqry_blk
FROM aa_fico_header_subject_blocks hdr_sub, aa_fico_enquiries_block nq_blk
WHERE hdr_sub.merged_ruid = nq_blk.merged_ruid (+) AND hdr_sub.merged_ruid IN (SELECT merged_ruid
                                                                               FROM all_mruid_urid_counts
                                                                               WHERE urids  =  {})
""".format(count)
    if isinstance(count, str):
        return sql.replace('  =  ', ' ')
    return sql


def fac_qry(count):
    sql = """
SELECT
  merged_ruid,
  facility_block
FROM aa_fico_all_facility_blocks
WHERE merged_ruid IN (SELECT merged_ruid
                      FROM all_mruid_urid_counts
                      WHERE urids  =  {})
GROUP BY merged_ruid, facility_block
""".format(count)
    if isinstance(count, str):
        return sql.replace('  =  ', ' ')
    return sql


def one_qry(lmt): return """
SELECT
  replace(hdr_sub.header_block, 'Header|', 'Header|' || TO_CHAR(SYSDATE, 'YMMDDHH24MI') || '-') hdr_blk,
  hdr_sub.subject_header                                                                        sbjt_blk,
  facility_block                                                                                fac_blk,
  nvl(nq_blk.enquiries_block, 'INQC|||')                                                        nqry_blk
FROM aa_fico_header_subject_blocks hdr_sub, aa_fico_all_facility_blocks fac_blk, aa_fico_enquiries_block nq_blk
WHERE hdr_sub.merged_ruid = fac_blk.merged_ruid
      AND fac_blk.merged_ruid IN (SELECT merged_ruid
                                  FROM mruid_sqnc
                                  WHERE sqnc >= {} - 1000000 and  sqnc < {})
      AND hdr_sub.merged_ruid = nq_blk.merged_ruid (+)
      AND fac_blk.merged_ruid = nq_blk.merged_ruid (+)
""".format(lmt, lmt)


def one_credit_scores():
    one_fn = str(base_file_name) + 'one.txt'
    with open(one_fn, 'w') as one_fh:
        for lmt in list(range(1000000, 7000000, 1000000)):
            one_df = pd.read_sql(one_qry(int(lmt)), scores_con)
            for rec in one_df.itertuples():
                one_fh.write("|".join((rec.hdr_blk, rec.sbjt_blk, rec.fac_blk, rec.nqry_blk)) + '\n')


# todo 
one_credit_scores()


def others_credit_scores():
    others_fn, x = str(base_file_name) + 'others.txt', 0
    with open(others_fn, 'a') as others_fh:
        for x in list(range(2, 5)):
            print('starting{}xDF@{}'.format(x, datetime.now()))

            fac_df = pd.read_sql(fac_qry(int(x)), scores_con)
            print('..got{}xfacDF@{} with{}'.format(x, datetime.now(), fac_df.shape))
            fac_pvt = fac_df.groupby(['merged_ruid'])['facility_block'].agg({'fac_blk': lambda x: '|'.join(x)})
            fac_pvt['merged_ruid'] = fac_pvt.index
            fac_pvt.rename_axis(None, inplace=True)
            del fac_df

            hdr_sub_enq_df = pd.read_sql(others_qry(int(x)), scores_con)
            print('got{}xhdr_sub@{} with{}'.format(x, datetime.now(), hdr_sub_enq_df.shape))

            print('...pivot3D{}x@{} with{}'.format(x, datetime.now(), fac_pvt.shape))
            df = pd.merge(hdr_sub_enq_df, fac_pvt, on='merged_ruid', how='inner')
            del fac_pvt, hdr_sub_enq_df

            print('.... writing@{} with{}'.format(datetime.now(), df.shape))
            for rec in df.itertuples():
                others_fh.write("|".join((rec.hdr_blk, rec.sbjt_blk, rec.fac_blk, rec.nqry_blk)) + '\n')

            # for mruid in fac_df.merged_ruid.unique():
            #     mruid_df = fac_df[fac_df['merged_ruid'] == mruid]
            #     fac_block = "|".join(str(col) for col in mruid_df.facility_block.tolist())
            #     mruid_hdr_sub_enq_df = hdr_sub_enq_df[hdr_sub_enq_df['merged_ruid'] == mruid]
            #     others_fh.write("|".join((mruid_hdr_sub_enq_df.hdr_blk.values[0],
            #                               mruid_hdr_sub_enq_df.sbjt_blk.values[0], fac_block,
            #                               mruid_hdr_sub_enq_df.nqry_blk.values[0])) + '\n')

            print('.completed{}x@{}'.format(x, datetime.now()))
            print('#' * 24 * 3 + '\n')
            x = x + 1

        fac_df = pd.read_sql(fac_qry('>= {}'.format(x)), scores_con)
        print('..got{}xfacDF@{} with{}'.format(x, datetime.now(), fac_df.shape))
        fac_pvt = fac_df.groupby(['merged_ruid'])['facility_block'].agg({'fac_blk': lambda x: '|'.join(x)})
        fac_pvt['merged_ruid'] = fac_pvt.index
        fac_pvt.rename_axis(None, inplace=True)
        del fac_df

        hdr_sub_enq_df = pd.read_sql(others_qry('>= {}'.format(x)), scores_con)
        print('got{}xhdr_sub@{} with{}'.format(x, datetime.now(), hdr_sub_enq_df.shape))

        print('...pivot3D{}x@{} with{}'.format(x, datetime.now(), fac_pvt.shape))
        df = pd.merge(hdr_sub_enq_df, fac_pvt, on='merged_ruid', how='inner')
        del fac_pvt, hdr_sub_enq_df

        print('..... writing@{} with{}'.format(datetime.now(), df.shape))
        for rec in df.itertuples():
            others_fh.write("|".join((rec.hdr_blk, rec.sbjt_blk, rec.fac_blk, rec.nqry_blk)) + '\n')


others_credit_scores()


# for x in list(range(2, 32)):
#     score10f = open(fn, 'a')
#     m10df = pd.read_sql(fac_blocks(int(x)), scores_con)
#     fac10df = pd.read_sql(fac10q(x), scores_con)
#     for idx, others in m10df.itertuples():
#         # print (others)
#         # if idx < 2:
#         fac_df = fac10df[fac10df['merged_ruid'] == others['merged_ruid']]
#         # print (fac_df.shape)
#         # print ('*' * 102)
#         line = others.header_block + '|' + others.subject_header + '|' + \
#                '|'.join([fac.facility_block for jidx, fac in fac_df.iterrows()]) \
#                + '|' + others.enquiry_block
#         # print (line)
#         score10list.append(line)
#         score10f.write(line + '\n')
#         # print ('#' * 123)
#     score10f.close()
