
import pandas as pd

from IoCEngine.utils.file import pym_db

def compute_raw_stats(dpid, cycle_ver, data_type):
    if 'fac' in data_type:
        corp_cursor = pym_db.corporate_submissions.find(
            {"dpid": dpid, "cycle_ver": cycle_ver,}
        )  # "batch_no": loaded_batch['batch_no'],
        corp_df = pd.DataFrame(list(corp_cursor))
    if 'corp' in data_type:
        corp_cursor = pym_db.corporate_submissions.find(
            {"dpid": dpid, "cycle_ver": cycle_ver,}
        )  # "batch_no": loaded_batch['batch_no'],
        corp_df = pd.DataFrame(list(corp_cursor))
    if 'ndvdl' in data_type:
        corp_cursor = pym_db.corporate_submissions.find(
            {"dpid": dpid, "cycle_ver": cycle_ver,}
        )  # "batch_no": loaded_batch['batch_no'],
        corp_df = pd.DataFrame(list(corp_cursor))
    if 'combo' in data_type:
        corp_cursor = pym_db.corporate_submissions.find(
            {"dpid": dpid, "cycle_ver": cycle_ver,}
        )  # "batch_no": loaded_batch['batch_no'],
        corp_df = pd.DataFrame(list(corp_cursor))