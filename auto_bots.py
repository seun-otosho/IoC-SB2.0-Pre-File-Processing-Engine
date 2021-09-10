#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Created 2016

@author: tolorun
"""

# from IoCEngine.celeryio import app
from IoCEngine.logger import get_logger
from IoCEngine.utils.file import DataFiles


# @app.task(name='upd8batch')
def upd8batch(process3DBatch, batches2U):  # , sb2file, syndifiles
    # if not process3DBatch['data_type'] in ('all', 'allcorp', 'allndvdl',):
    mdjlog = get_logger(batches2U[0]['dp_name'])
    for loaded_batch in batches2U:
        status = 'Loaded' if '_fac_' in loaded_batch['file_name'] else 'Syndicated'
        data_file = DataFiles.objects(dp_name=loaded_batch['dp_name'], cycle_ver=loaded_batch['cycle_ver'],
                                      dpid=loaded_batch['dpid'], in_mod=loaded_batch['in_mod'],
                                      out_mod=loaded_batch['out_mod'],
                                      data_type=loaded_batch['data_type'], ).first()
        # , batch_no=loaded_batch['batch_no']
        try:
            loaded_batch.update(batch_no=process3DBatch['batch_no'], status=status)  # , sb2file=sb2file
        except Exception as e:
            mdjlog.error(e)
        try:
            data_file.update(batch_no=process3DBatch['batch_no'], status=status, )
        except Exception as e:
            mdjlog.error(e)
        mdjlog.info('Batch {} & Data File {} Updat3D.'.format(loaded_batch, data_file))
        # load3Dbatch.update(batch_no=load3Dbatch['batch_no'], status='Syndicated')  # , sb2file=sb2file