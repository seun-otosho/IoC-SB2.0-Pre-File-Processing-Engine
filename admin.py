#!/usr/bin/python3.6
# -*- coding: utf-8 -*-
"""
Created 2016

@author: tolorun
"""

from flask import Flask
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_admin.contrib.mongoengine import ModelView as MongoModelView

from IoCEngine import app, db
from IoCEngine.utils.file import DataBatchProcess, SB2FileInfo, DataFiles
from IoCEngine.models import CtgryCode, DataProvider


from lib.flask_admin_material import setup_templates

app = setup_templates(app)

# app = Flask(__name__)

admin = Admin(
    app, name='IoCEngine',
    template_mode='bootstrap3'
)


# Add administrative views here

class CtgryCodeView(ModelView):
    can_create = True
    can_edit = True
    can_delete = False
    page_size = 25


class DataProviderView(ModelView):
    column_searchable_list = ['code_name', 'dpid', 'ctgry', 'sbmxn_pt']
    column_filters = ['ctgry', 'sbmxn_pt']
    can_create = True
    can_edit = True
    can_delete = False
    page_size = 10


class DataBatchProcessView(MongoModelView):
    column_filters = ('dpid', 'dp_name', 'category', 'file_id', 'stage', 'severity', 'account_no',)
    column_searchable_list = ('dpid', 'dp_name', 'category', 'stage', 'severity', 'account_no',)
    column_list = ('file_id', 'dp_name', 'category', 'account_no', 'error_description', 'severity', 'stage',)
    can_delete = False
    can_edit = False
    can_export = True


class DataFilesView(MongoModelView):
    # column_filters = ('dp_name', 'dpid', 'file_name', 'data_type', 'date_reported', 'date_processed', 'status',)
    column_searchable_list = ('dp_name', 'dpid', 'file_name', 'data_type', 'status',)
    # column_list = ('dp_name', 'dpid', 'file_name', 'data_type', 'date_reported', 'date_processed', 'status',)
    can_delete = False
    can_edit = False
    can_export = True


class SB2FileInfoView(MongoModelView):
    # column_filters = ('dpid', 'dp_name', 'category', 'file_id', 'stage', 'severity', 'account_no',)
    column_searchable_list = ('dpid', 'dp_name',)
    # column_list = ('file_id', 'dp_name', 'category', 'account_no', 'error_description', 'severity', 'stage',)
    can_delete = False
    can_edit = False
    can_export = True


admin.add_view(MongoModelView(DataBatchProcess, db.session))
# admin.add_view(DataFilesView(DataFiles, db.session))
admin.add_view(MongoModelView(DataFiles, db.session))
admin.add_view(MongoModelView(SB2FileInfo, db.session))
admin.add_view(CtgryCodeView(CtgryCode, db.session))
admin.add_view(DataProviderView(DataProvider, db.session))

if __name__ == '__main__':
    app.run(port=5000, debug=True, host='0.0.0.0')
