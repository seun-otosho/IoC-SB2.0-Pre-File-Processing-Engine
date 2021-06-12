from flask import Flask
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView

from IoCEngine import app, db
from IoCEngine.models import CtgryCode, DataProvider


# app = Flask(__name__)

admin = Admin(app, name='IoCEngine', template_mode='bootstrap3')
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


admin.add_view(CtgryCodeView(CtgryCode, db.session))
admin.add_view(DataProviderView(DataProvider, db.session))


if __name__ == '__main__':
    app.run(port=5000, debug=True, host='0.0.0.0')
