import datetime

from flask import Flask
from flask.ext import admin
from flask.ext.admin.contrib.mongoengine import ModelView
from flask.ext.admin.form import rules

# Create application

app = Flask(__name__)

# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = '123456790'
app.config['MONGODB_SETTINGS'] = {'DB': 'IoC'}

# Create models
from utilities import db

db.init_app(app)

from utilities.models import ColumnMapping, InMode, IoCField, SB2ExceptionLog


# Define mongoengine documents
class User(db.Document):
    name = db.StringField(max_length=40)
    tags = db.ListField(db.ReferenceField('Tag'))
    password = db.StringField(max_length=40)

    def __unicode__(self):
        return self.name


class Todo(db.Document):
    title = db.StringField(max_length=60)
    text = db.StringField()
    done = db.BooleanField(default=False)
    pub_date = db.DateTimeField(default=datetime.datetime.now)
    user = db.ReferenceField(User, required=False)

    # Required for administrative interface
    def __unicode__(self):
        return self.title


class Tag(db.Document):
    name = db.StringField(max_length=10)

    def __unicode__(self):
        return self.name


class Comment(db.EmbeddedDocument):
    name = db.StringField(max_length=20, required=True)
    value = db.StringField(max_length=20)
    tag = db.ReferenceField(Tag)


class Post(db.Document):
    name = db.StringField(max_length=20, required=True)
    value = db.StringField(max_length=20)
    inner = db.ListField(db.EmbeddedDocumentField(Comment))
    lols = db.ListField(db.StringField(max_length=20))


class File(db.Document):
    name = db.StringField(max_length=20)
    data = db.FileField()


class Image(db.Document):
    name = db.StringField(max_length=20)
    image = db.ImageField(thumbnail_size=(100, 100, True))


# Customized admin views
class ColumnMappingView(ModelView):
    # column_filters = ['name']'ioc_name'
    column_searchable_list = ('in_name',)


class SB2LogDetailView(ModelView):
    column_filters = ('dpid', 'dp_name', 'category', 'file_id', 'stage', 'severity', 'account_no',)
    column_searchable_list = ('dpid', 'dp_name', 'category', 'stage', 'severity', 'account_no',)
    column_list = ('file_id', 'dp_name', 'category', 'account_no', 'error_description', 'severity', 'stage',)
    can_delete = False
    can_edit = False
    can_export = True


class IoCFieldView(ModelView):
    # column_filters = ['in_mode']  # 'ioc_name'
    column_searchable_list = ('name',)


class UserView(ModelView):
    column_filters = ['name']

    column_searchable_list = ('name', 'password')

    form_ajax_refs = {
        'tags': {
            'fields': ('name',)
        }
    }


class TodoView(ModelView):
    column_filters = ['done']

    form_ajax_refs = {
        'user': {
            'fields': ['name']
        }
    }


'''
class IoCFieldView(ModelView):
    form_subdocuments = {
        'data_type': {
            'form_subdocuments': {
                None: {
                    # Add <hr> at the end of the form
                    'form_rules': ('name', rules.HTML('<hr>')),
                    'form_widget_args': {
                        'name': {
                            'style': 'color: red'
                        }
                    }
                }
            }
        }
    }
'''


class PostView(ModelView):
    form_subdocuments = {
        'inner': {
            'form_subdocuments': {
                None: {
                    # Add <hr> at the end of the form
                    'form_rules': ('name', 'tag', 'value', rules.HTML('<hr>')),
                    'form_widget_args': {
                        'name': {
                            'style': 'color: red'
                        }
                    }
                }
            }
        }
    }


# Flask views
@app.route('/')
def index():
    return '<a href="/admin/">Click me to get to Admin!</a>'


if __name__ == '__main__':
    # Create admin
    admin = admin.Admin(app, 'CRC: IoC Engine')

    # Add views
    # admin.add_view(UserView(User))
    # admin.add_view(TodoView(Todo))
    # admin.add_view(ModelView(Tag))
    # admin.add_view(PostView(Post))
    admin.add_view(ColumnMappingView(ColumnMapping))
    # admin.add_view(IoCFieldView(IoCField))
    admin.add_view(IoCFieldView(IoCField))
    # admin.add_view(ModelView(Image))
    # admin.add_view(ModelView(File))
    # admin.add_view(ModelView(InMode))
    admin.add_view(SB2LogDetailView(SB2ExceptionLog))

    # Start app
    app.run(debug=True, port=8800)
