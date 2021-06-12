from flask_mongoengine import MongoEngine

from utilities import db


class SB2ExceptionLog(db.DynamicDocument):
    dpid = db.StringField()
    dp_name = db.StringField()
    category = db.StringField()
    file_id = db.IntField()
    split_file_id = db.IntField()
    segment_name = db.StringField()
    severity = db.StringField()
    account_no = db.StringField()
    error_description = db.StringField()
    stage = db.StringField()

    # meta = {'indexes': [{'fields': ['name', ], 'unique': True}]}
    meta = {
        'indexes': [
            {
                'fields': ['dpid', 'file_id', 'account_no', 'stage'],
                'unique': True
            }
        ],
        'collection': 'IoC_SB2Exception_logs'
    }


class InMode(db.EmbeddedDocument):
    name = db.StringField(max_length=10)
    meta = {'indexes': [{'fields': ['name', ], 'unique': True}]}

    # Required for administrative interface
    def __repr__(self):
        return 'IoC Data Type: {}'.format(self.name)

    def __str__(self):
        return 'IoC Data Type: {}'.format(self.name)

    def __unicode__(self):
        return 'IoC Data Type: {}'.format(self.name)


class IoCField(db.Document):
    name = db.StringField(max_length=40)
    # in_mode = db.StringField(max_length=10)
    in_mode = db.ListField(db.EmbeddedDocumentField(InMode), required=True)  # EmbeddedDocumentField
    # in_mode = db.ListField(db.StringField(max_length=10), required=True)  # EmbeddedDocumentField

    meta = {
        'indexes': [{'fields': ['name', ], 'unique': True}],
        'collection': 'ioc_field',
    }

    # Required for administrative interface
    def __repr__(self):
        try:  # if len(self.in_mode) > 0
            in_modes = ', '.join([inmod for inmod in self.in_mode if inmod != '' or inmod])
        except Exception as e:  # else
            in_modes = ''
        print(self.in_mode)
        return '{} provided in {}'.format(self.name.upper(),
                                          ' | '.join([inmode.name.upper() for inmode in self.in_mode]))

    def __str__(self):
        return '{} provided in {}'.format(self.name.upper(),
                                          ' | '.join([inmode.name.upper() for inmode in self.in_mode]))

    def __unicode__(self):
        return '{} provided in {}'.format(self.name.upper(),
                                          ' | '.join([inmode.name.upper() for inmode in self.in_mode]))


class ColumnMapping(db.Document):
    # in_mode = db.StringField(max_length=10, required=False)
    in_name = db.StringField(max_length=40)
    ioc_name = db.ReferenceField(IoCField, required=False)

    meta = {'indexes': [{'fields': ['in_name', 'ioc_name', ],
                         'unique': True}]}

    # Required for administrative interface
    def __repr__(self):
        return 'Field: {} Mapped to: {}'.format(self.in_name, self.ioc_name)

    def __str__(self):
        return 'Field: {} Mapped to: {}'.format(self.in_name, self.ioc_name)

    def __unicode__(self):
        return 'Field: {} Mapped to: {}'.format(self.in_name, self.ioc_name)
