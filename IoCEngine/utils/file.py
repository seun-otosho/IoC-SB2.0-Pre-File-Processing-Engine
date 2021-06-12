import json
from datetime import datetime, timedelta
from IoCEngine.commons import get_logger
from mongoengine import *
from pymongo import MongoClient

dbhs = 'localhost' # 'localhost'  # '172.16.2.12'
pym_conn = MongoClient(host=dbhs)
pym_db = pym_conn['IoC']

connect('IoC', host=dbhs, port=27017)


def xtrct_file_details(file):
    """extracts the details from the file name"""
    mdjlog = get_logger('IoC')
    try:
        fl_sgmnts0 = file.lower().split('_')
        fl_sgmnts1 = fl_sgmnts0.pop(len(fl_sgmnts0) - 1).split('.')
        fl_sgmnts = fl_sgmnts0 + fl_sgmnts1
        mdjlog.debug('extracted file details: {}'.format(fl_sgmnts))
        return fl_sgmnts
    except Exception as e:
        mdjlog.error(e)


def get_d8rprt3D(yyyymm, mdjlog):
    mdjlog = get_logger(mdjlog.name)
    try:
        any_day = datetime.date(datetime(int(yyyymm[:4]), int(yyyymm[4:]), 1))
        next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
        # mdjlog.info(next_month - datetime.timedelta(days=next_month.day))
        return (next_month - timedelta(days=next_month.day)).strftime('%d-%b-%Y')
    except Exception as e:
        mdjlog.error(e)
        mdjlog.info("Check file name for Data Provider '{}', is probably wrong!".format(mdjlog.name))


def dict_file(fl, dtls, mdjlog=None):
    mdjlog = get_logger(dtls[0])
    """converts file details into a dictionary"""
    try:
        data_dtls, dtls_count = {}, len(dtls)

        # mdjlog.info('dtls_count: {}'.format(dtls_count))

        data_dtls['dp_name'], data_dtls['in_mod'], data_dtls['data_type'], data_dtls['out_mod'], \
        data_dtls['cycle_ver'], data_dtls['date_reported'] = dtls[0], dtls[1], dtls[2], dtls[3], dtls[4], \
                                                             get_d8rprt3D(dtls[4], mdjlog)

        data_dtls['file_ndx'] = dtls[5] if len(dtls[5]) == 1 else None
        data_dtls['xtra_nfo'] = None
        if dtls_count == 7 and len(dtls[5]) > 1: data_dtls['xtra_nfo'] = dtls[5]
        if dtls_count == 8: data_dtls['xtra_nfo'] = dtls[6]
        data_dtls['file_name'], data_dtls['file_ext'] = fl, dtls[dtls_count - 1]

        mdjlog.info('\nfile analyses below\n{}'.format(json.dumps(data_dtls, indent=4)))
        svd_dtls = DataFiles(**data_dtls)
        try:
            svd_dtls.save()
        except Exception as e:
            mdjlog.error(e)
            svd_dtls = DataFiles.objects(file_name=data_dtls['file_name']).first()
            svd_dtls.update(re_dropped=svd_dtls['re_dropped'] + 1 if svd_dtls['re_dropped'] is not None else 1,
                            last_date_re_dropped=datetime.now())
        return str(svd_dtls.id)
    except Exception as e:
        mdjlog.error(e)


class DataFiles(DynamicDocument):
    file_name = StringField(required=True, unique=True, max_length=100)
    dpid = StringField(required=False, max_length=10)
    dp_name = StringField(required=True, max_length=20)
    data_type = StringField(required=True, max_length=20)
    in_mod = StringField(required=True, max_length=20)
    out_mod = StringField(required=True, max_length=20)
    file_ext = StringField(required=True, max_length=5)
    xtra_nfo = StringField(required=False, max_length=20)
    file_ndx = IntField(required=False)
    cycle_ver = IntField(required=True)
    date_reported = StringField(required=True, max_length=11)
    date_processed = DateTimeField(default=datetime.utcnow)
    re_dropped = IntField(defautl=0)
    last_date_re_dropped = DateTimeField(default=datetime.utcnow)
    status = StringField(required=False, max_length=20)
    batch_no = StringField(required=False, max_length=5)
    # meta = {
    #     'indexes': [
    #         'cycle_ver', ]
    # }

    def __str__(self):
        return "<{}: {} {} {} {}>".format(self.__class__.__name__, self.dp_name, self.cycle_ver, self.data_type,
                                          self.in_mod)


class DataFileReport(DynamicDocument):
    # embed data file object
    pass


class DataCycleReport(DynamicDocument):
    # reference dp_name and file name
    pass


class CombinedSubmissions(DynamicDocument):
    # reference dp_name and file name
    status = StringField(default='Loaded')
    # meta = {
    #     'indexes': [
    #         {'fields': ['account_no'], 'unique': False, },
    #         {'fields': ['cust_id'], 'unique': False, },
    #         {'fields': ['cycle_ver'], 'unique': False, },
    #         {'fields': ['dpid'], 'unique': False, },
    #     ]
    # }


class CorporateSubmissions(DynamicDocument):
    # reference dp_name and file name
    status = StringField(default='Loaded')
    # meta = {
    #     'indexes': [
    #         {'fields': ['account_no'], 'unique': False, },
    #         {'fields': ['cust_id'], 'unique': False, },
    #         {'fields': ['cycle_ver'], 'unique': False, },
    #         {'fields': ['dpid'], 'unique': False, },
    #     ]
    # }


class FacilitySubmissions(DynamicDocument):
    # reference dp_name and file name
    status = StringField(default='Loaded')
    # meta = {
    #     'indexes': [
    #         {'fields': ['account_no'], 'unique': False, },
    #         {'fields': ['cust_id'], 'unique': False, },
    #         {'fields': ['cycle_ver'], 'unique': False, },
    #         {'fields': ['dpid'], 'unique': False, },
    #     ]
    # }


class IndividualSubmissions(DynamicDocument):
    # reference dp_name and file name
    status = StringField(default='Loaded')
    # meta = {
    #     'indexes': [
    #         {'fields': ['account_no'], 'unique': False, },
    #         {'fields': ['cust_id'], 'unique': False, },
    #         {'fields': ['cycle_ver'], 'unique': False, },
    #         {'fields': ['dpid'], 'unique': False, },
    #     ]
    # }


class DataSegment(EmbeddedDocument):
    name = StringField(required=False, max_length=10, unique=True)

    def __str__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)


class DataBatchProcess(DynamicDocument):
    # dpid, cycle_ver, getID
    batch_no = StringField(required=False, max_length=5)
    cycle_ver = IntField(required=True)
    data_type = StringField(required=True, max_length=20)
    dp_name = StringField(required=True, max_length=20)
    dpid = StringField(required=False, max_length=10)
    status = StringField(required=False, max_length=20)
    segments = ListField(StringField())
    # segments = ListField(EmbeddedDocumentField(DataSegment))
    # meta = {
    #     'indexes': [
    #         {
    #             'fields': ['cycle_ver', 'dp_name', 'data_type', ],
    #             'unique': True
    #         }
    #     ]
    # }

    def __str__(self):
        return "<{}: {} {} {}>".format(self.__class__.__name__, self.dp_name, self.cycle_ver, self.data_type)


class FacilityHistoryData(DynamicDocument):
    # reference dp_name and file name
    account_no = StringField(required=True)
    cust_id = StringField(required=True)
    batch_no = StringField(required=False, max_length=5)
    cycle_ver = IntField(required=True)
    dpid = StringField(required=False, max_length=10)
    status = StringField(required=False, max_length=20)
    # meta = {
    #     'indexes': [
    #         'account_no',
    #         'cust_id',
    #         'cycle_ver',
    #         'dpid',
    #         {
    #             'fields': ['dpid', 'cust_id', 'account_no', 'cycle_ver', ],  # 'data_type',
    #             'unique': True
    #         }
    #     ]
    # }

    def __str__(self):
        return "<{}: {} {} {}>".format(self.__class__.__name__, self.dpid, self.account_no, self.cycle_ver)


class CustomerHistoryData(DynamicDocument):
    # reference dp_name and file name
    cust_id = StringField(required=True)
    batch_no = StringField(required=False, max_length=5)
    cycle_ver = IntField(required=True)
    dpid = StringField(required=False, max_length=10)
    status = StringField(required=False, max_length=20)
    # meta = {
    #     'indexes': [
    #         'cust_id',
    #         'cycle_ver',
    #         'dpid',
    #         {
    #             'fields': ['dpid', 'cust_id', 'cycle_ver', ],  # 'data_type',
    #             'unique': True
    #         }
    #     ]
    # }

    def __str__(self):
        return "<{}: {} {} {}>".format(self.__class__.__name__, self.dpid, self.cust_id, self.cycle_ver)


class SB2FileInfo(DynamicDocument):
    # embed data file object
    cycle_ver = IntField()
    dpid = StringField(max_length=10)
    dp_name = StringField(max_length=20)

    meta = {"collection": "sb2file_info"}

    def __str__(self):
        return "<{}: {} {} {}>".format(self.__class__.__name__, self.dpid, self.dp_name, self.cycle_ver)


class GlobalStats(DynamicDocument):
    # embed data file object
    cycle_ver = IntField()
    dpid = StringField(max_length=10)
    dp_name = StringField(max_length=20)

    meta = {"collection": "global_stats"}

    def __str__(self):
        return "<{}: {} {} {}>".format(self.__class__.__name__, self.dpid, self.dp_name, self.cycle_ver)
