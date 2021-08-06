import multiprocessing
import os

from elasticsearch import Elasticsearch

# from IoCEngine.logger import get_logger
#
# logger = get_logger()

DEBUG = True
# IGNORE_AUTH = False
SECRET_KEY = 'e3dcc2d78da6ceb302da66ff5a09b7f5eae0ff616ca8d710a52a4bf7a0672ef29cfe603d4ddfeccbc9525f6fb175a69fbc' \
             '0396bce825eb8470a8b44cf06d8fe9b40144e08846eddff9f0bef58897c06600529e36a2c70989c85bd5bf93314b15d881' \
             '54d87a62b7170c2d32ee4f0978bdcafbfc20f9350ba4a0b410ad6f120401e5495c7d826cddb6aff51e605da47f2927a175' \
             'be86684216255bfe564193a39a0299a3fbf64a532e232f3e0bfbe8aaff1b3ea1d1b226b7f2271357ef1e60169919afbef0' \
             '9780b11d702b7fa8596ebb31eaebd5111f6c771c4df4a5e9af29ee94909d5669557b023a8c35983833baf14c314deafa59' \
             '93de7037601331f1a6153c4736cc0908505538bf596f5f99edf4b3ecc4fb6bd7aa8ded882f4ad58e61c4e0f1dacc2e5f77' \
             'b12d5a74200ef46d779f532ef086bf0b767a72b1f05c8ad60e951f6a76652f4da55c60d7f9eb022abb0c5d03bb0d57af8f' \
             '5a317fe855c9dd5b9125481e714dd3dbad77abbeb5171708ebd1345c9411a8265333894221d3723d01fdc46281eeec2902' \
             '30ccefb38a508b5b5e8a78f1eebc11993c8db999956927d958b9153960d5b6dbb8739e2e4464720e5535904264e044c29d' \
             '8a7d7d8b149afe920e0c09220aa05153879af4ca7ab740228c69ebef0a1c868009f70b65d0833abf2ca5dd52cdbc9a7d6f' \
             '24df5042f31b48a3304234f3eb1cbeba85b2a3ac6b77'

dbhs = 'localhost'  # '172.16.2.12'  #
# SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres'
# SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://postgres:postgres@localhost:5432/IoC'
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://postgres:postgres@%s:5432/CRCIoCDev' % dbhs
SQLALCHEMY_TRACK_MODIFICATIONS = True
# SQLALCHEMY_TRACK_MODIFICATIONS = True
SQLALCHEMY_BINDS = {
    'lgcy': 'postgresql+psycopg2://postgres:postgres@localhost:5432/postgres',
    'sb2': 'postgresql+psycopg2://postgres:postgres@localhost:5432/sb2fixes',
    # 'logging': 'postgresql://postgres:postgres@104.131.174.54:5432/edc'
    # 'logging': 'postgresql://postgres:postgres@localhost:5432/edc'
}

WTF_CSRF_ENABLED = True

APP_NAME = "the CRC IOC"

# Flask-Mail settings
# MAIL_USERNAME = os.getenv('MAIL_USERNAME', 'info@crccreditbureau.com')
# MAIL_PASSWORD = os.getenv('MAIL_PASSWORD', 'AidCus2012!!!')
MAIL_USERNAME = os.getenv('MAIL_USERNAME', 'support@crccreditbureau.com')
MAIL_PASSWORD = os.getenv('MAIL_PASSWORD', 'POrtssup2014')
MAIL_DEFAULT_SENDER = os.getenv('MAIL_DEFAULT_SENDER', '"{}" <noreply@crccreditbureau.com>'.format(APP_NAME))
MAIL_SERVER = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
MAIL_PORT = int(os.getenv('MAIL_PORT', '465'))
MAIL_USE_SSL = os.getenv('MAIL_USE_SSL', True)
MAIL_USE_TLS = False

MONGODB_SETTINGS = {'DB': 'IoC'}

from flask_mongoengine import MongoEngine

mdb = MongoEngine()

# Flask-User settings
USER_APP_NAME = APP_NAME  # Used by email templates

USER_ENABLE_MULTIPLE_EMAILS = True

# celery configs
# CELERY_BROKER_URL = 'redis://localhost:6379/0'
# CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
BROKER_URL = 'mongodb://localhost:27017/IoC'
# BROKER_URL = 'amqp://guest:guest@localhost:5672//'
# CELERY_RESULT_BACKEND = 'amqp://guest:guest@localhost:5672//'
CELERY_RESULT_BACKEND = "mongodb"
CELERY_ACCEPT_CONTENT = ['pickle']  # 'json', 'pickle',, 'msgpack', 'yaml'
# CELERY_TASK_SERIALIZER = 'json'
CELERY_MONGODB_BACKEND_SETTINGS = {
    "host": "127.0.0.1",
    "port": 27017,
    "database": "IoC",
    "taskmeta_collection": "IoC_taskmeta",
}

try:
    host_str = "http://elastic:T31nbgRkSyxMNdyKvC87@0.0.0.0:9200/"
    hosts = host_str
    es = Elasticsearch(
        hosts=[hosts],
        timeout=100, max_retries=10, retry_on_timeout=True
    )
    es_ver = es.info()['version']['number']
    print(f"{es_ver=}")
except:
    es = Elasticsearch(
        timeout=100, max_retries=10, retry_on_timeout=True
    )

es_i = "ioc"
es_ii = "iocnow"

'''
from elasticsearch import Elasticsearch
def fMap(document_type):
    mapping={document_type:{\
             "properties":{\
             "a_field":{"type":"integer","store":"yes"},\
             "other_field": {"type":"string","index":"analyzed","store":"no"},\
             "title":{"type":"string","store":"yes","index": "analyzed","term_vector":"yes","similarity":"BM25"},\
             "content":{"type":"string","store":"yes","index":  "analyzed","term_vector": "yes","similarity":"BM25"}\
             }}}
    return mapping
def dSetting(nShards,nReplicas):
    dSetting={
    "settings":{"index":{"number_of_shards":nShards,"number_of_replicas":nReplicas}},\
    "analysis":{\
        "filter":{\
        "my_english":{"type":"english","stopwords_path":"english.txt"},\
        "english_stemmer":{"type":"stemmer","language":"english"}},\
        "analyzer":{"english":{"filter":["lowercase","my_english","english_stemmer"]}}}}
    return dSetting
def EsSetup(con,idx,docType,dset,mapping):
    con.indices.delete(index=idx,ignore=[400, 404])
    con.indices.create(index=idx,body=dset,ignore=400)
    con.indices.put_mapping(index=idx,doc_type=docType,body=mapping)

conEs=Elasticsearch([{'host':'localhost','port':9200,'timeout':180}])
dMap = fMap('docTypeName')
dSet = dSetting(8,1)
EsSetup(conEs,'idxName','docTypeName',dset,dMap)
'''

chunk_size = 40123  # 1234  #  12357  # 45789  #

cores = multiprocessing.cpu_count()
cores2u = round(cores / 3)
print(f"{cores=}\t|\t{cores2u=}\t|\t{chunk_size=}")
