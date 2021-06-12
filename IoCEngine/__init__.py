import logging
import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from IoCEngine.config.pilot import BROKER_URL, mdb

app = Flask(__name__)
config_name = 'pilot'

app.config.from_object('IoCEngine.config.pilot')

curfilePath = os.path.abspath(__file__)
curDir = os.path.abspath(
    os.path.join(curfilePath, os.pardir))  # this will return current directory in which python file resides.
parentDir = os.path.abspath(os.path.join(curDir, os.pardir))  # this will return parent directory.
parentDir = os.path.abspath(os.path.join(parentDir, os.pardir))  # this will return parent directory.
drop_zone = parentDir + os.path.sep + 'drop_zone' + os.path.sep
log_dir = parentDir + os.path.sep + 'logs' + os.path.sep
xtrcxn_area = parentDir + os.path.sep + 'extraction_zone' + os.path.sep

db = SQLAlchemy(app)
mdb.init_app(app)

level = logging.INFO
