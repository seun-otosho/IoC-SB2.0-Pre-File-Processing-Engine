import inspect
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from os import makedirs

from IoCEngine import level, log_dir

loggers = {}


def get_logger(logger_name=None, level=level):
    global loggers
    if loggers.get(logger_name):
        return loggers.get(logger_name)
    else:
        logger_name = inspect.stack()[1][3].replace('<', '').replace('>', '') if not logger_name else logger_name
        l = logging.getLogger(logger_name)
        l.propagate = False
        # formatter = logging.Formatter('%(asctime)s : %(message)s')     %(os.getpid())s|
        formatter = logging.Formatter(
            # '%(processName)s : %(process)s | %(threadName)s : %(thread)s:\n'
            '%(process)s - %(thread)s @ '
            '%(asctime)s {%(name)s:%(lineno)5d  - %(funcName)20s()} %(levelname)5s - %(message)s')
        # '[%(asctime)s] - {%(name)s:%(lineno)d  - %(funcName)20s()} - %(levelname)s - %(message)s')
        # fileHandler = TimedRotatingFileHandler(log_dir + '%s.log' % logger_name, mode='a')
        log_dir2use = log_dir + os.sep + logger_name + os.sep
        if not os.path.exists(log_dir2use): makedirs(log_dir2use)
        if l.handlers:
            l.handlers = []
        fileHandler = TimedRotatingFileHandler(log_dir2use + '%s.log' % logger_name)
        fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        l.setLevel(level)
        l.addHandler(fileHandler)
        l.addHandler(streamHandler)
        loggers.update(dict(name=logger_name))

        return l
