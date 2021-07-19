from inspect import stack
from logging import Filter, Formatter, getLogger, StreamHandler
from logging.handlers import TimedRotatingFileHandler
from os import makedirs, sep
from os.path import abspath, exists, split, splitext

from IoCEngine import level, log_dir

dcrtd_frmtr = Formatter('%(process)s - %(thread)s @ %(asctime)s {%(name)s:%(lineno)5d - '
                        '%(func_name)14s() ~> %(funcName)18s()} %(levelname)s - %(message)s')

loggers = {}


def module_logger():
    path, filename = split(abspath(__file__))
    file, ext = splitext(filename)
    return file


class MyFunctionLogFilter(Filter):

    def __init__(self, func_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.func_name = func_name

    def filter(self, record, *args, **kwargs):
        self.__dict__.update(kwargs)
        try:
            record.func_name = record.args['func_name']
        except:
            record.func_name = self.func_name
        return True


def get_logger(logger_name=None, func_name=None, funcname=True, level=level, mini=False):
    global loggers, dcrtd_frmtr
    stck = stack()
    src_fyl = stck[1][1]
    func_name = func_name if func_name else splitext(src_fyl.split(sep)[-1])[0]
    if loggers.get(logger_name):
        return loggers.get(logger_name)
    else:
        logger_name = stack()[1][3].replace('<', '').replace('>', '') if not logger_name else logger_name
        l = getLogger(logger_name)
        l.propagate = False
        # formatter = logging.Formatter('%(asctime)s : %(message)s')     %(os.getpid())s|

        if mini:
            formatter = Formatter('%(message)s')
        elif funcname or func_name:
            formatter = dcrtd_frmtr
        else:
            formatter = Formatter(
                # '%(processName)s : %(process)s | %(threadName)s : %(thread)s:\n'
                '%(process)s - %(thread)s @ '
                '%(asctime)s {%(name)s:%(lineno)d  - %(funcName)18s()} %(levelname)s - %(message)s')
        # '[%(asctime)s] - {%(name)s:%(lineno)d  - %(funcName)20s()} - %(levelname)s - %(message)s')
        # fileHandler = TimedRotatingFileHandler(log_dir + '%s.log' % logger_name, mode='a')
        log_dir2use = log_dir + sep  # + logger_name + sep
        if not exists(log_dir2use):
            makedirs(log_dir2use)
        if l.handlers:
            l.handlers = []
        fileHandler = TimedRotatingFileHandler(log_dir2use + '%s.log' % logger_name)
        fileHandler.setFormatter(formatter)
        streamHandler = StreamHandler()
        streamHandler.setFormatter(formatter)

        l.setLevel(level)
        l.addFilter(MyFunctionLogFilter(func_name=func_name))
        l.addHandler(fileHandler)
        l.addHandler(streamHandler)
        loggers.update(dict(name=logger_name))

        return l

# def get_logger(logger_name=None, level=level):
#     global loggers
#     if loggers.get(logger_name):
#         return loggers.get(logger_name)
#     else:
#         logger_name = inspect.stack()[1][3].replace('<', '').replace('>', '') if not logger_name else logger_name
#         l = logging.getLogger(logger_name)
#         l.propagate = False
#         # formatter = logging.Formatter('%(asctime)s : %(message)s')     %(os.getpid())s|
#         formatter = logging.Formatter(
#             # '%(processName)s : %(process)s | %(threadName)s : %(thread)s:\n'
#             '%(process)s - %(thread)s @ '
#             '%(asctime)s {%(name)s:%(lineno)5d  - %(funcName)20s()} %(levelname)5s - %(message)s')
#         # '[%(asctime)s] - {%(name)s:%(lineno)d  - %(funcName)20s()} - %(levelname)s - %(message)s')
#         # fileHandler = TimedRotatingFileHandler(log_dir + '%s.log' % logger_name, mode='a')
#         log_dir2use = log_dir + os.sep + logger_name + os.sep
#         if not os.path.exists(log_dir2use): makedirs(log_dir2use)
#         if l.handlers:
#             l.handlers = []
#         fileHandler = TimedRotatingFileHandler(log_dir2use + '%s.log' % logger_name)
#         fileHandler.setFormatter(formatter)
#         streamHandler = logging.StreamHandler()
#         streamHandler.setFormatter(formatter)
#
#         l.setLevel(level)
#         l.addHandler(fileHandler)
#         l.addHandler(streamHandler)
#         loggers.update(dict(name=logger_name))
#
#         return l
