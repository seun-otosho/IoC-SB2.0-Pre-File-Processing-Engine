# coding: utf-8

# In[71]:


import inspect
import logging
import os
from base64 import b64decode
from concurrent import futures
from datetime import datetime
from json import loads, dumps
from logging.handlers import TimedRotatingFileHandler
from os import makedirs, mkdir, sep
from os.path import exists
from time import sleep

import pandas as pd
import requests
import xmltodict
from fuzzywuzzy import fuzz

# from time import sleep

level = logging.INFO
loggers = {}

# test = 'strUserID=1A0L199999999903&strPassword=Abcd1234'  # mimi
# test = 'strUserID=852886livere4&strPassword=Uqxnp888'
test = 'strUserID=910995unionb1&strPassword=Lhab1863'
gender = {
    'M': '001',
    'MALE': '001',
    'F': '002',
    'FEMALE': '002'
}
pdf_txt = datetime.now().strftime('%Y%b%d') + 'pdf' + datetime.now().strftime('%H')
pdf_dir = 'reports' + sep + pdf_txt

if not exists(pdf_dir):
    mkdir(pdf_dir)
url = "https://webserver.creditreferencenigeria.net/crcweb/liverequestinvoker.asmx/PostRequest"

log_dir = pdf_dir + sep + 'logs' + sep

df = pd.read_csv(
    'union_batch_cir.csv', dtype=str, keep_default_na=True, sep='|', thousands=',', encoding="ISO-8859-1")  # _xcptn

df['DOB'].fillna('01/01/1900', inplace=True)


def dob2s(x):
    try:
        return datetime.strptime(str(x), '%m/%d/%Y').strftime('%d-%b-%Y')
    except Exception as e:
        return datetime.strptime(str(x), '%d/%m/%Y').strftime('%d-%b-%Y')


df['dob'] = df['DOB'].apply(lambda x: dob2s(x))


# def file_logger(acctname):
#     logger_txt = get_logger(pdf_txt)
#     with open(pdf_txt, "a") as myfile:
#         try:
#             myfile.write(acctname + '\n')
#         except Exception as e:
#             sleep(10)
#             myfile.write(acctname + '\n')


def get_logger(logger_name=None, level=level, mini=False):
    global loggers
    if loggers.get(logger_name):
        return loggers.get(logger_name)
    else:
        logger_name = inspect.stack()[1][3].replace('<', '').replace('>', '') if not logger_name else logger_name
        l = logging.getLogger(logger_name)
        l.propagate = False
        # formatter = logging.Formatter('%(asctime)s : %(message)s')     %(os.getpid())s|
        
        if mini:
            formatter = logging.Formatter('%(message)s')
        else:
            formatter = logging.Formatter(
                # '%(processName)s : %(process)s | %(threadName)s : %(thread)s:\n'
                '%(process)s - %(thread)s @ '
                '%(asctime)s {%(name)30s:%(lineno)5d  - %(funcName)23s()} %(levelname)s - %(message)s')
        # '[%(asctime)s] - {%(name)s:%(lineno)d  - %(funcName)20s()} - %(levelname)s - %(message)s')
        # fileHandler = TimedRotatingFileHandler(log_dir + '%s.log' % logger_name, mode='a')
        log_dir2use = log_dir + os.sep  # + logger_name + os.sep
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


def combine_search(name, gender, dob, acno, bvn):
    # name = ' '.join([t for t in name.replace('.', ' ').split() if len(t) > 3])
    return '''<REQUEST REQUEST_ID="1">
        <REQUEST_PARAMETERS>
            <REPORT_PARAMETERS RESPONSE_TYPE="2" SUBJECT_TYPE="1" REPORT_ID="6110"/>
            <INQUIRY_REASON CODE="1"/>
            <APPLICATION CURRENCY="NGN" AMOUNT="0" NUMBER="232" PRODUCT="017"/>
        </REQUEST_PARAMETERS>
        <SEARCH_PARAMETERS SEARCH-TYPE="6">
            <NAME>{}</NAME>
            <SURROGATES>
                <GENDER VALUE="{}"/>
                <DOB VALUE="{}"/>
            </SURROGATES>
            <ACCOUNT NUMBER="{}"/>
            <BVN_NO>{}</BVN_NO>
            <TELEPHONE_NO></TELEPHONE_NO>
        </SEARCH_PARAMETERS>
    </REQUEST>'''.format(name, gender, dob, acno, bvn)


# In[97]:


def bvn_search(bvn):
    return '''<REQUEST REQUEST_ID="1">
        <REQUEST_PARAMETERS>
            <REPORT_PARAMETERS RESPONSE_TYPE="2" SUBJECT_TYPE="1" REPORT_ID="6110"/>
            <INQUIRY_REASON CODE="1"/>
            <APPLICATION CURRENCY="NGN" AMOUNT="0" NUMBER="0" PRODUCT="017"/>
        </REQUEST_PARAMETERS>
        <SEARCH_PARAMETERS SEARCH-TYPE="4">
            <BVN_NO>{}</BVN_NO>
        </SEARCH_PARAMETERS>
    </REQUEST>'''.format(bvn)


def merge_search(ref, ruids):
    if len(ruids) >= 2:
        mrgstr = """<BUREAU_ID>{}</BUREAU_ID>""".format(ruids[1])
        for r in ruids[2:]:
            try:
                mrgstr += """<BUREAU_ID>{}</BUREAU_ID>""".format(r)
            except Exception as e:
                logger = get_logger(ref)
                logger.error(e)
        return '''<REQUEST REQUEST_ID="1">
                    <REQUEST_PARAMETERS>
                        <REPORT_PARAMETERS REPORT_ID="6110" SUBJECT_TYPE="1"  RESPONSE_TYPE="2"/>
                        <INQUIRY_REASON CODE="1" />
                        <APPLICATION PRODUCT="017" NUMBER="12345" AMOUNT="0" CURRENCY="NGN"/>
                        <REQUEST_REFERENCE REFERENCE-NO="{}">
                            <MERGE_REPORT PRIMARY-BUREAU-ID="{}">
                                <BUREAU_ID>{}</BUREAU_ID>
                                {}
                            </MERGE_REPORT>
                        </REQUEST_REFERENCE>
                    </REQUEST_PARAMETERS>
                </REQUEST>'''.format(ref, ruids[0], ruids[0], mrgstr)
    else:
        return '''<REQUEST REQUEST_ID="1">
                    <REQUEST_PARAMETERS>
                        <REPORT_PARAMETERS REPORT_ID="6110" SUBJECT_TYPE="1"  RESPONSE_TYPE="2"/>
                        <INQUIRY_REASON CODE="1" />
                        <APPLICATION PRODUCT="017" NUMBER="12345" AMOUNT="0" CURRENCY="NGN"/>
                        <REQUEST_REFERENCE REFERENCE-NO="{}">
                            <MERGE_REPORT PRIMARY-BUREAU-ID="{}">
                                <BUREAU_ID>{}</BUREAU_ID>
                            </MERGE_REPORT>
                        </REQUEST_REFERENCE>
                    </REQUEST_PARAMETERS>
                </REQUEST>'''.format(ref, ruids[0], ruids[0])


def order3D2dict(input_ordered_dict):
    return loads(dumps(input_ordered_dict))


def pdfRez(acname, resdict, x):
    logger = get_logger(acname)
    try:
        d = xmltodict.parse(resdict)
    except Exception as e:
        logger.info(e)
        return True, resdict
    d = order3D2dict(d)
    d = d['string']['#text']
    
    if 'ERROR-CODE' in d:
        ec = xmltodict.parse(d)['DATAPACKET']['BODY']['ERROR-LIST']['ERROR-CODE']
        logger.error(dumps(order3D2dict(xmltodict.parse(d)), indent=4))
        return None, ec
    
    if 'DATAPACKET' in d:
        return False, d
    else:
        return True, d


def call_live_request_dict(kwargs):
    acno, acname, bvn, fn, x = kwargs['ACCTNO'], kwargs['ACCOUNTNAME'], kwargs['BVN'], kwargs['FULLNAME'], kwargs['x']
    if str(fn).strip() == '':
        fn = acname
    logger = get_logger(acname)
    if 'i' not in kwargs:
        kwargs['i'] = 0
    logger.info(dumps(kwargs, indent=4))
    
    payload = """{}&strRequest={}""".format(
        test, combine_search(fn, gender.get(kwargs['SEX'].strip().upper(), '001'), kwargs['dob'], acno, bvn))
    headers = {'content-type': "application/x-www-form-urlencoded"}
    logger.info(
        "{} {}".format('^' * 55, "\ncombined search request sent for {}\nrequest payload is\n{}".format(fn, payload)))
    response = requests.request("POST", url, data=payload, headers=headers)
    if 'ERROR' in response.text and 'CODE' in response.text:
        logger.error(response.text)
    else:
        rez = pdfRez(acname, response.text, x)
        
        if rez[0]:
            if rez[1] == response.text:
                rezstr = response.text[79:-9]
            else:
                rezstr = rez[1]
            with open("{}{}{}.pdf".format(pdf_dir, sep, acname), "wb") as fh:
                try:
                    fh.write(b64decode(rezstr))
                    logger_txt.info(acname)
                    logger.info("{} {}".format("file {}.pdf written to disk".format(acname), '#' * 55))
                except Exception as e:
                    logger.info(e)
            return rez[1]
        else:
            # sleep(5)
            # payload = """{}&strRequest={}""".format(test, bvn_search(bvn))
            # logger.info("BVN search request re-sent for {} using {}\nrequest payload is\n{}".format(fn, bvn, payload))
            # response = requests.request("POST", url, data=payload, headers=headers)
            # rez0 = pdfRez(acname, response.text, x)
            # if rez0[0]:
            #     if rez0[1] == response.text:
            #         rezstr = response.text[79:-9]
            #     else:
            #         rezstr = rez0[1]
            #     with open("{}{}{}.pdf".format(pdf_dir, sep, acname), "wb") as fh:
            #         try:
            #             fh.write(b64decode(rezstr))
            #             logger_txt.info(acname)
            #             logger.info("{} {}".format("file {}.pdf written to disk".format(acname), '#' * 55))
            #         except Exception as e:
            #             logger.info(e)
            #     return rez0[1]
            # else:
            # merge multi hits
            # logger.info(
            #     dumps(order3D2dict(xmltodict.parse(order3D2dict(xmltodict.parse(response.text))['string']['#text'])),
            #           indent=4))
            ref, ruids = decide_merge(kwargs, rez[1])
            payload = """{}&strRequest={}""".format(test, merge_search(ref, ruids))
            logger.info(
                "merged report spool request sent for {} using {}\nrequest payload is\n{}".format(
                    fn, ', '.join(ruids), payload))
            response = requests.request("POST", url, data=payload, headers=headers)
            # if '-3000' in response.text:
            
            rez1 = pdfRez(acname, response.text, x)
            
            if rez1[0]:
                if rez1[1] == response.text:
                    rezstr = response.text[79:-9]
                else:
                    rezstr = rez1[1]
                with open("{}{}{}.pdf".format(pdf_dir, sep, acname), "wb") as fh:
                    try:
                        fh.write(b64decode(rezstr))
                        logger_txt.info(acname)
                        logger.info("{} {}".format("file {}.pdf written to disk".format(acname), '#' * 55))
                    except Exception as e:
                        logger.error(e)
            # # added this for those that fail repeated with error. ..
            else:
                # logger.info('\n\n\n\n\nwill recursive in 5 seconds for \n{}'.format(kwargs))
                # sleep(5)
                # while kwargs['i'] < 5:
                #     kwargs['i'] += 1
                #     try:
                #         call_live_request_dict(kwargs)
                #     except Exception as e:
                #         logger.error(e)
                logger.warn(response.text)
        return rez[1]
    return None


def decide_merge(reqdict, d):
    logger = get_logger(reqdict['ACCOUNTNAME'])
    d, x = xmltodict.parse(d), reqdict['x']
    d = order3D2dict(d)
    ld, ll, ua = {}, [], []
    ref = d['DATAPACKET']['@REFERENCE-NO']
    l = d['DATAPACKET']['BODY']['SEARCH-RESULT-LIST']['SEARCH-RESULT-ITEM']
    prev_max_name, prev_max_dob, pruid, ruids = None, None, None, []
    for n, i in enumerate(l):
        logger.info(dumps(i, indent=4))
        try:
            if fuzz.partial_ratio(i['@PHONE-NUMBER'], reqdict['phone']) >= 95:
                ruids.append(i['@BUREAU-ID'])
        except Exception as e:
            logger.error(e)
        dob_ratio = fuzz.partial_ratio(i['@DATE-OF-BIRTH'], reqdict['dob'])
        
        try:
            ld[str(dob_ratio)] = i['@BUREAU-ID']
        except Exception as e:
            logger.error(e)
        
        # if len(ll) == 0:
        #     if any(li == dob_ratio for li in ll):
        ua.append(i['@BUREAU-ID'])
        try:
            if dob_ratio >= 64:
                ll.append(dob_ratio)
        except Exception as e:
            logger.error(e)
        
        logger.info("{} compared with {} is {}".format(i['@DATE-OF-BIRTH'], reqdict['dob'], dob_ratio))
        
        if dob_ratio == 100:
            pruid = i['@BUREAU-ID']
        if dob_ratio >= 91 and pruid is None:
            pruid = i['@BUREAU-ID']
        if dob_ratio == 82 and pruid is None:
            pruid = i['@BUREAU-ID']
        
        if (dob_ratio == 100
                or dob_ratio >= 91
                or dob_ratio in (82, 64, 55)):
            ruids.append(i['@BUREAU-ID'])
    
    if pruid is None:
        pruidi = max(ll)
        pruid = ld[str(pruidi)]
    
    if len(ruids) < 2:
        if len(ua) > 3:
            ruids = ll.sort(reverse=True)[:3]
        else:
            ruids = ua
    
    ruids = set(ruids)
    
    if pruid is not None:
        while pruid in ruids:
            ruids.remove(pruid)
    
    ruids = tuple([pruid] + list(set(ruids)))
    
    logger.info(ref)
    logger.info(ruids)
    return ref, ruids


# df['rezstr'] = df[
#     ['ACCOUNTNAME', 'ACCTNO', 'FULLNAME', 'dob', 'BVN', 'SEX']
# ].apply(lambda x: call_live_request(*x), axis=1)  # .head(1)
df['x'] = df.index
df.fillna('', inplace=True)
df_dict_list = df[['ACCOUNTNAME', 'ACCTNO', 'FULLNAME', 'dob', 'BVN', 'SEX', 'phone', 'x']].to_dict('records')
# pool = mp.Pool(processes=100)
# max_workers=1, pool.map(call_live_request_dict, df_dict_list)
logger_txt = get_logger(pdf_txt, level, True)
with futures.ThreadPoolExecutor(thread_name_prefix='BatchLiveRequest') as executor:  # max_workers=100
    executor.map(call_live_request_dict, df_dict_list)

# for row in df.iterrows():
#     call_live_request(row['ACCOUNTNAME'], row['ACCTNO'], row['FULLNAME'], row['dob'], row['BVN'])
# In[61]:


# df['lrezstr'] = df['rezstr'].apply(lambda x: len(x))  # .head(1)
