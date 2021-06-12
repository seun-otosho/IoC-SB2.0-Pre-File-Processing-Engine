# coding: utf-8

# In[71]:


import pandas as pd
import requests
import xmltodict
from base64 import b64decode
from datetime import datetime
from os import mkdir, sep
from os.path import exists

# In[98]:
# api_access = 'strUserID=1A0L199999999903&strPassword=Brp1890' # mimi
api_access = 'strUserID=852886livere4&strPassword=Uqxnp888'  # test
# api_access = 'strUserID=852886livere4&strPassword=Uqxnp888' # test
# api_access = 'strUserID=910995unionb1&strPassword=Lhab1863' # union
gender = {
    'M': '001',
    'MALE': '001',
    'F': '002',
    'FEMALE': '002'
}

pdf_dir = datetime.now().strftime('%Y%b%d') + 'pdf' + datetime.now().strftime('%H')
if not exists(pdf_dir):
    mkdir(pdf_dir)
url = "https://webserver.creditreferencenigeria.net/crcweb/liverequestinvoker.asmx/PostRequest"

# In[43]:


df = pd.read_csv(
    'union_batch_cir-Copy.csv', dtype=str, keep_default_na=True, sep='|', thousands=',',
    encoding="ISO-8859-1")  # _xcptn

# In[50]:


# df.head()

# In[49]:


df['dob'] = df['DATE_OF_BIRTH'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').strftime('%d-%b-%Y'))


# In[35]:

#
# def testf(acno, name, dob, bvn):
#     rezstr = '|'.join((acno, name, dob, bvn))
#     return rezstr


# In[66]:


# df[df['lrezstr'].str.len() < 1000]


# In[96]:


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


# In[72]:


from json import loads, dumps
from collections import OrderedDict


def order3D2dict(input_ordered_dict):
    return loads(dumps(input_ordered_dict))


# In[90]:


def pdfRez(resdict):
    try:
        d = xmltodict.parse(resdict)
    except Exception as e:
        print(e)
        return True, resdict
    d = order3D2dict(d)
    d = d['string']['#text']
    if 'DATAPACKET' in d:
        return False, d
    else:
        return True, d


def call_live_request(filename, acno, name, dob, bvn, sex):
    name = filename if str(name) == 'nan' else name
    payload = """{}&strRequest={}""".format(api_access, combine_search(name, gender[sex.upper()], dob, acno, bvn))
    headers = {'content-type': "application/x-www-form-urlencoded"}
    response = requests.request("POST", url, data=payload, headers=headers)
    rez = pdfRez(response.text)

    if rez[0]:
        if rez[1] == response.text:
            rezstr = response.text[79:-9]
        else:
            rezstr = rez[1]
        with open("{}{}{}.pdf".format(pdf_dir, sep, filename), "wb") as fh:
            try:
                fh.write(b64decode(rezstr))
            except Exception as e:
                print(e)
    else:
        payload = """{}&strRequest={}""".format(api_access, bvn_search(bvn))
        response = requests.request("POST", url, data=payload, headers=headers)
        rez0 = pdfRez(response.text)
        if rez0[0]:
            if rez0[1] == response.text:
                rezstr = response.text[79:-9]
            else:
                rezstr = rez0[1]
            with open("{}{}{}.pdf".format(pdf_dir, sep, filename), "wb") as fh:
                try:
                    fh.write(b64decode(rezstr))
                except Exception as e:
                    print(e)
            return rez0[1]
        else:
            # merge multi hits
            decide_merge(rez0[1])

    print(str(datetime.now()), '*' * 8, name, filename)

    return rez[1]


def decide_merge(d):
    d = xmltodict.parse(d)
    d = order3D2dict(d)
    # merge report
    l = d['DATAPACKET']['BODY']['SEARCH-RESULT-LIST']['SEARCH-RESULT-ITEM']


# In[99]:


df['rezstr'] = df[
    ['ACCOUNTNAME', 'ACCOUNTNO', 'FULLNAME', 'dob', 'BVN', 'SEX']
].apply(lambda x: call_live_request(*x), axis=1)  # .head(1)

# for row in df.iterrows():
#     call_live_request(row['ACCOUNTNAME'], row['ACCOUNTNO'], row['FULLNAME'], row['dob'], row['BVN'])
# In[61]:


df['lrezstr'] = df['rezstr'].apply(lambda x: len(x))  # .head(1)

# In[68]:
'''

df


# In[46]:


# df.apply(lambda x: x['ACCOUNTNO'])


# In[ ]:





# In[9]:


payload = """strUserID=1A0L199999999903&strPassword=Brp1890&strRequest=<REQUEST REQUEST_ID="1">
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
</REQUEST>""".format(name, gender, dob, acno, bvn)

headers = {
    'content-type': "application/x-www-form-urlencoded"
    }


# In[10]:




for rw in df.head().iterrows():
    


# In[ ]:




'''
