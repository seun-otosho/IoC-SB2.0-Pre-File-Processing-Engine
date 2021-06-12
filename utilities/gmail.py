#!/usr/bin/env python
#
# Very basic example of using Python 3 and IMAP to iterate over emails in a
# gmail folder/label.  This code is released into the public domain.
#
# This script is example code from this blog post:
# http://www.voidynullness.net/blog/2013/07/25/gmail-email-with-python-via-imap/
#
# This is an updated version of the original -- modified to work with Python 3.4.
#
import sys
import imaplib
import getpass
import email
import email.header
import datetime

from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(timeout=100, max_retries=10, retry_on_timeout=True)

EMAIL_ACCOUNT = "support@crccreditbureau.com"

# Use 'INBOX' to read inbox.  Note that whatever folder is specified,
# after successfully running this script all emails in that folder
# will be marked as read.
EMAIL_FOLDER = "INBOX"


def process_mailbox(M):
    """
    Do something with emails messages in the folder.
    For the sake of this example, print some headers.
    """

    # rv, data = M.search(None, "ALL")
    rv, data = M.search(None, '(SINCE "30-Sep-2017")')
    if rv != 'OK':
        print("No messages found!")
        return

    for num in data[0].split():
        body = {}
        rv, data = M.fetch(num, '(RFC822)')
        if rv != 'OK':
            print("ERROR getting message", num)
            return

        msg = email.message_from_bytes(data[0][1])
        hdr = email.header.make_header(email.header.decode_header(msg['Subject']))
        frm = email.header.make_header(email.header.decode_header(msg['From']))
        to = email.header.make_header(email.header.decode_header(msg['To']))
        _id = email.header.make_header(email.header.decode_header(msg['Message-ID']))
        body['subject'], body['from'], body['to'] , body['_index'] = str(hdr), str(frm), str(to), 'crc_support'
        try:
            bcc = email.header.make_header(email.header.decode_header(msg['Bcc']))
            body['bcc'] = str(bcc)
        except Exception as e:
            print(e)
        try:
            cc = email.header.make_header(email.header.decode_header(msg['CC']))
            body['cc'] = str(cc)
        except Exception as e:
            print(e)

        try:
            mailThread = email.header.make_header(email.header.decode_header(msg['Thread-Topic']))
            body['thread_topic'] = str(mailThread)
        except Exception as e:
            print(e)

        try:
            threadIndex = email.header.make_header(email.header.decode_header(msg['Thread-Index']))
            body['thread_index'] = str(threadIndex)
        except Exception as e:
            print(e)

        # subject = str(hdr)
        # print('Message %s: %s' % (num, subject))
        # print('Raw Date:', msg['Date'])
        # Now convert to local date-time
        date_tuple = email.utils.parsedate_tz(msg['Date'])
        if date_tuple:
            local_date = datetime.datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
            # print("Local Date:", local_date.strftime("%a, %d %b %Y %H:%M:%S"))
            body['date'] = local_date

        body['_id'], body['_type'] , body['mail_body'] = str(_id).split('@')[0][1:], 'mails', str(get_text(msg))

        helpers.bulk(es, (body))
        # es.index(index='crc_support',, doc_type='mails', body=body)


def get_text(msg):
    if msg.is_multipart():
        return get_text(msg.get_payload(0))
    else:
        return str(msg.get_payload(None, True))


M = imaplib.IMAP4_SSL('imap.gmail.com')

try:
    rv, data = M.login(
        EMAIL_ACCOUNT,
        'POrtSSup20!4'
        # getpass.getpass()
    )
except imaplib.IMAP4.error:
    print("LOGIN FAILED!!! ")
    sys.exit(1)

print(rv, data)

rv, mailboxes = M.list()
if rv == 'OK':
    print("Mailboxes:")
    print(mailboxes)

rv, data = M.select(EMAIL_FOLDER)
if rv == 'OK':
    print("Processing mailbox...\n")
    process_mailbox(M)
    M.close()
else:
    print("ERROR: Unable to open mailbox ", rv)

M.logout()
