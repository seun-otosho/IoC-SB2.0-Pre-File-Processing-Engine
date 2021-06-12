#!/usr/bin/python
# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.sql import text

scores_tns = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.1.16)(PORT = 1521)) '
scores_tns += '(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = crcpdb01) ) )'
scores_con = create_engine("oracle+cx_oracle://scores:scores@" + scores_tns)

sb2pub_tns = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.1.20)(PORT = 1521)) '
sb2pub_tns += '(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = sb2prod) ) )'
sb2pub_con = create_engine("oracle+cx_oracle://sbapi:sbapi@" + sb2pub_tns)


"""

we need to run a number of scripts in specified sequence
we need to keep track of start time, end time and number of rows affected for each script
then we need to call the next scripts after each previous script till we finish

need to track:
    start time
    drop time
    create time
    finished time

3 buckets:
    1 hist buckets and dependencies
    2 positional bucket and dependencies
    3 others bucket

"""

with scores_con.connect() as con:

    con.execute(text('DROP TABLE IF EXISTS Cars'))
    con.execute(text('''CREATE TABLE Cars(Id INTEGER PRIMARY KEY, 
                 Name TEXT, Price INTEGER)'''))