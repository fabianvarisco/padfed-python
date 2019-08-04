
#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urllib import request
import sys
import json
import cx_Oracle
import code
import os
import csv

USER = 'HLF'
PASSWORD = 'HLF'
URL = 'localhost/xe'

INSERT="""
INSERT INTO HLF.BC_VALID_TX_WRITE_SET(
BLOCK,
TXSEQ,
ITEM,
KEY,
VALUE,
IS_DELETE)
VALUES (
:block,
:txseq,
:item,
:key,
:value,
:is_delete)"""

try:
    os.environ["NLS_LANG"] = os.getenv("NLS_LANG", ".AL32UTF8")
    os.environ['no_proxy'] = '*'

    POOL = cx_Oracle.SessionPool(USER, PASSWORD, URL)
    CNX = POOL.acquire()
    CUR = CNX.cursor()
  
    with open("writeset-4.txt", "r", encoding="ISO-8859-1") as f: 
        reader = csv.reader(f)
        i = 0
        for row in reader:
            print( "row %d" % i )
            CUR.execute(INSERT, ( row[0], row[1], row[2], row[3], row[4], row[5] ) )
            i = i + 1
            if (i % 100) == 0: CUR.execute("COMMIT");  

    CUR.execute("COMMIT")
finally:
    if CUR is not None: 
        CUR.execute("ROLLBACK")
        CUR.close()
        POOL.release(CNX)
 
print( "%d rows - OK !!!" % i)
