
#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urllib import request
from urllib.error import HTTPError
import sys
import json
import ssl
import cx_Oracle
import code
import os
import csv

class hlfdb:
  os.environ["NLS_LANG"] = os.getenv("NLS_LANG", ".AL32UTF8")
  os.environ['no_proxy'] = '*'

if __name__ == '__main__':

  # Instancio el cliente padr
  # user = os.getenv('PUC_DB_USER', 'puc')
  # passwd = os.getenv('PUC_DB_PASSWORD', 'escorpio1')
  # host = os.getenv('PUC_DB_HOST', '10.30.205.101')
  # port = os.getenv('PUC_DB_PORT', '1521')
  # sid = os.getenv('PUC_DB_SID', 'padr')
  # url = "%s:%s/%s"%(host, port, sid)

  print( "abot quering ...")
  
  user = 'HLF'
  passwd = 'HLF'
  url = 'localhost/xe'

  pool = cx_Oracle.SessionPool(user, passwd, url)

  query = """select * from hlf.bc_valid_tx_write_set"""

  print( query )

  i = 0
  
  try:
      cnx = pool.acquire()
      cur = cnx.cursor()
      print( cur.execute(query).fetchall() )

  finally:
      if cur is not None: cur.close()
      if cnx is not None: pool.release(cnx)
  
  
  print( "OK !!!" % i)



