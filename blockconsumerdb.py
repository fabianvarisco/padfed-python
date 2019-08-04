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
  
  user = 'adminbd'
  passwd = 'adminbd'
  url = '10.30.205.21/blchain'

  pool = cx_Oracle.SessionPool(user, passwd, url)

  query = """select * 
  from hlf.bc_valid_tx_write_set 
  where block between 50000 and 54000 
  and key like '%s'""" % "per:%"

  print( query )

  i = 0
  
  try:
      cnx = pool.acquire()
      cur = cnx.cursor()
      cur.execute(query)
      cur.arraysize = 100

      f=open("writeset-4.txt","w+", newline='')
      cvsw = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
      for row in cur:
           cvsdata = [ row[0], row[1] , row[2], row[3] , row[4], row[5] ]
           cvsw.writerow( cvsdata )
           i = i + 1

  finally:
      if cur is not None: cur.close()
      if cnx is not None: pool.release(cnx)
      if f is not None: f.close()
  
  print( "%d rows - OK !!!" % i)



