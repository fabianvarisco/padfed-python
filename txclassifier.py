#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import cx_Oracle
import pandas as pd
import os

# Oracle Data access
class db_access:

  def __init__(self, user = 'HLF', passwd = 'HLF', url = 'localhost/xe'):
    self.pool = cx_Oracle.SessionPool(user, passwd, url)
  
  def queryall(self, query: str,  vars: dict):
    try:
      cnx = self.pool.acquire()
      cur = cnx.cursor()
      return cur.execute(query, vars).fetchall()
    finally:
      if cur is not None: cur.close()
      if cnx is not None: self.pool.release(cnx)

  def queryone(self, query: str,  vars: dict):
    try:
      cnx = self.pool.acquire()
      cur = cnx.cursor()
      return cur.execute(query, vars).fetchone()
    finally:
      if cur is not None: cur.close()
      if cnx is not None: self.pool.release(cnx)

QUERY_BLOCK_WSET = """
select * 
from hlf.bc_valid_tx_write_set 
where block = :block
order by txseq, item"""

QUERY_KEYPATTERN_WSET = """
select *
from hlf.bc_valid_tx_write_set
where block < :block
and key like :keypattern
order by key, block desc"""

def mk_wsetdf( res ):
  df = pd.DataFrame(res, columns=['block', 'txseq', 'item', 'key', 'value', 'isdelete']) 
  new = df["key"].str.split("#", n=1, expand=True)
  personaid = new[0].str.split(":", n=1, expand=True)
  component = new[1].str.split(":", n=1, expand=True)
  df["personaid"] = personaid[1]
  df["componentid"] = component[0]
  df["componentvalue"] = component[1] 
  return df

def get_persona_state( block: int, personaid: int ):
  keypattern = "per:" + personaid + "#%"
  res = db.queryall( QUERY_KEYPATTERN_WSET, { "block" : block, "keypattern" : keypattern } )
  if len(res) == 0: return pd.DataFrame() # Empty DataFrame
  return mk_wsetdf( res ).groupby( ['key'] ).first()

def get_group( groups, key: str ):
  try:
    return groups.get_group( key )
  except:
    return pd.DataFrame() # Empty DataFrame

def process_txpersona( block: int, txseq: int, personaid: int, wsetdf: pd.DataFrame ):
  # print( "processing block {} personaid {} ...".format( block, personaid ) )

  state = get_persona_state( block, personaid )
  if len(state) > 0:
     st_components = state.groupby( ['componentid'] )
     st_per  = get_group( st_components, 'per' )
     st_wit  = get_group( st_components, 'wit' )
     st_imps = get_group( st_components, 'imp' )
     st_jurs = get_group( st_components, 'jur' )
     st_acts = get_group( st_components, 'act' )
     st_doms = get_group( st_components, 'dom' )
     st_dors = get_group( st_components, 'dor' )
     st_cmss = get_group( st_components, 'cms' )
     st_rels = get_group( st_components, 'rel' )
  else:
     st_per = st_wit = st_imps = st_jurs = st_acts = st_doms = st_dors = st_cmss = st_rels = pd.DataFrame()
  
  print( "personaid {} st: wit {} per {} imps {} jurs {} acts {} doms {} dors {} cmss {} rels {} ".format( 
          personaid, 
          len(st_wit), len(st_per), len(st_imps), len(st_jurs), 
          len(st_acts), len(st_doms), len(st_dors), len(st_cmss), len(st_rels)) )

  components = wsetdf.groupby( ['componentid'] )
  per  = get_group( components, 'per' )
  wit  = get_group( components, 'wit' )
  imps = get_group( components, 'imp' )
  jurs = get_group( components, 'jur' )
  acts = get_group( components, 'act' )
  doms = get_group( components, 'dom' )
  dors = get_group( components, 'dor' )
  cmss = get_group( components, 'cms' )
  rels = get_group( components, 'rel' )

  print( "personaid {} tx: wit {} per {} imps {} jurs {} acts {} doms {} dors {} cmss {} rels {} ".format( 
          personaid, 
          len(wit), len(per), len(imps), len(jurs), 
          len(acts), len(doms), len(dors), len(cmss), len(rels)) )


USER = 'HLF'
PASSW = 'HLF'
URLDB = 'localhost/xe'

if __name__ == '__main__':

  # user = os.getenv('PUC_DB_USER', 'puc')
  # passwd = os.getenv('PUC_DB_PASSWORD', 'escorpio1')
  # host = os.getenv('PUC_DB_HOST', '10.30.205.101')
  # port = os.getenv('PUC_DB_PORT', '1521')
  # sid = os.getenv('PUC_DB_SID', 'padr')
  # url = "%s:%s/%s"%(host, port, sid)

  print( "running ...")

  db = db_access(USER, PASSW, URLDB)

  block = 53319

  res = db.queryall( QUERY_BLOCK_WSET, { "block" : block } )

  if len(res) == 0: exit ## Empty block

  groups = mk_wsetdf( res ).groupby(['txseq', 'personaid'])

  for name, group in groups: process_txpersona( block, name[0], name[1], group )

