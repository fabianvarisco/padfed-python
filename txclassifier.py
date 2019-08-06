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

EMPTY_DATAFRAME = pd.DataFrame()

# TODO: Agregar todos los impuestos
# Incluyendo los municipales
DICT_IMP_ORG = {
  "5900": 900,
  "5901": 901,
  "5902": 902
}

# Retorna un array xq un dom puede tener mas de una org (roles de disintas orgs)
def resolve_orgs( idkey: str, idval: str, value: str ):
  org = 0
  if   idkey == 'cms': 
       org = 900
  elif idkey == 'imp': 
       org = DICT_IMP_ORG.get( idval, 0 )
  elif idkey == 'con': 
       org = DICT_IMP_ORG.get( idval.split('.')[0], 0 )
  elif idkey in [ 'jur', 'act', 'dom', 'rol' ]: 
       org = int( idval.split('.')[0] )
  
  if org > 1: return [ org ]

  if idkey in [ 'dom', 'rol' ]: 
     # TODO: si idKey comienza con 1 puede 
     # relacionarse con mas de una org
     # hay que buscar en el state
     # por ahora retorna []
     return []

     # Propuesta: 
     # - dor: cambiar la key: ORG.DOM_ORG.TIPO.ORG.ROL
     # - agregar columna T_JURISDICCION_ROL.ID_AT_ROL (para identificar la org del rol) 
  return [ ]


# Suppress SettingWithCopyWarning: 
# A value is trying to be set on a copy of a slice from a DataFrame.
# Try using .loc[row_indexer,col_indexer] = value instead
pd.options.mode.chained_assignment = None

def add_org( df: pd.DataFrame, state: pd.DataFrame ):

  if len( df ) == 0: return df

  # return df.assign(org=lambda row: resolve_orgs( row.componentid, 
  #                                                row.componentvalue, 
  #                                                row.value,
  #                                                state ),
  #                                                axis=1 ) 
 
  df['org'] = df.apply(lambda row: resolve_orgs( row.componentid, 
                                                 row.componentvalue, 
                                                 row.value ), 
                                                 axis=1 ) 
  # agrega org
  # desde actiidad, domicilio, impuesto, cms
  # bien dificil !!!
  return df

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
  if len(res) == 0: return EMPTY_DATAFRAME
  return mk_wsetdf( res ).groupby( ['key'] ).first()

def process_txpersona( block: int, txseq: int, personaid: int, changes: pd.DataFrame ):
  # print( "processing block {} personaid {} ...".format( block, personaid ) )

  state = get_persona_state( block, personaid )

  changes = add_org( changes, state )
 
  if len( changes.query( 'componentid == "imp" & componentvalue  >= "5000"' ) ) > 0:
     print("personaid {} con impuesto jurisdiccional !!!".format( personaid ))
     print( changes ) 

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
  block = 53401

  res = db.queryall( QUERY_BLOCK_WSET, { "block" : block } )

  if len(res) == 0: exit ## Empty block

  groups = mk_wsetdf( res ).groupby(['txseq', 'personaid'])

  for name, group in groups: process_txpersona( block, name[0], name[1], group )

