#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cx_Oracle
import pandas as pd
import os
from wset import *

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

QUERY_WSET_BY_BLOCK = """
select * 
from hlf.bc_valid_tx_write_set 
where block = :block
order by txseq, item"""

QUERY_WSET_BY_KEYPATTERN = """
select *
from hlf.bc_valid_tx_write_set
where block < :block
and key like :keypattern
order by key, block desc"""
  
# Suppress SettingWithCopyWarning: 
# A value is trying to be set on a copy of a slice from a DataFrame.
# Try using .loc[row_indexer,col_indexer] = value instead
pd.options.mode.chained_assignment = None

def mk_persona_state( block: int, personaid: int ):
    keypattern = "per:" + str( personaid ) + "#%"
    res = db.queryall(QUERY_WSET_BY_KEYPATTERN, {"block" : block, "keypattern" : keypattern})
    return Wset(res).resolve_state().extend()  

def process_txpersona(block: int, txseq: int, personaid: int, changes: pd.DataFrame) -> list:
    # print( "processing block {} personaid {} ...".format( block, personaid ) )

    state = mk_persona_state(block, personaid)

    # Si el state no tiene datos jurisdiccionales lo vacia
    if not state.has_orgs(): state = Wset() 

    changes = Wset(changes).extend(state.get_df())

    txs = list()

    if  not state.has_orgs() and not changes.has_orgs(): return txs # sin datos jurisdiccionales

    print( "personaid {} con datos jurisdiccionales !!!".format( personaid ))
    if state.has_orgs():
       print( "state with orgs: {}".format( state.count_with_orgs() ) )
       print( state.get_df().loc[ getattr(state.get_df(), "orgs").notnull(), ["component_key", "orgs"]] )
    if changes.has_orgs():
       print( "changes with orgs: {}".format( changes.count_with_orgs() ) )
       print( changes.get_df().loc[ getattr(changes.get_df(), "orgs").notnull(), ["component_key", "orgs"]] )

    # TODO: Tener en cuenta que las rows puede ser DELETEs !!!!
    #
    # {"impuesto":11,"inscripcion":"2019-05-31","estado":"AC","dia":1,"periodo":201905,"motivo":{"id":44},"ds":"2019-05-31"}
    #
    # MIGRACION
    # - no tiene del
    # - tiene imp con org X y estado AC 
    # - no tiene state con org X
    # - tiene dom con org == component_key[0] # Domicilio migrado
    # accion:
    # - una tx { imp.org, MIGRACION }
    # - una tx por cada jur { jur.org, MIGRACION_DESDE_CM }
    #
    changes_impuesto = changes.get_impuesto()
    state_impuesto = None if changes_impuesto is None else state.get_impuesto( org=changes_impuesto["org"] )

    # MIGRACION | INSCRIPCION
    if  not changes.has_deletes() \
    and not changes_impuesto is None \
    and changes_impuesto["estado"] == "AC" \
    and state_impuesto is None:
        org = changes_impuesto["org"]
        tx = "MIGRACON" if changes.has_domicilios( org=org ) else "INSCRIPCION"
        txs.append( {"org" : org, "tx" : tx} )
        if org == 900:
           tx += " DESDE CM"
           for jur in changes.get_jurisdicciones(): 
               txs.append( { "org" : jur["org"], "tx" : tx} )
        return txs
    
    # REINSCRIPCION
    if  not changes_impuesto is None \
    and not state_impuesto is None \
    and changes_impuesto["estado"] == "AC" \
    and state_impuesto["estado"] != "AC":
        tx = "REINSCRIPCION"
        org = changes_impuesto["og"]
        txs.append( {"org" : org, "tx" : tx} )
        if org == 900:
           tx += " DESDE CM"
           for jur in changes.get_jurisdicciones(): 
               if jur.get( "hasta", None ) is None:
                  txs.append( { "org" : jur["org"], "tx" : tx} )
        return txs
    
    # ?????
    return txs

USER = 'HLF'
PASSW = 'HLF'
URLDB = 'localhost/xe'

###################################################################    

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
  block = 53410

  res = db.queryall( QUERY_WSET_BY_BLOCK, { "block" : block } )

  if len(res) == 0: 
     print( "block {} does not exists or empty".format( block ) )
     quit()

  for name, group in Wset(res).groupby_tx_personaid(): 
      txs = process_txpersona(block, name[0], name[1], group)
      if len(txs) > 0:
         print(name)
         print(txs)

