#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cx_Oracle
import pandas as pd
import os
from wset import *
from organizaciones import *

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

def txs_append(txs: list, org: int, txt: str): txs.append({"org": org, "tx": txt})

def mk_persona_state( block: int, personaid: int ):
    keypattern = "per:" + str( personaid ) + "#%"
    res = db.queryall(QUERY_WSET_BY_KEYPATTERN, {"block" : block, "keypattern" : keypattern})
    return Wset(res).resolve_state().extend()  

def inscripcion_alta(state_impuesto: dict, changes_impuesto: dict) -> bool:

    if  state_impuesto.get("estado", "") == "AC" \
    and len(changes_impuesto) == 0: 
        return True

    if  changes_impuesto.get("estado", "") == "AC" \
    and changes_impuesto.get("isdelete", "") != "T":
        return True
    
    return False   

def txs_by_component(txs: list, org: int, name: str, components: list, extrict: bool = False):
    if len(components) == 0: return
    
    if not extrict:
       txs_append(txs, org, "CAMBIO EN {}".format(name))
       return
      
    # Extrict Mode
    for c in components:
        if c.get("org", -1) == org: 
           txs_append(txs, org, "CAMBIO EN {}".format(name))
           return

def txs_by_persona(state_persona, changes_persona):
    if len(state_persona) == 0 or len(changes_persona) == 0: return
    # TODO: buscar cambios en razon social, nombre, apellido, estadoid
    return 

def txs_by_org(state: Wset, changes: Wset, org: int) -> list:
    txs = list()

    state_impuesto   = state.get_impuesto_by_org(org)
    changes_impuesto = changes.get_impuesto_by_org(org)

    if inscripcion_alta(state_impuesto, changes_impuesto):

       # MIGRACION o INSCRIPCION
       if len(state_impuesto) == 0: # No estaba inscripto
          txt = "MIGRACON" if changes.from_migration(org) else "INSCRIPCION"
          txs_append(txs, org, txt)
          if org == COMARB:
             txt += " DESDE CM"
             for j in changes.get_jurisdicciones(): 
                 txs_append(txs, j.get("org"), txt)
          return txs

       # REINSCRIPCION
       if len(state_impuesto) > 0 and state_impuesto.get("estado", "") != "AC":
          txt = "REINSCRIPCION"
          txs_append(txs, org, txt)
          if org == COMARB:
             txt += " DESDE CM"
             for j in changes.get_jurisdicciones(): 
                 if j.get("hasta", None) is None:
                    txs_append(txs, j.get("org"), txt)
          return txs
    
       if org == COMARB:
          # CAMBIO DE JURISDICCION CM
          rows = changes.get_jurisdicciones()
          if len(rows) > 0:
             txs_append(txs, COMARB, "CAMBIO JURISDICCION CM")
             for j in rows:
                 txs_append(txs, j.get("org"), "CAMBIO JURISDICCION CM")
 
          # CAMBIO DE SEDE CM
          rows = changes.get_cmsedes()
          if len(rows) > 0:
             txs_append(txs, COMARB, "CAMBIO DE SEDE CM")
             for s in rows:
                 s_org = get_org_by_provincia(s.get("provincia"))
                 if s_org != -1: txs_append(txs, s_org, "CAMBIO DE SEDE CM")

    txs_by_persona(  txs, changes.get_persona())
    txs_by_component(txs, org, "IMPUESTO",  changes.get_impuestos(), extrict=True)
    txs_by_component(txs, org, "ACTIVIDAD", changes.get_actividades())
    txs_by_component(txs, org, "DOMICILIO", changes.get_domicilios()) # TODO: Verificar si el dom tiene rol jurisdiccional
    txs_by_component(txs, org, "RELACION",  changes.get_relaciones())
    txs_by_component(txs, org, "EMAIL",     changes.get_emails())
    txs_by_component(txs, org, "TELEFONO",  changes.get_relaciones()) 
    txs_by_component(txs, org, "DOMIROL",   changes.get_domiroles()) 

    # TODO: Detectar cambio de socio !!!!!!!!
    return txs

def process_txpersona(block: int, txseq: int, personaid: int, changes: pd.DataFrame) -> list:
    # print( "processing block {} personaid {} ...".format( block, personaid ) )

    state = mk_persona_state(block, personaid)

    # Si el state no tiene datos jurisdiccionales lo vacia
    if not state.has_orgs(): state = Wset() 

    changes = Wset(changes).extend()

    changes.reduce(state)

    txs = list()

    for org in state.get_orgs().union(changes.get_orgs()): 
        print("personaid {} con datos jurisdiccionales !!!".format( personaid ))
        orgs_txs = txs_by_org(state, changes, org)
        if len(orgs_txs) > 0: txs += orgs_txs

    return txs

USER = 'HLF'
USER = 'BC_ROSI'
PASSW = 'HLF'
PASSW = 'BC_ROSI'
URLDB = 'localhost/xe'
URLDB='10.30.205.101/padr'

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
  block = 228802
  block = 228567

  res = db.queryall( QUERY_WSET_BY_BLOCK, { "block" : block } )

  if len(res) == 0: 
     print( "block {} does not exists or empty".format( block ) )
     quit()

  for name, group in Wset(res).groupby_tx_personaid(): 
      txs = process_txpersona(block, name[0], name[1], group)
      if len(txs) > 0:
         print(name)
         print(txs)
