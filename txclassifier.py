#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import cx_Oracle
import pandas as pd
import os
import json

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

# TODO: Agregar todos los impuestos
# Incluyendo los municipales
DICT_IMP_ORG = {
  5900: 900,
  5901: 901,
  5902: 902
}

class Wset():

  BASE_COLUMNS  = ['block', 'txseq', 'item', 'key', 'value', 'isdelete']
  ADDED_COLUMNS = ['personaid', 'component_type', 'component_key', 'obj', 'orgs']

  def __init__(self, src=None):  

      if src is None or len(src) == 0:
         self.df = pd.DataFrame()  
         return
      
      if isinstance(src, list):
         self.df = pd.DataFrame(src, columns=self.BASE_COLUMNS) 
         new = self.df["key"].str.split("#", n=1, expand=True)
         personaid = new[0].str.split(":", n=1, expand=True)
         component = new[1].str.split(":", n=1, expand=True)
         self.df["personaid"]      = personaid[1].astype( int )
         self.df["component_type"] = component[0].astype( str ) # ej: dom
         self.df["component_key"]  = component[1].astype( str ) # ej: 1.3.10, vamos a tratar de no usuar la key . mejor procesar el value json
         return

      elif isinstance(src, pd.DataFrame):
           self.df = src
           return

      raise ValueError("type of src [{}] unexpected".format(type(src)))

  def groupby_tx_personaid(self):
      return self.df.groupby(['txseq', 'personaid'])

  def resolve_state(self):
      if not self.df.empty:
         self.df = self.df.groupby(['key']).first()
         self.df = self.df[self.df["isdelete"] != "T"]
      return self

  # Add columns obj and orgs
  def extend(self, state: pd.DataFrame = None):
      if not self.df.empty:
         self.df["obj"]  = self.df.apply(lambda row: None if row.value is None or not isinstance( row.value, str ) else json.loads( row.value ), axis=1 )             
         self.state = state
         self.df["orgs"] = self.df.apply(lambda row: self.resolve_orgs( row.component_type, row.obj ), axis=1 )
         self.state = None
      return self

  def get_df(self) -> pd.DataFrame: return self.df

  def is_empty(self) -> bool: return self.df.empty  

  def has_deletes(self) -> bool: 
      try:
        return self.df["isdelete"].count() > 0
      except:
        return 0

  def has_orgs(self) -> bool:
      return self.count_with_orgs() > 0

  def count_with_orgs(self) -> int:
      try:
        return self.df["orgs"].count()
      except: 
        return 0  

  def resolve_orgs(self, component_type: str, obj: dict ) -> str:
    
      if component_type == 'cms': return "900"
      
      if component_type in [ 'imp', 'con' ]: 
         org = DICT_IMP_ORG.get( obj.get( "impuesto", -1 ), 1 )
         obj["org"] = org # Add org to impuesto or contribmuni
         return None if org == 1 else str( org )
      
      if component_type in [ 'jur', 'act', 'dom', 'dor' ]: 
         if obj.get( "org", -1 ) > 1: return str( obj.get( "org" ) )
      
      if self.state is None \
      or self.state.empty \
      or component_type != 'dom': return None
      
      # dom de AFIP: en el state puede tener roles de distintas jurisdicciones
      return self.resolve_orgs_from_dors( obj )

  def resolve_orgs_from_dors(self, dom: dict) -> str:
      # TODO: revisar con team SR
      # hay que recuperar las disintas orgs desde los dor del state

      roles = self.state.loc[ self.state["component_type"] == "dor", "obj"]

      if roles.empty: return None
        
      orgs = set()

      for rol in roles:
          if  rol.get("org", -1 ) > 1 \
          and rol.get("org") not in orgs \
          and rol.get("tipo",  -1) == dom.get("tipo",  -2) \
          and rol.get("orden", -1) == dom.get("orden", -2):
              orgs.add( rol.get( "org" ) )
        
      return None if len(orgs) == 0 else ",".join(orgs)

  def get_impuesto( self, org: int = -1, impuesto: int = -1) -> dict:
      impuestos = self.df.loc[getattr( self.df, "component_type" ) == "imp", "obj"]
      for obj in impuestos:
          if obj["org"] > 1:
             if (org == -1 and impuesto == -1) \
             or (org == obj["org"] and impuesto == obj["impuesto"] ) \
             or (org == obj["org"] and impuesto == -1) \
             or (org == -1         and impuesto == obj["impuesto"]): return obj
      return None
  
  def has_domicilios(self, org: int = -1 ) -> bool:
      domicilios = self.df.loc[getattr(self.df, "component_type") == "dom", "obj"] 
      for obj in domicilios: 
          if obj["org"] > 1 and (obj["org"] == org or org == -1): return True
      return False

  def get_jurisdicciones(self): 
      return self.df.loc[ getattr(self.df, "component_type") == "jur", "obj"]
    
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

    if  not state.has_orgs() \
    and not changes.has_orgs(): return txs # sin datos jurisdiccionales

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

