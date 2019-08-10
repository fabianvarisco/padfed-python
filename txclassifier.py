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

EMPTY_DATAFRAME = pd.DataFrame()

def mk_resolver_orgs( state: pd.DataFrame ):

    # Inner func using state
    def resolve_orgs_from_dors( dom_value: str ):
        # TODO: revisar con team SR
        # hay que recuperar las disintas orgs desde los dor del state
      
        if len( state ) == 0: return None
      
        # busca roles del domicilio y se queda con los values
        roles = state.loc[ state.component_type == "dor", "value" ]
      
        if len( roles ) == 0: return None
      
        dom = json.loads( dom_value )
      
        orgs = set()
      
        for v in roles.values:
            dor = json.loads( v )
            if dor["tipo"] == dom["tipo"] and dor["orden"] == dom["orden"]:
               if dor["org"] not in orgs: 
                  if dor["org"] > 1:
                     # TODO: no debe ser 1
                     orgs.add( dor["org"] )
        
        return None if len( orgs ) == 0 else ",".join( orgs )
    
    # Inner func
    # Retorna un string conteniendo una lista de orgs 
    # separados por coma (roles de distintas orgs)
    def resolve_orgs( component_type: str, value: str ):
    
        if component_type == 'cms': return "900"
      
        if component_type in [ 'imp', 'con' ]: 
           imp = json.loads( value )["impuesto"]
           org = DICT_IMP_ORG.get( imp, None )
           return None if org == None else str( org )
      
        if component_type in [ 'jur', 'act', 'dom', 'dor' ]: 
           org = json.loads( value )["org"]
           if org > 1: return str( org )
      
        if len( state ) == 0: return None
      
        # dom de AFIP: en el state puede tener roles de distintas jurisdicciones
        return None if component_type != 'dom' else resolve_orgs_from_dors( value )
    
    # return clousure func 
    return resolve_orgs

# Suppress SettingWithCopyWarning: 
# A value is trying to be set on a copy of a slice from a DataFrame.
# Try using .loc[row_indexer,col_indexer] = value instead
pd.options.mode.chained_assignment = None

def add_org( df: pd.DataFrame, state: pd.DataFrame ):

  if len( df ) == 0: return df

  # no se puede pasar como argumento al resolve_orgs
  # entonces se utiliza una variable global
  resolve_orgs = mk_resolver_orgs( state )

  df['org'] = df.apply(lambda row: resolve_orgs( row.component_type, 
                                                 row.value ), 
                                                 axis=1 ) 
  return df

def mk_wsetdf( res ):
  df = pd.DataFrame(res, columns=['block', 'txseq', 'item', 'key', 'value', 'isdelete']) 
  new = df["key"].str.split("#", n=1, expand=True)
  personaid = new[0].str.split(":", n=1, expand=True)
  component = new[1].str.split(":", n=1, expand=True)
  df["personaid"]      = personaid[1]
  df["component_type"] = component[0] # ej: dom
  df["component_key"]  = component[1] # ej: 1.3.10, vamos a tratar de no usuar la key . mejor procesar el value json
  return df

def get_persona_state( block: int, personaid: int ):
  keypattern = "per:" + personaid + "#%"
  res = db.queryall( QUERY_WSET_BY_KEYPATTERN, { "block" : block, "keypattern" : keypattern } )
  if len(res) == 0: return EMPTY_DATAFRAME
  
  state = mk_wsetdf( res ).groupby( ['key'] ).first()
  # TODO: eliminar los filas con delete antes de retornar el state
  return state

def process_txpersona( block: int, txseq: int, personaid: int, changes: pd.DataFrame ):
  # print( "processing block {} personaid {} ...".format( block, personaid ) )

  state = get_persona_state( block, personaid )

  if len( state ) > 0: 
     state = add_org( state, EMPTY_DATAFRAME )
     if state.org.count() > 0:
        print("personaid {} con state con datos jurisdiccionales ...".format( personaid ))
     else:
        # Si el state no tiene datos jurisdiccionales se vacia
        state = state[0:0] 

  changes = add_org( changes, state )

  if changes.org.count() == 0: return None # sin transacciones jurisdiccionales

  print("personaid {} con datos jurisdiccionales !!!".format( personaid ))
  print( changes[["key","org"]] ) 

  txs = pd.DataFrame(columns=['org', 'tx'])

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

  # ALTA EN JUR
  # - tiene imp con org X y estado AC 
  # - no tiene state con org X
  # - no tiene dom con org == component_key[0] # Domicilio migrado
  # accion:
  # - una tx { imp.org, ALTA EN JUR }
  # - una tx por cada jur { jur.org, ALTA EN CM }
  #

  # CM CAMBIO DE JURISDICCION
  # - tiene jur con value distinto de state
  #
  #
  #
  #

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

  groups = mk_wsetdf( res ).groupby(['txseq', 'personaid'])

  for name, group in groups: 
      process_txpersona( block, name[0], name[1], group )

