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

# TODO: Agregar todos los impuestos
# Incluyendo los municipales
DICT_IMP_ORG = {
  "5900": "900",
  "5901": "901",
  "5902": "902"
}

EMPTY_DATAFRAME = pd.DataFrame()
global_state = EMPTY_DATAFRAME

def resolve_orgs_from_dors( idvalue: str ):
  # TODO: si idKey de dom comienza con 1 puede 
  # relacionarse con mas de una org
  # hay que recuperar las disintas orgs desde los dor del state

  if len( global_state ) == 0: return ""

  # busca roles del domicilio
  # org.domiorg.tipo.orden.rol
  roles = global_state[ global_state['componentid'] == "dor" \
                      & global_state['componentvalue'].str.contains( idvalue ) ]
  
  if len( roles ) == 0: return None
  
  orgs = roles['componentvalue'].str.split(".", n=1, expand=True)[0]

  return ",".join( orgs.unique().tolist() )

# Retorna un string conteniendo una lista de orgs separados por coma (roles de distintas orgs)
def resolve_orgs( idkey: str, idvalue: str ):
  if idkey == 'cms': return "900"
  if idkey == 'imp': return DICT_IMP_ORG.get( idvalue, None )
  if idkey == 'con': return DICT_IMP_ORG.get( idvalue.split('.')[0], None )
  if idkey in [ 'jur', 'act', 'dom', 'dor' ]: 
     org = idvalue.split('.')[0]
     if org != "1": return org

  if len( global_state ) == 0: return None

  # dom de AFIP: en el state puede tener roles de distintas jurisdicciones
  if idkey == 'dom': return resolve_orgs_from_dors( idvalue )

  return None


# Suppress SettingWithCopyWarning: 
# A value is trying to be set on a copy of a slice from a DataFrame.
# Try using .loc[row_indexer,col_indexer] = value instead
pd.options.mode.chained_assignment = None

def add_org( df: pd.DataFrame, state: pd.DataFrame ):

  if len( df ) == 0: return df

  # no se puede pasar como argumento al resolve_orgs
  # entonces se utiliza una variable global
  global global_state 
  global_state = state

  df['org'] = df.apply(lambda row: resolve_orgs( row.componentid, 
                                                 row.componentvalue ), 
                                                 axis=1 ) 
  # TODO: agrega otra columna que indica si 
  # el tipo de operacion es Create, Update, Delete
  # si el state esta vacio, son todas Create
  # si el value esta vacio, es un Delete

  # TODO: agregar otra columna period que indica Start o End
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

  if len( state ) > 0: 
     state = add_org( state, EMPTY_DATAFRAME )
     if state.org.count() > 0:
        print("personaid {} con state con datos jurisdiccionales ...".format( personaid ))
     else:
        # Si el state no tiene datos jurisdiccionales se vacia
        state = state[0:0] 

  changes = add_org( changes, state )

  if changes.org.count() == 0: return None # sin transacciones jurisdiccionales

  print("personaid {} con impuesto jurisdiccional !!!".format( personaid ))
  print( changes ) 

  txs = pd.DataFrame(columns=['org', 'tx'])

  
  

     

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
  block = 53410

  res = db.queryall( QUERY_BLOCK_WSET, { "block" : block } )

  if len(res) == 0: exit ## Empty block

  groups = mk_wsetdf( res ).groupby(['txseq', 'personaid'])

  for name, group in groups: 
      process_txpersona( block, name[0], name[1], group )

