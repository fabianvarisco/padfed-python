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

EMPTY_WSETDF = pd.DataFrame(columns=['block', 'txseq', 'item', 'key', 'value', 'isdelete', 'orgs', 'obj'])

def mk_resolver_orgs( state: pd.DataFrame ):

    # Inner func using state
    def resolve_orgs_from_dors( dom: dict ) -> str:
        if len( state ) == 0: return None
        
        # TODO: revisar con team SR
        # hay que recuperar las disintas orgs desde los dor del state

        roles = state.loc[ state["component_type"] == "dor", "obj" ].values
        
        orgs = set()

        for rol in roles:
            if  rol.get( "org", -1 ) > 1 \
            and rol.get( "org" ) not in orgs \
            and rol.get( "tipo", -1 ) == dom.get( "tipo", -2 ) \
            and rol.get( "orden", -1 ) == dom.get( "orden", -2 ):
                orgs.add( rol.get( "org" ) )
        
        return None if len( orgs ) == 0 else ",".join( orgs )
    
    # Inner func
    # Retorna un string conteniendo una lista de orgs 
    # separados por coma (roles de distintas orgs)
    def resolve_orgs( component_type: str, obj: dict ) -> str:
    
        if component_type == 'cms': return "900"
      
        if component_type in [ 'imp', 'con' ]: 
           org = DICT_IMP_ORG.get( obj.get( "impuesto", -1 ), None )
           return None if org == None else str( org )
      
        if component_type in [ 'jur', 'act', 'dom', 'dor' ]: 
           if obj.get( "org", -1 ) > 1: return str( obj.get( "org" ) )
      
        if len( state ) == 0 or component_type != 'dom': return None
      
        # dom de AFIP: en el state puede tener roles de distintas jurisdicciones
        return resolve_orgs_from_dors( obj )
    
    # return clousure func 
    return resolve_orgs

# Suppress SettingWithCopyWarning: 
# A value is trying to be set on a copy of a slice from a DataFrame.
# Try using .loc[row_indexer,col_indexer] = value instead
pd.options.mode.chained_assignment = None

def add_orgs_column( df: pd.DataFrame, state: pd.DataFrame ) -> pd.DataFrame:
    if len( df ) == 0: return df
    # state no se puede pasar como argumento al resolve_orgs
    # entonces se utiliza una variable global
    resolve_orgs = mk_resolver_orgs( state )    
    df["orgs"] = df.apply(lambda row: resolve_orgs( row.component_type, row.obj ), axis=1 ) 
    return df

def mk_wsetdf( res ) -> pd.DataFrame:
    if len( res ) == 0: return EMPTY_WSETDF
    df = pd.DataFrame(res, columns=['block', 'txseq', 'item', 'key', 'value', 'isdelete']) 
    new = df["key"].str.split("#", n=1, expand=True)
    personaid = new[0].str.split(":", n=1, expand=True)
    component = new[1].str.split(":", n=1, expand=True)
    df["personaid"]      = personaid[1].astype( int )
    df["component_type"] = component[0].astype( str ) # ej: dom
    df["component_key"]  = component[1].astype( str ) # ej: 1.3.10, vamos a tratar de no usuar la key . mejor procesar el value json
    return df

def add_obj_column( df: pd.DataFrame ) -> pd.DataFrame:
    if len( df ) == 0: return df
    df["obj"] = df.apply(lambda row: None if row.value is None or not isinstance( row.value, str ) else json.loads( row.value ), axis=1 )
    return df

def get_persona_state( block: int, personaid: int ):
    keypattern = "per:" + str( personaid ) + "#%"
    res = db.queryall( QUERY_WSET_BY_KEYPATTERN, { "block" : block, "keypattern" : keypattern } )
    if len(res) == 0: return EMPTY_WSETDF  
    state = mk_wsetdf( res ).groupby( ['key'] ).first()
    return add_orgs_column( add_obj_column( state[ state["isdelete"] != "T" ] ), EMPTY_WSETDF )

def process_txpersona( block: int, txseq: int, personaid: int, changes: pd.DataFrame ) -> pd.DataFrame:
    # print( "processing block {} personaid {} ...".format( block, personaid ) )

    state = get_persona_state( block, personaid )

    if len( state ) > 0:
       if state["orgs"].count() == 0:
          # Si el state no tiene datos jurisdiccionales se vacia
          state = EMPTY_WSETDF 

    changes = add_orgs_column( add_obj_column( changes ), state )

    txs = pd.DataFrame(columns=['org', 'tx'])

    if state["orgs"].count() == 0 and changes["orgs"].count() == 0: 
       return txs # sin transacciones jurisdiccionales

    print( "personaid {} con datos jurisdiccionales !!!".format( personaid ))
    print( "state with orgs: {}".format( state["orgs"].count() ) )
    print( state.loc[getattr(state, "orgs").notnull(), "orgs" ] )
    print( "changes with orgs: {}".format( changes["orgs"].count() ) )
    print( changes.loc[getattr(changes, "orgs").notnull(), "orgs"] )

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

    changes_has_deletes = (changes["isdelete"].count() > 0)

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

