#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys    
import os    
import configparser
import logging
  
import cx_Oracle
import pandas as pd

from wset import *
from organizaciones import *

logger = logging.getLogger()

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

  def queryone(self, query: str,  vars: dict = {}):
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

QUERY_MAX_BLOCK = """
select max(block) as max_block
from hlf.bc_valid_tx_write_set
"""

def select_max_block() -> int:
    res = db.queryone(QUERY_MAX_BLOCK)
    if len(res) == 0: raise ValueError("HLF.BC_VALID_TX_WRITE_SET is empty")
    logger.debug("fetched max_block {} from db ...".format(res[0]))
    return res[0]

# Suppress SettingWithCopyWarning: 
# A value is trying to be set on a copy of a slice from a DataFrame.
# Try using .loc[row_indexer,col_indexer] = value instead
pd.options.mode.chained_assignment = None

def txs_append(txs: list, org: int, txt: str): 
    txs.append({"block": None, "txseq": None, "personaid": None, "org": org, "kind": txt})

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

def txs_by_org(state: Wset, changes: Wset, org: int, target_orgs: set) -> list:
    txs = list()

    state_impuesto   = state.get_impuesto_by_org(org)
    changes_impuesto = changes.get_impuesto_by_org(org)

    if inscripcion_alta(state_impuesto, changes_impuesto):

       # MIGRACION o INSCRIPCION
       if len(state_impuesto) == 0: # No estaba inscripto
          txt = "MIGRACON" if changes.from_migration(org) else "INSCRIPCION"
          if org in target_orgs: txs_append(txs, org, txt)
          if org == COMARB:
             txt += " DESDE CM"
             for j in changes.get_jurisdicciones():
                 o = get_org_by_provincia(j.get("provincia", -1))
                 if o in target_orgs: txs_append(txs, o, txt)
          return txs

       # REINSCRIPCION
       if len(state_impuesto) > 0 and state_impuesto.get("estado", "") != "AC":
          txt = "REINSCRIPCION"
          if org in target_orgs: txs_append(txs, org, txt)
          if org == COMARB:
             txt += " DESDE CM"
             for j in changes.get_jurisdicciones(): 
                 if j.get("estado", "") == "AC":
                    o = get_org_by_provincia(j.get("provincia", -1))
                    if o in target_orgs: txs_append(txs, o, txt)
          return txs
    
       if org == COMARB:
          # CAMBIO DE JURISDICCION CM
          rows = changes.get_jurisdicciones()
          if len(rows) > 0:
             if org in target_orgs: txs_append(txs, org, "CAMBIO JURISDICCION CM")
             for j in rows:
                 o = get_org_by_provincia(j.get("provincia", -1))
                 if o in target_orgs: txs_append(txs, o, "CAMBIO JURISDICCION CM")
 
          # CAMBIO DE SEDE CM
          rows = changes.get_cmsedes()
          if len(rows) > 0:
             if org in target_orgs: txs_append(txs, org, "CAMBIO DE SEDE CM")
             for s in rows:
                 o = get_org_by_provincia(s.get("provincia", -1))
                 if o in target_orgs: txs_append(txs, o, "CAMBIO DE SEDE CM")

    if not org in target_orgs: return txs

    txs_by_persona(  txs, changes.get_persona())
    txs_by_component(txs, org, "IMPUESTO",  changes.get_impuestos(), extrict=True)
    txs_by_component(txs, org, "CONTRIBMUNI", changes.get_contribmunis(), extrict=True)
    txs_by_component(txs, org, "ACTIVIDAD", changes.get_actividades())
    txs_by_component(txs, org, "DOMICILIO", changes.get_domicilios()) 
    txs_by_component(txs, org, "RELACION",  changes.get_relaciones())
    txs_by_component(txs, org, "EMAIL",     changes.get_emails())
    txs_by_component(txs, org, "TELEFONO",  changes.get_relaciones()) 
    txs_by_component(txs, org, "DOMIROL",   changes.get_domisroles()) 

    # TODO: Detectar cambio de socio !!!!!!!!
    return txs

def process_txpersona(block: int, target_orgs: set, txseq: int, personaid: int, changes: pd.DataFrame) -> list:
    logger.debug("processing block {} txseq {} personaid {} ...".format(block, txseq, personaid))

    state = mk_persona_state(block, personaid)

    # Si el state no tiene datos jurisdiccionales lo vacia
    if not state.has_orgs(): state = Wset() 

    changes = Wset(changes).set_state(state).extend().reduce()

    txs = list()

    union_orgs = state.get_orgs().union(changes.get_orgs())

    orgs = union_orgs.intersection(target_orgs)

    if len(orgs) == 0: return txs

    if  COMARB not in target_orgs \
    and COMARB in union_orgs:
        orgs.add(COMARB) 

    for org in orgs: 
        orgs_txs = txs_by_org(state, changes, org, target_orgs)
        if len(orgs_txs) > 0: txs += orgs_txs

    return txs

def process_block(block: int, target_orgs: set) -> list:
    logger.debug("processing block {} ...".format(block))

    res = db.queryall(QUERY_WSET_BY_BLOCK, {"block" : block})

    txs = list()

    if len(res) == 0: 
       logger.debug("block {} non-existing or empty".format(block))
       return txs

    for name, group in Wset(res).groupby_tx_personaid():
        txseq     = name[0] 
        personaid = name[1]
        for tx in process_txpersona(block, target_orgs, txseq, personaid, group):
            tx["block"] = block
            tx["txseq"] = txseq
            tx["personaid"] = personaid
            txs.append(tx)
    return txs

def config_getint(config, section: str, option: str) -> int:
    try:
      return config.getint(section, option)
    except:
      return -1

def config_target_orgs(config, section: str, option: str) -> set:
    target_orgs = {}
    try:
       target_orgs = json.loads(config.get(section, option))
       target_orgs = set(target_orgs) # array
    except:
       try:
          target_orgs = {target_orgs} # single
       except:
          # All orgs
          target_orgs = DEF_ORGANIZACIONES.keys()

    if len(target_orgs) == 0:
       raise ValueError("Config: section [{}] option [{}] invalid (target_orgs empty !!!)".format(section, option))

    for o in target_orgs:
        if o < COMARB:
           raise ValueError("Config: section [{}] option [{}] org [{}] invalid".format(section, option, o))
        if not o in DEF_ORGANIZACIONES.keys():
           raise ValueError("Config: section [{}] option [{}] org [{}] invalid".format(section, option, o))        

    return target_orgs

def save_config(filename: str, config, block: int):
    logger.debug("saving config file [{}] with block_proceseed [{}] ...".format(filename, block))
    config["filters"]["block_processed"] = str(block)
    with open(filename, 'w') as f: config.write(f) 

def config_logging(level: str):
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)-5s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if level.lower() == "debug" else logging.INFO)

db: db_access

# MAIN #############################################################    

if __name__ == '__main__':

  script_name = os.path.basename(sys.argv[0])
  print("{} running ...".format(script_name))

  config_file_name = "{}.ini".format(script_name.split(".")[0])
  print("reading config from {} ...".format(config_file_name))
  config = configparser.ConfigParser()
  config.read(config_file_name)
  
  ll = config["behaviour"].get("logging_level", "INFO")
  config_logging(ll)

  user            = config["db"]["user"]
  password        = config["db"]["password"]
  url             = config["db"]["url"]
  block_start     = config_getint(config, "filters", "block_start")
  block_stop      = config_getint(config, "filters", "block_stop")
  block_processed = config_getint(config, "filters", "block_processed")
  target_orgs     = config_target_orgs(config, "behaviour", "target_orgs")

  if  block_processed > -1:
      if block_processed < block_start: 
         raise ValueError("Config: section [{}] block_processed {} must be greater than or equal to block_start {}".format("filters", block_processed, block_start))

  if  block_stop > -1 and block_stop < block_start: 
      raise ValueError("Config: section [{}] block_stop {} must be greater than or equal to block_start {}".format("filters", block_stop, block_start))

  if  block_processed > -1 \
  and block_stop > -1 \
  and block_stop < block_processed: 
      raise ValueError("Config: section [{}] block_stop {} must be greater than or equal to block_processed {}".format("filters", block_stop, block_processed))

  db = db_access(user, password, url)

  if block_stop == -1: 
     max_block = select_max_block()
     if block_processed > max_block:
        raise ValueError("Config: section [{}] block_processed {} must be less than db max_block {}".format("filters", block_processed, max_block))
     if block_processed == -1 and block_start > max_block:
        raise ValueError("Config: section [{}] block_start {} must be less than max_block {}".format("filters", block_start, max_block))
     block_stop = max_block

  if block_processed > -1: block_start = block_processed
   
  i = 0
  logger.info("processing from block {} to {} ...".format(block_start, block_stop))
  for block in range(block_start, block_stop+1):
      for tx in process_block(block, target_orgs): print(tx)
      block_processed = block
      i += 1
      if i % 100 == 0: save_config(config_file_name, config, block_processed)

  save_config(config_file_name, config, block_processed)
