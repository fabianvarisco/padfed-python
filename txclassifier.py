#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys    
import os    
import configparser
import logging
import time
  
import cx_Oracle
import pandas as pd

from wset import *
from organizaciones import *

logger = logging.getLogger()

EMPTY_LIST = list()

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
and key||'' /* avoid index by key */ like 'per:%'
order by txseq, item"""

QUERY_WSET_BY_KEYPATTERN = """
select *
from hlf.bc_valid_tx_write_set
where block+0 < :block
and key like :keypattern
order by key, block desc"""

QUERY_MAX_BLOCK = """
select max(block) as max_block
from hlf.bc_valid_tx_write_set
"""

def select_max_block() -> int:
    res = db.queryone(QUERY_MAX_BLOCK)
    if not res: raise ValueError("HLF.BC_VALID_TX_WRITE_SET is empty")
    logger.debug("fetched max_block {} from db ...".format(res[0]))
    return res[0]

# Suppress SettingWithCopyWarning: 
# A value is trying to be set on a copy of a slice from a DataFrame.
# Try using .loc[row_indexer,col_indexer] = value instead
pd.options.mode.chained_assignment = None

def txs_append(txs: list, org: int, txt: str): 
    txs.append({"block": None, "txseq": None, "personaid": None, "org": org, "kind": txt})

def mk_persona_state( block: int, personaid: int ):
    keypattern = "per:" + str(personaid) + "#%"
    res = db.queryall(QUERY_WSET_BY_KEYPATTERN, {"block" : block, "keypattern" : keypattern})
    if not res: return None
    state = Wset(res).resolve_state().extend()    
    # Si el state no tiene datos jurisdiccionales lo elimina 
    return state if state.has_orgs() else None

def impuesto_estado_AC(state_impuesto: dict, changes_impuesto: dict) -> bool:

    if  state_impuesto.get("estado") == "AC" \
    and not changes_impuesto: 
        return True

    if  changes_impuesto.get("estado") == "AC" \
    and changes_impuesto.get("isdelete", "x") != "T":
        return True
    
    return False   

def txs_by_component(txs: list, changes: Wset, component_type: str, org: int):
    name = COMPONENT_NAME_BY_TYPE.get(component_type)
    if name is None:
       raise ValueError("component_type {} unknow".format(component_type))

    if not component_type in ("imp", "con", "jur"):
       txs_append(txs, org, "CAMBIO EN {}".format(name))
       return

    for c in changes.get_objs(component_type):
        if (component_type in ("imp", "con") and org == c.get("org")) \
        or (component_type == "jur" and org == get_org_by_provincia(c.get("provincia"))):
           txs_append(txs, org, "CAMBIO EN {}".format(name))
           return

def txs_by_org(state: Wset, changes: Wset, org: int, target_orgs: set) -> (bool, list):
    txs = list()

    changes_components = changes.get_components_unique()

    state_impuesto   = EMPTY_DICT if state is None else state.get_impuesto_by_org(org)
    changes_impuesto = changes.get_impuesto_by_org(org)

    if  (state_impuesto or changes_impuesto) \
    and impuesto_estado_AC(state_impuesto, changes_impuesto):

       txt = None

       if   not state_impuesto: # No estaba inscripto
            txt = "MIGRACION" if changes.from_migration(org) else "INSCRIPCION"
       elif state_impuesto.get("estado", "x") != "AC":
            txt = "REINSCRIPCION"
       
       if txt:
          if org in target_orgs: txs_append(txs, org, txt)
          if org == COMARB and "jur" in changes_components:
             txt += " DESDE CM"
             for j in changes.get_objs("jur"):
                 if txt[0] in ("M", "I") or j.get("estado", "x") == "AC":
                    o = get_org_by_provincia(j.get("provincia"))
                    if o in target_orgs: txs_append(txs, o, txt)
          return True, txs # break

       # TODO: Puede ser una consolidacon   
       if org == COMARB: 
          for c in ("jur", "cms"):
              if c in changes_components:
                 # CAMBIO de JURISDICCION o de SEDE CM
                 rows = changes.get_objs(c)
                 if len(rows) > 0:
                    txt = "CAMBIO {} CM".format("JURISDICCION" if c == "jur" else "SEDE")
                    if org in target_orgs: txs_append(txs, org, txt)
                    for row in rows:
                        o = get_org_by_provincia(row.get("provincia"))
                        if o in target_orgs: txs_append(txs, o, txt)

    if not org in target_orgs: return False, txs # continue
    
    if "wit" in changes_components:
       txs_append(txs, org, "NUEVA PERSONA")
       return False, txs # continue

    for c in changes_components:
        if c != "cms":
           txs_by_component(txs, changes, c, org)

    # TODO: Detectar cambio de socio !!!!!!!!
    return False, txs # continue

def process_txpersona(block: int, target_orgs: set, txseq: int, personaid: int, changes: pd.DataFrame) -> list:
    logger.debug("processing block {} txseq {} personaid {} ...".format(block, txseq, personaid))

    state = mk_persona_state(block, personaid)

    changes = Wset(changes).set_state(state).extend().reduce()

    orgs = changes.get_orgs().intersection(target_orgs)

    if not orgs: return EMPTY_LIST

    txs = list()

    if  COMARB not in target_orgs \
    and COMARB in changes.get_orgs():
        orgs.add(COMARB) 

    for org in sorted(orgs): 
        do_break, orgs_txs = txs_by_org(state, changes, org, target_orgs)
        if orgs_txs: txs.extend(orgs_txs)
        if do_break: break

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

def config_logging(filename: str, level: str):
    level = logging.DEBUG if level.lower() == "debug" else logging.INFO
    logging.basicConfig(filename=filename, filemode='w', level=level, format='%(asctime)s %(levelname)-5s %(message)s')

def print_output(message: str, file = None):
    print(message)
    if not file is None: print(message, file=f, flush=True)

db: db_access

# MAIN #############################################################    

if __name__ == '__main__':

  try: 
     script_name = os.path.basename(sys.argv[0])
   
     timestr = time.strftime("%Y%m%d-%H%M%S")
   
     output_file_name = "{}.{}.output".format(script_name.split(".")[0], timestr)
     log_file_name    = "{}.{}.log".format(script_name.split(".")[0], timestr)
   
     f = open(output_file_name,"w+")
   
     print_output("{} - {} running - output_file {} ...".format(timestr, script_name, output_file_name), f)
     
     config_file_name = "{}.conf".format(script_name.split(".")[0])
     print_output("reading config from {} ...".format(config_file_name), f)
     config = configparser.ConfigParser()
     config.read(config_file_name)
     
     config_logging(log_file_name, config["behaviour"].get("logging_level", "INFO"))
   
     user            = config["db"]["user"]
     password        = config["db"]["password"]
     url             = config["db"]["url"]
     block_start     = config_getint(config, "filters", "block_start")
     block_stop      = config_getint(config, "filters", "block_stop")
     block_processed = config_getint(config, "filters", "block_processed")
     target_orgs     = config_target_orgs(config, "behaviour", "target_orgs")

     print_output("db.url: {}".format(user), f)
     print_output("db.user: {}".format(url), f)
     print_output("filters.block_start: {}".format(block_start), f)
     print_output("filters.block_stop: {}".format(block_stop), f)
     print_output("filters.block_processed: {}".format(block_processed), f)
     print_output("filters.target_orgs: {}".format(target_orgs), f)
   
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
      
     logger.info("processing from block {} to {} ...".format(block_start, block_stop))
     for i, block in enumerate(range(block_start, block_stop+1)):
         for tx in process_block(block, target_orgs): print_output(tx, f)
         block_processed = block
         if i % 100 == 0: save_config(config_file_name, config, block_processed)
   
     save_config(config_file_name, config, block_processed)

     timestr = time.strftime("%Y%m%d-%H%M%S")
     print_output("{} - {} success end".format(timestr, script_name), f)
  finally:
     if not f is None: 
        timestr = time.strftime("%Y%m%d-%H%M%S")
        print_output("{} - {} stop".format(timestr, script_name), f)
        f.close()
