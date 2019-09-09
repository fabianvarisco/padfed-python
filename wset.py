#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import pandas as pd
from organizaciones import *
  
COMPONENT_NAME_BY_TYPE = {
    "per": "PERSONA",
    "jur": "JURISDICCION",
    "act": "ACTIVIDAD",
    "cms": "CMSEDES",
    "ema": "EMAILS",
    "tel": "TELEFONO",
    "con": "CONTRIBMUNI",
    "dom": "DOMICILIO",
    "imp": "IMPUESTO",
    "dor": "DOMIROL",
    "eti": "ETIQUETA",
    "rel": "RELACION",
    "cat": "CATEGORIA",
    "wit": "TESTIGO",
}

JSONABLE_COMPONENT_TYPE=("imp", "con", 'jur', 'act', 'dom', 'dor', 'cms')

EMPTY_LIST = list()
EMPTY_DICT = dict()
EMPTY_SET = set()
EMPTY_DF = pd.DataFrame()
class Wset():

  BASE_COLUMNS = ['block', 'txseq', 'item', 'key', 'value', 'isdelete']

  def __init__(self, src=None): 
    
      # Instance variables

      if src is None or len(src) == 0:
         self.df = None  
         return

      self.target_orgs = set()
      self.migration_orgs = set()
      self.components_unique = None
      self.state = None

      if isinstance(src, list):
         self.df = pd.DataFrame(src, columns=self.BASE_COLUMNS) 
         new = self.df["key"].str.split("#", n=1, expand=True)
         personaid = new[0].str.split(":", n=1, expand=True)
         component = new[1].str.split(":", n=1, expand=True)
         self.df["personaid"]      = personaid[1].astype( 'int64' )
         self.df["component_type"] = component[0].astype( str ) # ej: dom
#        self.df["component_key"]  = component[1].astype( str ) # ej: 1.3.10, vamos a tratar de no usuar la key . mejor procesar el value json
         return

      elif isinstance(src, pd.DataFrame):
           self.df = src
           return

      raise ValueError("Unexpected src type [{}] must by list or pd.DataFrame".format(type(src)))

  def set_state(self, state):
      self.state = state
      return self

  def groupby_tx_personaid(self):
      return self.df.groupby(['txseq', 'personaid'])

  def resolve_state(self):
      if self.df is not None:
         self.df = self.df.groupby(['key']).first()
         self.df = self.df[self.df["isdelete"] != "T"]
      return self

  # Add obj column (json.loads(value)) and gather orgs
  def extend(self):

      def gather_orgs(component_type: str, obj: dict):
          if component_type in ['imp', 'con']:
             # en estos objetos siempre viene el org real
             # que fue seteado en resolver_obj 
             org = obj.get("org", -1)
             if org > 1: self.target_orgs.add(org)
             return
    
          if component_type in ('jur', 'act', 'dom', 'dor'): 
             org = obj.get("org", -1)
             if org > 1: 
                # si tienen org > 1, entonces fueron migrados 
                self.migration_orgs.add(org) 
                self.target_orgs.add(org)
      
          if component_type in ('jur', 'cms'):
             # recupera el org desde la provincia
             org = get_org_by_provincia(obj.get("provincia"))
             if org > 1: self.target_orgs.add(org) 
             if component_type == 'cms': self.target_orgs.add(COMARB)   
 
      def resolve_obj(row):
          obj = EMPTY_DICT
          if row.isdelete == "T":
             # When deleted getting obj from state by key
             if  not self.state is None \
             and not self.state.df.empty:
                 try:
                    obj = self.state.get_df().at[row.key, "obj"]
                 except KeyError:
                    pass
          
          elif isinstance(row.value, str) and len(row.value) > 0 \
          and  row.component_type in JSONABLE_COMPONENT_TYPE:
               obj = json.loads(row.value)
               if row.component_type in ['imp', 'con']: 
                  org = DEF_IMPUESTOS.get(obj.get("impuesto"), 1)
                  if org > 1: obj["org"] = org # Add org into impuesto and contribmuni
          
          if obj: gather_orgs(row.component_type, obj)

          return obj

      if self.df is not None:
         self.df["obj"] = self.df.apply(lambda row: resolve_obj(row), axis=1)
      return self

  # Delete dummies changes(state == change)
  def reduce(self):
      if self.df is None or self.df.empty or self.state is None or self.state.df.empty: return self
      
      idx=self.df.fillna("x").merge(self.state.get_df().fillna("x"), how='inner', on=['key', 'value', 'isdelete']).index
      if len(idx) == 0: return self
      
      self.df=self.df.iloc[~self.df.index.isin(idx)]
      return self

  def get_df(self) -> pd.DataFrame: return self.df

  def is_empty(self) -> bool: return self.df is None or self.df.empty  

  def has_deletes(self) -> bool: 
      try:
        return self.df["isdelete"].count() > 0
      except:
        return 0
      
  def get_impuesto_by_org(self, org: int) -> dict:
      # obj puede estar vacio si fue un delete 
      # y el obj no se pudo recuperar desde el state
      for o in self.get_objs("imp"): 
          if o.get("org") == org: return o
      return EMPTY_DICT
   
  def get_components_unique(self) -> set:
      if self.is_empty(): 
         return EMPTY_SET
      if self.components_unique is None:
         self.components_unique = set(self.df.component_type.unique())
      return self.components_unique 

  def get_objs(self, component_type: str) -> list:
      if component_type not in self.get_components_unique(): return EMPTY_LIST
      return self.df.loc[getattr(self.df, "component_type") == component_type, "obj"]         

  def get_orgs(self) -> set: 
      return self.target_orgs if self.state is None else self.target_orgs.union(self.state.get_orgs())

  def has_orgs(self) -> bool: 
      return bool(self.target_orgs)

  def from_migration(self, org: int) -> bool:
      return org in self.migration_orgs
