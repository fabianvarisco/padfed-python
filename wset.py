#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import pandas as pd
from organizaciones import *

class Wset():

  BASE_COLUMNS = ['block', 'txseq', 'item', 'key', 'value', 'isdelete']

  def __init__(self, src=None): 
    
      # Instance variables
      self.target_orgs = set()
      self.migration_orgs = set()

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
      if not self.df.empty:
         self.df = self.df.groupby(['key']).first()
         self.df = self.df[self.df["isdelete"] != "T"]
      return self

  # Add obj column (json.loads(value)) 
  # and scan orgs
  def extend(self):
 
      def resolve_obj(row) -> dict:
          if row.value is None and row.isdelete == "T":
             # Getting obj from state by key
             if self.state is None or self.state.is_empty(): return {}
             state_obj = self.state.get_df().loc[row.key, "obj"]
             return {} if len(state_obj) == 0 else state_obj.get("obj", {})
          
          if isinstance(row.value, str) and len(row.value) > 0: 
             obj = json.loads(row.value)
             if row.component_type in ['imp', 'con']: 
                org = DEF_IMPUESTOS.get(obj.get("impuesto", -1), 1)
                obj["org"] = org # Add org to impuesto or contribmuni
             return obj     

          return {}

      if not self.df.empty:
         self.df["obj"] = self.df.apply(lambda row: resolve_obj(row), axis=1)
         for row in self.df.itertuples(): 
             self.scan_row(row.component_type, row.obj)
      return self

  # Delete dummies changes(state == change)
  def reduce(self):
      if self.df.empty or self.state is None or self.state.is_empty(): return self
      
      idx=self.df.fillna("x").merge(self.state.get_df().fillna("x"), how='inner', on=['key', 'value', 'isdelete']).index
      if len(idx) == 0: return self
      
      self.df=self.df.iloc[~self.df.index.isin(idx)]
      return self

  def get_df(self) -> pd.DataFrame: return self.df

  def is_empty(self) -> bool: return self.df.empty  

  def has_deletes(self) -> bool: 
      try:
        return self.df["isdelete"].count() > 0
      except:
        return 0

  # TODO: jntar con resolve_org para resolver en una unica barrida
  def scan_row(self, component_type: str, obj: dict):

      if obj is None: return
    
      if component_type == 'cms': 
         # TODO: revisar si hay que recuperar org desde provincia
         self.add_target_org(COMARB)
      
      elif component_type in ['imp', 'con']: 
           org = obj.get("org", -1)
           if org > 1: self.add_target_org(org)
      
      elif component_type in ['jur', 'act', 'dom', 'dor']: 
           org = obj.get("org", -1)
           if org > 1: 
              # si tienen org > 1, entonces fueron migrados 
              self.add_migration_org(org) 
              self.add_target_org(org)

           if component_type == 'jur':
              # recupera el org de la provincia
              org = get_org_by_provincia(obj.get("provincia", -1))
              if org > 1: self.add_target_org(org)
      
  def get_impuesto_by_org(self, org: int) -> dict:
      # obj puede estar vacio si fue un delete 
      # y el obj no se pudo recuperar desde el state
      for o in self.get_impuestos(): 
          if not o is None and o.get("org", -1) == org: return o
      return dict()
  
  # TODO: funcion para obtener una lista con los disintos type del wset
  #     
  def get_persona(self) -> list:        return self.get_objs("per")
  def get_jurisdicciones(self) -> list: return self.get_objs("jur")
  def get_actividades(self) -> list:    return self.get_objs("act")
  def get_cmsedes(self) -> list:        return self.get_objs("cms")
  def get_emails(self) -> list:         return self.get_objs("ema")
  def get_telefonos(self) -> list:      return self.get_objs("tel")
  def get_relaciones(self) -> list:     return self.get_objs("rel")
  def get_contribmunis(self) -> list:   return self.get_objs("con")
  def get_domicilios(self) -> list:     return self.get_objs("dom")
  def get_impuestos(self) -> list:      return self.get_objs("imp")
  def get_persona(self) -> list:        return self.get_objs("per")
  def get_domisroles(self) -> list:     return self.get_objs("dor")

  def get_objs(self, component_type: str) -> list:
      if self.is_empty(): return list()
      return self.df.loc[getattr(self.df, "component_type") == component_type, "obj"] 

  def add_target_org(self, org: int): 
      if not org in self.target_orgs: self.target_orgs.add(org)         

  def get_orgs(self) -> set: 
      return self.target_orgs

  def has_orgs(self) -> bool: 
      return len(self.target_orgs) > 0

  def add_migration_org(self, org: int):
      if not org in self.migration_orgs: self.migration_orgs.add(org) 

  def from_migration(self, org: int) -> bool:
      return org in self.migration_orgs
