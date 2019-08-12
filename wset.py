#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import pandas as pd
from organizaciones import *

class Wset():

  BASE_COLUMNS = ['block', 'txseq', 'item', 'key', 'value', 'isdelete']

  def __init__(self, src=None): 
    
      self.target_orgs = set()

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

      raise ValueError("Unexpected src type [{}] must by list or pd.DataFrame".format(type(src)))

  def groupby_tx_personaid(self):
      return self.df.groupby(['txseq', 'personaid'])

  def resolve_state(self):
      if not self.df.empty:
         self.df = self.df.groupby(['key']).first()
         self.df = self.df[self.df["isdelete"] != "T"]
      return self

  # Add columns obj and orgs
  def extend(self, state = None):
      if not self.df.empty:
         self.df["obj"] = self.df.apply(lambda row: None if row.value is None or not isinstance( row.value, str ) else json.loads( row.value ), axis=1)
         for row in self.df.itertuples(): 
             self.add_orgs(row.component_type, row.obj, state)
      return self

  def get_df(self) -> pd.DataFrame: return self.df

  def is_empty(self) -> bool: return self.df.empty  

  def has_deletes(self) -> bool: 
      try:
        return self.df["isdelete"].count() > 0
      except:
        return 0

  def add_orgs(self, component_type: str, obj: dict, state):

      if obj is None: return
    
      if component_type == 'cms': 
         self.add_org(COMARB)
         return
      
      if component_type in ['imp', 'con']: 
         org = DICT_IMP_ORG.get(obj.get("impuesto", -1), 1)
         obj["org"] = org # Add org to impuesto or contribmuni
         if org > 1: self.add_org(org)
         return
      
      if component_type in ['jur', 'act', 'dom', 'dor']: 
         if obj.get("org", -1) > 1: 
            self.add_org(obj.get("org"))
            return
      
      if state is None \
      or state.is_empty() \
      or component_type != 'dom': return 
      
      # dom de AFIP: en el state puede
      # tener relaciones con roles de distintas orgs
      state_roles = state.get_domiroles()
        
      domi_orgs = set()

      for rol in state_roles:
          if  rol.get("org", -1 ) > 1 \
          and rol.get("org") not in domi_orgs \
          and rol.get("tipo",  -1) == obj.get("tipo",  -2) \
          and rol.get("orden", -1) == obj.get("orden", -2):
              domi_orgs.add(rol.get("org"))
              self.add_org(rol.get("org"))
        
      if len(domi_orgs) > 0: obj["orgs"] = domi_orgs

  def get_impuesto_by_org(self, org: int) -> dict:
      for o in self.get_impuestos(): 
          if o.get("org") == org: 
             return o
      return dict()
  
  def has_domicilios(self, org: int = -1) -> bool:
      for o in self.get_domicilios(): 
          if o.get("org") > 1 and (o.obj("org") == org or org == -1): return True
      return False

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
  def get_domiroles(self) -> list:      return self.get_objs("dor")

  def get_objs(self, component_type: str) -> list:
      if self.is_empty(): return list()
      return self.df.loc[getattr(self.df, "component_type") == component_type, "obj"] 

  def add_org(self, org: int): 
      if not org in self.target_orgs: self.target_orgs.add(org)         

  def get_orgs(self) -> set: 
      return self.target_orgs

  def has_orgs(self) -> bool: 
      return len(self.target_orgs) > 0
