#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urllib import request
from urllib.error import HTTPError
import sys
import json
import ssl
import cx_Oracle
import code
import os

def enchular(coso):
  return json.dumps(coso, indent=2, sort_keys=True)

class PucClient:
  os.environ["NLS_LANG"] = os.getenv("NLS_LANG", ".AL32UTF8")
  os.environ['no_proxy'] = '*'

  def __init__(self, user = 'adminbd', passwd = 'adminbd', url = '10.30.205.21/blchain'):
    self.pool = cx_Oracle.SessionPool(user, passwd, url)

  def query(self, query):
    try:
      cnx = self.pool.acquire()
      cur = cnx.cursor()
      result = cur.execute(query).fetchall()
      return result
    finally:
      if cur is not None: cur.close()
      if cnx is not None: self.pool.release(cnx)


  def query_lob(self, query):
    try:
      cnx = self.pool.acquire()
      cur = cnx.cursor()
      result = cur.execute(query).fetchall()[0][0].read()
      return result
    finally:
      if cur is not None: cur.close()
      if cnx is not None: self.pool.release(cnx)

  def getPersona(self, clave):
    r = self.query("""select   p.id,  
         p.tipo_id,                 
         p.tipo_persona,            
         p.estado_id,               
         p.fecha_inscripcion,       
         p.id_forma_juridica,
         p.mes_cierre,
         p.fecha_contrato_social,
         p.sexo,
         p.fecha_nacimiento,
         p.fecha_fallecimiento,
         p.id_tipo_documento,
         p.id_activo,
         p.id_pais,
         p.razon_social,
         p.nombre,
         p.apellido,
         p.apellido_materno,
         p.documento,
         da.id_organismo_inscripcion,
         da.numero_inscripcion,
         da.plazo_duracion,
         greatest(p.fecha_actualizacion, nvl(da.fecha_actualizacion, p.fecha_actualizacion)) as fecha_actualizacion
        from     puc.t_persona p
        left     outer join puc.t_dato_adicional da on (p.id = da.id_persona)
        where    1 = 1
        and      p.tipo_id in ('C','E','I','L')
        and      p.tipo_persona in ('F','J')
        and      p.id = %s """ % str(clave))
    return (r)

  def getImpuestos(self, clave):
    r = self.query("""
          select   *
          from     puc.t_impuesto
          where    id_persona = %s
          and      es_vigente = 'S'
          and      estado in ('AC', 'EX', 'NA', 'BD')
          and     (estado != 'BD' or periodo > 200912)
          and     (id_impuesto in (10, 11, 20, 21, 30, 32, 34, 308) or id_impuesto > 5000)
          order by id_impuesto
    """%str(clave))
    return r

  def getCategorias(self, clave):
    r = self.query("""
          select   *
          from     puc.t_categoria
          where    id_persona = %s
          and      es_vigente='S'
          and     (estado != 'BD' or periodo > 200912)
          and      id_impuesto in (20,21,308)
          order by id_impuesto, id_categoria
    """%str(clave))
    return r

  def getContribucionesMunicipales(self, clave):
    r = self.query("""
          select   id_impuesto,
                   id_municipio,
                   id_provincia,
                   to_char(to_date(periodo_desde, 'YYYYMMDD'), 'YYYY-MM-DD') as periodo_desde,
                   to_char(to_date(periodo_hasta, 'YYYYMMDD'), 'YYYY-MM-DD') as periodo_hasta,
                   fecha_actualizacion
          from     puc.t_impuesto_municipio
          where    id_persona = %s
          and      es_vigente = 'S'
          order by id_impuesto, id_municipio
    """%str(clave))
    return r

  def getActividades(self, clave):
    r = self.query("""
        with actividad_desde_hasta as
             (
             select
             id_persona,
             cod_nomenclador,
             id_actividad,
             orden,
             case estado when 'AC' then periodo else lag_periodo end as desde,
             case estado when 'AC' then null    else periodo     end as hasta,
             to_char(fecha_actualizacion, 'YYYY-MM-DD') as fecha_actualizacion
             from
             (
             select
             id_persona,
             orden,
             cod_nomenclador,
             id_actividad,
             periodo,
             estado,
             es_vigente,
             lag(periodo)             over(partition by id_persona, case orden when 1 then 1 else 2 end, id_actividad order by periodo, estado) as lag_periodo,
             lag(estado)              over(partition by id_persona, case orden when 1 then 1 else 2 end, id_actividad order by periodo, estado) as lag_estado,
             lag(es_vigente)          over(partition by id_persona, case orden when 1 then 1 else 2 end, id_actividad order by periodo, estado) as lag_es_vigente,
             max(fecha_actualizacion) over(partition by id_persona, case orden when 1 then 1 else 2 end, id_actividad) as fecha_actualizacion
             from puc.t_actividad
             where id_persona = %s
             and   cod_nomenclador = 883
             and   id_actividad > 100
             and   ((orden = 1 and estado = 'AC') or (orden > 1 and estado in ('AC','BD')))
             )
             where es_vigente = 'S'
             )
        --
        select   *
        from     actividad_desde_hasta
        where   (hasta is null or hasta > 200912)
        order by orden
    """%str(clave))
    return r

  def getDomicilios(self, clave):
    r = self.query("""
        select   *
        from     puc.t_domicilio
        where    id_persona = %s
        and      orden > 0
        and      id_tipo_domicilio in (1,2,3)
        and     (fecha_baja is null or to_char(fecha_baja, 'YYYYMM') > to_char(200912))
        order by id_tipo_domicilio, orden
      """%str(clave))
    return r

  def getEtiquetas(self, clave):
    r = self.query("""
      select   *
      from     puc.t_caracterizacion
      where    id_persona = %s
      and      periodo between 19000101 and 21001212
      and      es_vigente = 'S'
      and      id_caracterizacion in (
      22,38,39,54,55,77,83,84,88,99,
      108,160,162,195,
      263,272,274,299,
      328,329,330,339,340,342,343,344,351,352,392,
      412,413,414)    
    """%str(clave))
    return r

  def getTelefonos(self, clave):
    r = self.query("""
      select   *
      from     puc.t_telefono
      where    id_persona = %s
      and      secuencia > 0
      order by secuencia    
    """%str(clave))
    return r

  def getRelaciones(self, clave):
    r = self.query("""
      select   *
      from     puc.t_relacion
      where    id_persona = %s
      and      id_tipo_relacion = 3
      order by id_persona_asociada
    """%str(clave))
    return r

  def getJson(self, clave):
    r = self.query_lob("""
          select padfed.padfed_util.get_json(%s, 1) from dual
        """ % str(clave))

    return json.loads(r)

class PadFedClient:
  #url = 'https://sr-blockchain.cloudhomo.afip.gob.ar/hlfproxy/api/v1/%s/%s/%s'
  headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
  ctx = None

  def __init__(self, url_base = 'https://sr-blockchain.cloudhomo.afip.gob.ar/hlfproxy/api/v1', channel='padfedchannel', cc='padfedcc'):
    self.url_base = url_base 
    self.channel = channel
    self.cc = cc
  
  def url_para(self, endpoint):
    return ('%s/%s/%s/%s' % (self.url_base, endpoint, self.channel, self.cc))
	
  def invoke(self,endpoint,body):
    try:
      req =  request.Request(self.url_para(endpoint), data=body, headers=self.headers)
      resp = request.urlopen(req, context=self.ctx)
      obj = json.loads(json.loads(resp.read())['ccResponse'])
    except HTTPError:
      ex_info = sys.exc_info()
      obj = json.loads(ex_info[1].read())
    except:
      ex_info = sys.exc_info()
      obj = {'errMsg': ex_info[1].__str__(), 'ex': ex_info}
    return obj
	  
  def dontVerify(self):
    self.ctx = ssl.create_default_context()
    self.ctx.check_hostname = False
    self.ctx.verify_mode = ssl.CERT_NONE
  
  def fn(self, fn, args):
    return json.dumps({"function":fn,"Args":args})

  def queryPersona(self, clave, pulenta = False):
    body = self.fn('queryPersona',[clave,pulenta]).encode('utf-8')
    return self.invoke('query',body)

  def queryByKey(self,key):
    body = self.fn('queryByKey',[key]).encode('utf-8')
    return self.invoke('query',body)

  def queryByKeyRange(self,range):
    body = self.fn('queryByKeyRange',range).encode('utf-8')
    return self.invoke('query',body)

  def getPersona(self, clave, pulenta = False):
    body = self.fn('getPersona',[clave]).encode('utf-8')
    return self.invoke('query',body)

  def queryHistory(self, key):
    body = self.fn('queryHistory', [key]).encode('utf-8')
    return self.invoke('query',body)
	
if __name__ == '__main__':

  # Instancio el cliente padfed
  url = os.getenv('HLF_PROXY_URL', 'https://sr-blockchain.cloudhomo.afip.gob.ar/hlfproxy/api/v1')
  channel = os.getenv('PADFED_CHANNEL','padfedchannel')
  cc = os.getenv('PADFED_CHAINCODE_NAME', 'padfedcc')

  pf = PadFedClient(url,channel,cc)
  pf.dontVerify()

  # Instancio el cliente padr
  user = os.getenv('PUC_DB_USER', 'puc')
  passwd = os.getenv('PUC_DB_PASSWORD', 'escorpio1')
  host = os.getenv('PUC_DB_HOST', '10.30.205.101')
  port = os.getenv('PUC_DB_PORT', '1521')
  sid = os.getenv('PUC_DB_SID', 'padr')
  url = "%s:%s/%s"%(host, port, sid)
  db = PucClient(user, passwd, url)

  el_banner = """Consola del Padrón Federal

Consultá al Padron usando el objeto pf con los metodos getPersona, queryByKey, etc.
A su vez podes consultar a la base con el objeto db con los metodos getJson, getImpuestos

Para consultar una persona del padron:

per = pf.getPersona('20181208873')

Para obtenerla de la base:

per_db = db.getJson('20181208873')

Ambos metodos devuelven el objeto json como array asociativo.

Si queres conocer mas sobre estos objetos y sus metodos podes escribir  dir(pf)

Happy hacking!"""
  code.interact(banner=el_banner,local=locals())
