# PadFedClient

Libreria cliente para Padrón Federal.

Se compone de dos clases:

* PadFedClient
* PucClient

La primera sirve para consultar al Padrón Federal y obtener los datos en formato json manipulable desde un script en python.

La segunda devuelve los arrays de la base de datos con los campos mediante queries e invocación de paquetes.

## Instalación

### Requisitos

Antes de arrancar debemos asegurarnos que están instaladas las siguientes herramientas:

* python3
* pip
* virtualenv (recomendado)

### Arranque

Habiéndonos asegurado las dependencias anteriroes instalamos las librerías que requiere python.

```
$ pip install -r requirements.txt
```

## Usos

El siguiente código consulta al Padrón Federal por una CUIT y trae el objeto en forma de array asociativo. Luego, verifica que tenga algún impuesto con un assert que hay un item 'impuestos' en el array asociativo y por último lo imprime de manera bonita.
 
```python
import padfedclient
import json

pf = padfedclient.PadFedClient()
# no verificamos los certificados
pf.dontVerify()

tieneImpuesto = pf.getPersona('20000000400')
assert 'impuestos' in tieneImpuesto
assert len(tieneImpuesto['impuestos'])>0
print(json.dumps(tieneImpuesto, indent=2, sort_keys=True))
```

Siguiendo el ejemplo uno podría pedirle a la base los impuestos de esa persona y verificar que tenga impuestos y que coincida en cantidad con los del Padrón Federal:

```python
db = padfedclient.PucClient()
imps = db.getImpuestos('20000000400')

assert(len(imps)>0)
assert(len(imps) == len(tieneImpuesto['impuestos']))

print(imps)
```

Por otra parte podemos consultar al GET_JSON de padfed_util mediante el método getJson. Siguiendo el mismo script:

```python
imps_get_json = db.getJson('20000000400')['impuestos']

# imps_get_json es un array asociativo.

imps_get_json.values() 
assert(len(imps_get_json)>0)

# esta comparación es más fuerte: compara diccionario a diccionario!
assert(imps_get_json == tieneImpuesto['impuestos'])

print(json.dumps(tieneImpuesto, indent=2))
```

## Estructura general

Actualmente el proyecto se compone de un sólo paquete que tiene las dos clases. Una clase para obtener datos de la base y otra del padrón. Cada consulta está mapeada por un método.

Se podría agregar entidades para forzar el chequeo de formato de los registros devueltos. 
  
## Colaborar

Sos bienvenida a colaborar reportando algún issue en el repositorio o bien forkeando el repositorio y proponiendo mediante un merge request y agregando funcionalidad y documentación.


## Usando docker

### Como correr el script incluido en la imagen docker

~~~
docker run -it padfedclient
~~~

### Como correr un script python fuera de la imagen

~~~
docker run -it -v "$PWD":/usr/src/myapp -w /usr/src/myapp padfedclient:latest python padfedclient.py 
~~~

### Generar nuevamente la imagen docker

~~~
docker build  --network=host --build-arg HTTP_PROXY=http://10.30.28.25:80 --build-arg HTTPS_PROXY=http://10.30.28.25:80 -t padfedclient .
~~~

### Guardar imagen localmente

~~~
docker save  padfedclient | gzip > padfedclient-latest.tar
~~~

### Importar la imagen en un equipo

~~~
 docker load < padfedclient-latest.tar
~~~
 