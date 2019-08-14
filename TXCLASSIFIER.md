# TX CLASSIFIER 

Script python clasificador de txs de **PADFED** registradas en una base de datos Oracle cargada por **Blockconsumer**.

Se compone de los siguientes archivos:

| archivo             | desc                                       |
| ------------------- | ------------------------------------------ |
| `txclasiffier.py`   | main                                       |
| `txclasiffier.conf` | archivos de configuración                  |
| `wset.py`           | clase que representa a un writeset         |
| `organizaciones.py` | estructuras con organizaciones e impuestos |

### Ejecución

``` 
./txsclassifier.py
``` 

Cada ejecución genera los siguientes archivos:

| archivo                        | desc                           |
| ------------------------------ | ------------------------------ |
| `txclasiffier.dia-hora.output` | resultados de la clasificación |
| `txclasiffier.dia-hora.log`    | archivo de log                 |

#### Configuración

El script se configura mediante el archivo `txclasiffier.conf` 

| section     | option                     | desc                                                                                                                                                                                     |
| ----------- | -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db`        | `user`, `password` y `url` | Parámetros para establecer conexión con un usuario de una base Oracle que tenga privilegios de lectura sobre la tabla `HLF.BC_VALID_TX_WRITE_SET`.                                       |
| `behaviour` | `logging_level`            | puede ser `info` o `debug`.                                                                                                                                                              |
| `behaviour` | `target_orgs`              | Códigos de las orgs de interés. Si queda vacia significa **todas**. Si se informa una lista, los códigos deben estar separados por coma y encerrados entre corchetes. Ej: `[ 900, 904 ]` |
| `filters`   | `block_start`              | Número de bloque inicial. Es obligatorio.                                                                                                                                                |
| `filters`   | `block_stop`               | Número de bloque final. Opcional.                                                                                                                                                        |
| `filters`   | `block_processed`          | Número de bloque procesado. Lo va actualizando automáticamente el script, cada 100 bloques procesados.                                                                                   |

Ejemplo:

``` 
[db]
user = BC_ROSI
password = xxxx
url = 10.30.205.101/padr

[behaviour]
logging_level = debug
target_orgs = 

[filters]
block_start = 228566
block_stop = 
block_processed = 
``` 
### Nota: Tesnet

En la Testnet el primer bloque con información consistente de PADFED es el `228566` que tiene una tx de **COMARB** con impuesto `5900` migrada desde un **F1275**.

Entonces, para clasificar las txs de la Testnet se puede configurar `block_start = 228566` para que comience desde ese número de bloque.

Si se procesa solamente ese bloque (`block_start = 228566` y `block_stop = 228566`) se obtiene el siguiente resultado:

```
20190814-175721 - txclassifier.py running - output_file txclassifier.20190814-175721.output ...
reading config from txclassifier.conf ...
db.url: BC_ROSI
db.user: 10.30.205.101/padr
filters.block_start: 228566
filters.block_stop: 228566
filters.block_processed: 228566
filters.target_orgs: dict_keys([900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 912, 913, 914, 915, 916, 917, 918, 919, 920, 921, 922, 923, 924])
{'block': 228566, 'txseq': 0, 'personaid': 27269391702, 'org': 900, 'kind': 'MIGRACON'}
{'block': 228566, 'txseq': 0, 'personaid': 27269391702, 'org': 901, 'kind': 'MIGRACON DESDE CM'}
{'block': 228566, 'txseq': 0, 'personaid': 27269391702, 'org': 903, 'kind': 'MIGRACON DESDE CM'}
{'block': 228566, 'txseq': 0, 'personaid': 27269391702, 'org': 901, 'kind': 'CAMBIO EN ACTIVIDAD'}
{'block': 228566, 'txseq': 0, 'personaid': 27269391702, 'org': 901, 'kind': 'CAMBIO EN DOMIROL'}
{'block': 228566, 'txseq': 0, 'personaid': 27269391702, 'org': 903, 'kind': 'CAMBIO EN ACTIVIDAD'}
{'block': 228566, 'txseq': 0, 'personaid': 27269391702, 'org': 903, 'kind': 'CAMBIO EN DOMIROL'}
20190814-175723 - txclassifier.py success end
20190814-175723 - txclassifier.py stop
``` 
