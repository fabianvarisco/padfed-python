COMARB = 900

DEF_ORGANIZACIONES = {
  900: { "org":900, "name":"COMISION ARBITRAL",         "cuit":30658892718, "provincia": -1 },  
  901: { "org":901, "name":"IP - CABA",                 "cuit":34999032089, "provincia":  0 },
  902: { "org":902, "name":"ARBA - BUENOS AIRES",       "cuit":30710404611, "provincia":  1 },
  903: { "org":903, "name":"AGR - CATAMARCA",           "cuit":30668085837, "provincia":  2 },
  904: { "org":904, "name":"TAS CORDOBA",               "cuit":30999256712, "provincia":  3 },
  905: { "org":905, "name":"DGR - CORRIENTES",          "cuit":30709110078, "provincia":  4 },
  906: { "org":906, "name":"DGR - CHACO",               "cuit":33999176769, "provincia": 16 },
  907: { "org":907, "name":"DGR - CHUBUT",              "cuit":30670499584, "provincia": 17 },
  908: { "org":908, "name":"ATER - ENTRE RIOS",         "cuit":30712147829, "provincia":  5 },
  909: { "org":909, "name":"DGR - FORMOSA",             "cuit":30671355942, "provincia": 18 },
  910: { "org":910, "name":"DPR - JUJUY",               "cuit":30671485706, "provincia":  6 },
  911: { "org":911, "name":"DGR - LA PAMPA",            "cuit":30999278139, "provincia": 21 },
  912: { "org":912, "name":"DGIP - LA RIOJA",           "cuit":30671853535, "provincia": 18 },
  913: { "org":913, "name":"ATM - MENDOZA",             "cuit":30713775505, "provincia":  7 },
  914: { "org":914, "name":"DGR - MISIONES",            "cuit":30672387120, "provincia": 19 },
  915: { "org":915, "name":"DPR - NEUQUEN",             "cuit":30707519092, "provincia": 20 },
  916: { "org":916, "name":"DGR - RIO NEGRO",           "cuit":30712264485, "provincia": 22 },
  917: { "org":917, "name":"DGR - SALTA",               "cuit":30711020264, "provincia":  9 },
  918: { "org":918, "name":"DGR - SAN JUAN",            "cuit":30999015162, "provincia": 10 },
  919: { "org":919, "name":"DPIP - SAN LUIS",           "cuit":30673377544, "provincia": 11 },
  920: { "org":920, "name":"ASIP - SANTA CRUZ",         "cuit":30673639603, "provincia": 23 },
  921: { "org":921, "name":"API - SANTA FE",            "cuit":30655200173, "provincia": 12 },
  922: { "org":922, "name":"DGR - SANTIAGO DEL ESTERO", "cuit":30999164990, "provincia": 19 },
  923: { "org":923, "name":"DGR - TIERRA DEL FUEGO",    "cuit":30715123238, "provincia": 24 },
  924: { "org":924, "name":"DGR - TUCUMAN",             "cuit":30675428081, "provincia": 23 },
}

dict_provincia_org = dict()

def get_org_by_provincia(provincia: int) -> int:
    global dict_provincia_org
    if not dict_provincia_org:
       dict_provincia_org = { v.get("provincia") : k for k, v in DEF_ORGANIZACIONES if v.get("provincia") > -1 } 
    return dict_provincia_org.get(provincia, -1)

DEF_IMPUESTOS = {        
  5900: 900, 
  5901: 901, 
  5902: 902,  
  5903: 903, 
  5904: 904, 
  5905: 905, 
  5906: 906,
  5907: 907, 
  5908: 908, 
  5909: 909, 
  5910: 910,
  5911: 911, 
  5912: 912, 
  5913: 913, 
  5914: 914, 
  5915: 915, 
  5916: 916, 
  5917: 917,
  5918: 918, 
  5919: 919, 
  5920: 920, 
  5921: 921, 
  5922: 922,  
  5923: 923,  
  5924: 924, 
# Fuera del rango 5900
  5243:	904, # IIBB CORDOBA
  5244:	904, # CONTRIB MUNI CORDOBA
  5445:	913, # IIBB MENDOZA
  5446:	913, # CONTRIB MUNI MENDOZA
  5556:	918, # IIBB SAN JUAN
  5557:	918, # CONTRIB MUNI SAN JUAN
  5578:	910, # IIBB JUJUY
  5579:	910, # CONTRIB MUNI JUJUY
}
