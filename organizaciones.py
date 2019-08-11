COMARB = 900
COMARB = 900

ORGANIZACIONES = {
  900: { "org": 900, "provincia": -1},
  901: { "org": 901, "provincia":  0},
  902: { "org": 902, "provincia":  1},
  903: { "org": 903, "provincia":  2},
  904: { "org": 904, "provincia":  3},
  905: { "org": 905, "provincia":  4},
  906: { "org": 906, "provincia":  5},
  907: { "org": 907, "provincia":  6},
  908: { "org": 908, "provincia":  7},
}

# TODO: Agregar lista completa de impuestos extra AFIP
DICT_IMP_ORG = {
  5900: 900,
  5901: 901,
  5902: 902
}

dict_provincia_org = dict()

def get_org_by_provincia(provincia: int) -> dict:
    if len(dict_provincia_org) == 0:
       for k,v in ORGANIZACIONES: 
           if v.get("provincia") > -1:
              dict_provincia_org[v.get("provincia")] = k
    
    return dict_provincia_org.get(provincia, -1)
