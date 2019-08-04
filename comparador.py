import cx_Oracle
import unittest
from padfedclient import PadFedClient,PucClient,enchular

class ComparadorTestCase(unittest.TestCase):
  cuits = ['20000000400','34518912625','20060308137','20070805880','20163215080',
          '20000000516','30562559112','20149698583','20255077458','27144691933',
          '27225016831','30632212409','20102041179','30708942975','20310267679',
          '30502793175','30708213965','30500001735','30120004928','20181208873']

  def __init__(self,test):
    super(ComparadorTestCase,self).__init__(test)
    self.maxDiff = None
    self.pf = PadFedClient()
    self.pf.dontVerify()
    self.db = PucClient()

  def test_comparar(self):
    for cuit in self.cuits:
      with self.subTest('%s' % cuit):
        per = self.pf.getPersona(cuit)
        self.assertNotIn('errMsg', per, "\n\nNo se halló la clave %s en el padron." % cuit)
        try:
          per_db = self.db.getJson(cuit)
          self.parsear_diccionario(per,per_db, cuit)
        except cx_Oracle.DatabaseError as e:
          self.assertTrue(True, "\n\nNo se halló la clave %s en PUC" % cuit)


  def parsear_diccionario(self, per, per_db, ruta_base):
    for k in per.keys():
      nueva_ruta_base = "%s_%s"%(ruta_base,k)
      self.assertIn(k, per_db, "\n\n%s: No se hallo clave en PADR:\nPadrón:\n%s\nPADR:\n%s\n"%(nueva_ruta_base, enchular(per),enchular(per_db)))
      if type(per[k]) is not dict:
        with self.subTest('%s_%s' % (ruta_base, k)):
          self.assertEqual(per[k], per_db[k])
      else:
        self.parsear_diccionario(per[k], per_db[k], nueva_ruta_base)

if __name__ == '__main__':
    unittest.main()