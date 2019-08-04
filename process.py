import csv

INSERT="""
INSERT INTO HLF.BC_VALID_TX_WRITE_SET(
BLOCK,
TXSEQ,
ITEM,
KEY,
VALUE,
IS_DELETE)
VALUES (
:block,
:txseq,
:item,
:key,
:value,
:is_delete)"""
 
    

i = 0
with open("writeset-4.txt", "r", encoding="ISO-8859-1") as f: 
    reader = csv.reader(f)
    for row in reader:
          i = i + 1 
          print( "row %d" % i )
          
          
         

          if i > 10:
              break

      
