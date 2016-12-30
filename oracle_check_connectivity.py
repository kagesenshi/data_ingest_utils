import cx_Oracle as ora
import sys
import csv
import traceback
from datetime import datetime

def now():
    return datetime.now().isoformat()

if len(sys.argv) != 2:
   print "%s <data_sources_list.csv>\n\nExpected format:\nname,ip,port,tns,schema,login,password" % sys.argv[0]

for ds in csv.DictReader(open(sys.argv[1]), delimiter='\t'):
    constr = '%(login)s/%(password)s@//%(ip)s:%(port)s/%(tns)s' % ds
    print '(%s) CONNECTING TO %s %s' % (now(), ds['name'], constr)
    try:
        con = ora.connect(constr)
        cur = con.cursor()
        res = cur.execute("select owner, table_name from all_tables where owner='%s'" % ds['schema'])
        tables = []
        for r in res:
            tables.append(r[1])
        print ','.join(tables) + '\n'
        res.close()
        con.close()
    except Exception, e:
        print e
        continue

