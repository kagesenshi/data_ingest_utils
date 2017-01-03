import cx_Oracle as ora
import sys
import csv
import traceback
from datetime import datetime
from pprint import pprint
import json

def now():
    return datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

if len(sys.argv) != 2:
   print "%s <data_sources_list.csv>\n\nExpected format:\nname,ip,port,tns,schema,login,password" % sys.argv[0]


class OracleProfiler(object):

    def __init__(self, name, ip, port, tns, schema, login, password):
        self.ds = {
            'name': name,
            'ip': ip,
            'port': port,
            'tns': tns,
            'schema': schema,
            'login': login,
            'password': password
        }
        self.connstr = '%(login)s/%(password)s@//%(ip)s:%(port)s/%(tns)s' % (
                    self.ds)
        self._tables = []
        self.result = {
            'connection_string': self.connstr,
            'datasource': self.ds,
            'tables': [],
        }


    def update(self):
        start_date = now()
        try:
            con = ora.connect(self.connstr)
        except Exception, e:
            traceback.print_exc()
            self.result['error'] = str(e)
            self.result['profiling_metadata'] = {
                   'start_dt': start_date,
                   'end_dt': now()
            }
            return 
        cur = con.cursor()
        self._tables = self._get_tables(cur)
        for t in self._tables:
            self.result['tables'].append(self._profile_table(cur, t))

        self.result['profiling_metadata'] = {
                'start_dt': start_date,
                'end_dt': now()
        }
        con.close()

    def _get_tables(self, cursor):
        res = cursor.execute("""
        select owner, table_name from all_tables where owner='%s'
        """ % self.ds['schema'])
        tables = []
        for r in res:
            tables.append(r[1])
        return tables

    def _profile_table(self, cursor, table_name):
        start_date = now()
        columns = self._get_columns(cursor, table_name)
        primary_keys = self._get_primary_keys(cursor, table_name)
        unique_keys = self._get_unique_keys(cursor, table_name)
        non_key_cols = []
        split_by = self._get_split_by(columns, primary_keys, unique_keys)
        for col in columns:
            if col in primary_keys:
               continue
            if col in unique_keys:
               continue
            non_key_cols.append(col)
        return {
            'table': table_name,
            'columns': columns,
            'primary_keys': primary_keys,
            'unique_keys': unique_keys,
            'no_key': False if (primary_keys or unique_keys) else True,
            'split_by': split_by,
        }

    def _get_split_by(self, columns, pkeys, ukeys):
        for c in [i for i in columns if i[0] in pkeys]:
            if c[1] == 'NUMBER':
               return c
        for c in [i for i in columns if i[0] in pkeys]:
            if c[1] == 'DATE':
               return c
        for c in [i for i in columns if i[0] in ukeys]:
            if c[1] == 'NUMBER':
               return c
        for c in [i for i in columns if i[0] in ukeys]:
            if c[1] == 'DATE':
               return c
        for c in columns:
            if c[1] == 'DATE':
               return c
        for c in columns:
            if c[1] == 'NUMBER':
               return c
        return (None, None)



    def _get_columns(self, cursor, table_name):
        _get_columns_sql = '''
           SELECT cols.table_name, cols.column_name, cols.data_type
           FROM all_tab_columns cols
           WHERE cols.table_name = '%s'
           AND cols.owner = '%s'
        ''' % (table_name, self.ds['schema'])
        res = cursor.execute(_get_columns_sql)
        cols = []
        for col in res:
            cols.append((col[1],col[2]))
        return cols

    def _get_primary_keys(self, cursor, table_name):
        _get_primary_key_sql = '''
            SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
            FROM all_constraints cons, all_cons_columns cols
            WHERE cols.table_name = '%s' 
            AND cons.constraint_type = 'P'
            AND cons.constraint_name = cols.constraint_name
            AND cons.owner = cols.owner
            ORDER BY cols.table_name, cols.position
        ''' % table_name

        res = cursor.execute(_get_primary_key_sql)
        pk = []
        for col in res:
            pk.append(col[1])
        return pk

    def _get_unique_keys(self, cursor, table_name):
        _get_unique_key_sql = '''
            SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
            FROM all_constraints cons, all_cons_columns cols
            WHERE cols.table_name = '%s' 
            AND cons.constraint_type = 'U'
            AND cons.constraint_name = cols.constraint_name
            AND cons.owner = cols.owner
            ORDER BY cols.table_name, cols.position
        ''' % table_name

        res = cursor.execute(_get_unique_key_sql)
        uk = []
        for col in res:
            uk.append(col[1])
        return uk


    def test_connection(self):
        print '(%s) ANALYZING %s %s' % (now(), self.ds['name'], self.connstr)
        self.update()
        if self.result.get('error', None):
           print self.result['error']

    def save(self):
        res = self.result
        fname = 'profiler-%s-%s.json' % (
                 res['datasource']['name'].replace(' ', '_'), 
                 res['profiling_metadata']['start_dt'])
        with open(fname, 'w') as f:
            f.write(json.dumps(res, indent=4))
        return fname

    def json(self):
        return json.dumps(self.result, indent=4)

result = []
for ds in csv.DictReader(open(sys.argv[1]), delimiter='\t'):
    profiler = OracleProfiler(**ds)
    profiler.test_connection()
    result.append(profiler.result)

fname = 'profiler-output-%s.json' % now()
with open(fname, 'w') as f:
    f.write(json.dumps(result, indent=4))
    print "WRITTEN %s" % fname

