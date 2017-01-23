import cx_Oracle as ora
import sys
import csv
import traceback
from datetime import datetime
from pprint import pprint
import getpass
import os
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
            'profiling_metadata': {
                'user': getpass.getuser(),
                'working_directory': os.getcwd(),
                'environment': dict(os.environ)
            }
        }
        # we dont need the terminal color variable
        if self.result['profiling_metadata']['environment'].get('LS_COLORS', None):
           del self.result['profiling_metadata']['environment']['LS_COLORS']


    def update(self):
        self.result['profiling_metadata']['start_dt'] = now()
        for t in range(3):
            try:
                con = ora.connect(self.connstr)
            except Exception, e:
                traceback.print_exc()
                self.result['error'] = str(e)
                self.result['profiling_metadata']['end_dt'] = now()
                if t != 2:
                    print "Retrying..."
                    continue
                return 
        cur = con.cursor()
        self._tables = self._get_tables(cur)
        for t in self._tables:
            self.result['tables'].append(self._profile_table(cur, t))

        self.result['profiling_metadata']['end_dt'] = now()
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
        num_rows, avg_row_len = self._get_row_stats(cursor, table_name)
        estimated_size = (None if (
            num_rows is None or avg_row_len is None) else 
            num_rows * avg_row_len)

        indexed_columns = self._get_indexes(cursor, table_name)
        unique_indexes = [i['field'] for i in indexed_columns if i['uniqueness'] == 'UNIQUE']
        primary_keys = self._get_primary_keys(cursor, table_name)
        unique_keys = self._get_unique_keys(cursor, table_name)
        non_key_cols = []
        split_by = self._get_split_by(columns, indexed_columns, primary_keys, unique_keys)
        for col in columns:
            if col in primary_keys:
               continue
            if col in unique_keys:
               continue
            non_key_cols.append(col)
        merge_key = self._get_merge_key(columns, primary_keys, unique_keys, unique_indexes)
        check_column = self._get_check_column(indexed_columns, columns)
        return {
            'table': table_name,
            'columns': columns,
            'primary_keys': primary_keys,
            'unique_keys': unique_keys,
            'indexed_columns': indexed_columns,
            'no_key': False if (primary_keys or unique_keys) else True,
            'split_by': split_by,
            'num_rows': num_rows,
            'avg_row_len': avg_row_len,
            'estimated_size': estimated_size,
            'unique_indexes': unique_indexes,
            'merge_key': merge_key,
            'check_column': check_column,
        }

    def _get_check_column(self, indexed_columns, columns):
        idxed = [i['field'] for i in indexed_columns]
        for col in columns:
            for kw in ['LAST_UPD', 'MOD_T']:
                if (kw in col['field'] and col['field'] in idxed
                            and col['type'] in ['DATE','NUMBER']):
                    return col['field']
        for col in columns:
            for kw in ['CREATED']:
                if (kw in col['field'] and col['field'] in idxed
                            and col['type'] in ['DATE','NUMBER']):
                    return col['field']

        for col in columns:
            for kw in ['LAST_UPD', 'MOD_T']:
                if kw in col['field']:
                    return col['field']

        for col in columns:
            for kw in ['CREATED']:
                if kw in col['field']:
                    return col['field']


        return None

    def _get_row_stats(self, cursor, table_name):
        res = cursor.execute("""
        select num_rows, avg_row_len from all_tables where table_name = '%s'
        """ % table_name)
        for r in res:
            return r
        return None, None


    def _get_merge_key(self, columns, pkeys, ukeys, uixs):
        for col in columns:
            if ((col['field'] in uixs) or
                (col['field'] in ukeys) or
                (col['field'] in pkeys)):
                return col['field']
        return None

    def _get_split_by(self, columns, indexes, pkeys, ukeys):
        for c in [i for i in columns if i['field'] in pkeys]:
            if c['type'] == 'DATE':
               return c['field']
        for c in [i for i in columns if i['field'] in pkeys]:
            if c['type'] == 'NUMBER':
               return c['field']
        for c in [i for i in columns if i['field'] in ukeys]:
            if c['type'] == 'DATE':
               return c['field']
        for c in [i for i in columns if i['field'] in ukeys]:
            if c['type'] == 'NUMBER':
               return c['field']
        index_names = [i['field'] for i in indexes]
        for c in [i for i in columns if i['field'] in index_names]:
            if c['type'] == 'DATE':
               return c['field']
            if c['type'] == 'NUMBER':
               return c['field']
        for c in columns:
            if c['type'] == 'DATE':
               return c['field']
        for c in columns:
            if c['type'] == 'NUMBER':
               return c['field']
        return None



    def _get_columns(self, cursor, table_name):
        _get_columns_sql = '''
           SELECT cols.column_name, cols.data_type, cols.column_id
           FROM all_tab_columns cols
           WHERE cols.table_name = '%s'
           AND cols.owner = '%s'
        ''' % (table_name, self.ds['schema'])
        res = cursor.execute(_get_columns_sql)
        cols = []
        for col in res:
            cols.append({'field': col[0], 'type': col[1], 'id': col[2]})

        _get_columns_comment = '''
           SELECT cols.column_name, cols.comments
           FROM all_col_comments cols
           WHERE cols.table_name = '%s'
           AND cols.owner = '%s'
        ''' % (table_name, self.ds['schema'])

        res = cursor.execute(_get_columns_comment)
        comments = {}
        for cmt in res:
            comments[cmt[0]] = cmt[1]
        
        for col in cols:
            col['comment'] = comments[col['field']]

        return sorted(cols, key=lambda x: x['id'])


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

    def _get_indexes(self, cursor, table_name):
        _get_index_sql = '''
            SELECT cidxs.table_name, cidxs.column_name, cidxs.index_name, 
                    idxs.uniqueness
            FROM all_ind_columns cidxs, all_indexes idxs
            WHERE idxs.index_name = cidxs.index_name
            AND idxs.table_name = '%s'
        ''' % table_name

        res = cursor.execute(_get_index_sql)
        idxs = []
        for col in res:
            idxs.append({ 'field': col[1],
                           'uniqueness': col[3]})
        return idxs

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
