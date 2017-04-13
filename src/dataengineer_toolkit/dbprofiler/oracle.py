#!/usr/bin/env python

# FIXME: Oracle / PgSQL Schema is annoying. We should use
# <SOURCE>_<SCHEMA>.<TABLE> in future. In TM currently its
# <SOURCE>.<SCHEMA>_<TABLE> which is not that nice semantically
#

import sys
import csv
import traceback
from datetime import datetime
from pprint import pprint
import getpass
import os
import json
import cx_Oracle as ora
import argparse
from ConfigParser import ConfigParser
import urlparse
from sshtunnel import SSHTunnelForwarder

def now():
    return datetime.now().strftime('%Y-%m-%d-%H-%M-%S')


class OracleProfiler(object):

    def __init__(self, name, ip, port, database, login, password, params,
                 exclude_tables=None, tunnel=None):
        self.ds = {
            'name': name,
            'ip': ip,
            'port': port,
            'tns': database,
            'schema': params['schema'][0],
            'login': login,
            'password': password
        }

        self.exclude_tables = exclude_tables or []

        self.connstr = '%(login)s/%(password)s@//%(ip)s:%(port)s/%(tns)s' % (
            self.ds)

        self.tunnel = tunnel
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
        if self.result['profiling_metadata']['environment'].get(
                'LS_COLORS', None):
            del self.result['profiling_metadata']['environment']['LS_COLORS']

    def update(self):
        if self.tunnel:
            with self.tunnel as tunnel:
                connds = self.ds
                connds = self.ds.copy()
                connds['ip'] = self.tunnel.local_bind_host
                connds['port'] = self.tunnel.local_bind_port

                connstr = '%(login)s/%(password)s@//%(ip)s:%(port)s/%(tns)s' % (
                            connds)
                self._update(connstr)
        else:
            connstr = '%(login)s/%(password)s@//%(ip)s:%(port)s/%(tns)s' % (
                       self.ds)
            self._update(connstr)

    def _update(self, connstr):

        self.result['profiling_metadata']['start_dt'] = now()

        for t in range(3):
            try:
                con = ora.connect(connstr)
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
        self.result['direct'] = self._get_directpermission(cur)
        con.close()

    def _get_tables(self, cursor):
        res = cursor.execute("""
        select owner, table_name from all_tables where owner='%s'
        """ % self.ds['schema'])
        tables = []
        for r in res:
            if '%s.%s' % (self.ds['schema'], r[1]) not in self.exclude_tables:
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
        unique_indexes = [i['field']
                          for i in indexed_columns if 
                          i['uniqueness'] == 'UNIQUE']
        primary_keys = self._get_primary_keys(cursor, table_name)
        unique_keys = self._get_unique_keys(cursor, table_name)
        non_key_cols = []
        split_by = self._get_split_by(
            columns, indexed_columns, primary_keys, unique_keys)
        for col in columns:
            if col in primary_keys:
                continue
            if col in unique_keys:
                continue
            non_key_cols.append(col)
        merge_key = self._get_merge_key(
            columns, primary_keys, unique_keys, unique_indexes)
        check_column = self._get_check_column(indexed_columns, columns)
        return {
            'source': self.ds['name'].replace(' ', '_'),
            'schema': self.ds['schema'],
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
            if col['field'] == 'DB_LAST_UPD':
                continue
            for kw in ['LAST_UPD', 'MOD_T', 'UPDATED_TIME']:
                if (kw in col['field'] and col['field'] in
                        idxed and col['type'] in ['DATE', 'NUMBER']):

                    return col['field']
        for col in columns:
            for kw in ['CREATED']:
                if (kw in col['field'] and col['field'] in 
                        idxed and col['type'] in ['DATE', 'NUMBER']):
                    return col['field']

        for col in columns:
            if col['field'] == 'DB_LAST_UPD':
                continue
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
            if c['type'] == 'NUMBER':
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
            SELECT cols.table_name, cols.column_name, cols.position, cons.status,
                    cons.owner
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
            idxs.append({'field': col[1],
                         'uniqueness': col[3]})
        return idxs

    def _get_unique_keys(self, cursor, table_name):
        _get_unique_key_sql = '''
            SELECT cols.table_name, cols.column_name, cols.position, cons.status, 
                    cons.owner
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

    def _get_directpermission(self, cursor):
        _get_directpermission_sql = '''
            SELECT * FROM V$INSTANCE WHERE ROWNUM < 2
        '''
        try:
            res = cursor.execute(_get_directpermission_sql)
            for col in res:
                return True
        except:
            return False

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


def get_exclude_tables(cfg, ds):

    global_exclude_tables = []
    if cfg.has_option('sources_global', 'exclude_tables'):
        global_exclude_tables = cfg.get('sources_global', 'exclude_tables'
                                        ).strip().split()

    exclude_tables = []
    if cfg.has_option('source:%s' % ds['name'], 'exclude_tables'):
        exclude_tables = cfg.get('source:%s' % ds['name'], 'exclude_tables'
                                 ).strip().split()

    return exclude_tables + ['%s.%s' % (
        ds['params']['schema'][0], i) for i in global_exclude_tables]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config File', default=None)
    args = parser.parse_args()
    result = []

    config = args.config or 'profiler.cfg'
    if not os.path.exists(config):
        raise Exception('Config file "%s" does not exist' % config)

    cfg = ConfigParser()
    cfg.readfp(open(config))

    datasources = []
    for name in [i.split(':')[1] for i in cfg.sections() if 'source:' in i]:
        uris = cfg.get('source:%s' % name, 'dburis').strip().split()
        for uri in uris:
            uridata = urlparse.urlparse(uri)
            qs = urlparse.parse_qs(uridata.query)
            ds = {
                'name': name.upper(),
                'ip': uridata.hostname,
                'port': uridata.port,
                'login': uridata.username,
                'password': uridata.password,
                'database': uridata.path.split('/')[1],
                'params': qs
            }
            exclude_tables = get_exclude_tables(cfg, ds)
            datasources.append(ds)

    tunnel = {}
    if cfg.has_option('tunnel', 'use_tunnel'):
        if cfg.getboolean('tunnel', 'use_tunnel'):
            tunnel['host'] = cfg.get('tunnel', 'host')
            tunnel['port'] = cfg.getint('tunnel', 'port')
            tunnel['user'] = cfg.get('tunnel', 'user')
            tunnel['password'] = cfg.get('tunnel', 'password')

    for ds in datasources:
        if tunnel:
            ds['tunnel'] = SSHTunnelForwarder(
                tunnel['host'],
                ssh_username=tunnel['user'],
                ssh_password=tunnel['password'],
                remote_bind_address=(ds['ip'], ds['port']))
        profiler = OracleProfiler(**ds)
        profiler.test_connection()
        result.append(profiler.result)

    fname = 'profiler-output-%s.json' % now()
    with open(fname, 'w') as f:
        f.write(json.dumps(result, indent=4))
        print "WRITTEN %s" % fname

if __name__ == '__main__':
    main()
