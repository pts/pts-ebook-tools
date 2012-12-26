#! /bin/sh --
""":" # Rebuilds (parts of) metadata.db from the metadata.opf files.

exec calibre-debug -e "$0" ${1+"$@"}

Don't run this script while somebody else (e.g. Calibre) is modifying the
library. If you do anyway, you may lose some data. To be safe, exit from
Calibre while this script is running.
!! TODO(pts): Verify this claim, use EXCLUSIVE locking.

TODO(pts): Renumber books with a conflicting ID.
"""

# by pts@fazekas.hu at Wed Dec 26 12:54:02 CET 2012
__author__ = 'pts@fazekas.hu (Peter Szabo)'

import cStringIO
import os
import os.path
import re
import sqlite3
import sys

import calibre
from calibre.library import database2
from calibre.ebooks.metadata import opf2

# Please note that `assert' statements are ignored in this script.
# It's too late to change it back.


def decode_unicode(data):
  if isinstance(data, str):
    return data
  elif isinstance(data, unicode):
    if u'\xfffd' in data:
      raise AssertionError('Uknown character in %r. Please set up system '
                           'locale properly, or use ASCII only.' % data)
    return data.decode(calibre.preferred_encoding)
  else:
    raise TypeError


SQLITE_KEYWORDS = frozenset((
    'ABORT', 'ACTION', 'ADD', 'AFTER', 'ALL', 'ALTER', 'ANALYZE', 'AND',
    'AS', 'ASC', 'ATTACH', 'AUTOINCREMENT', 'BEFORE', 'BEGIN', 'BETWEEN',
    'BY', 'CASCADE', 'CASE', 'CAST', 'CHECK', 'COLLATE', 'COLUMN', 'COMMIT',
    'CONFLICT', 'CONSTRAINT', 'CREATE', 'CROSS', 'CURRENT_DATE',
    'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'DATABASE', 'DEFAULT',
    'DEFERRABLE', 'DEFERRED', 'DELETE', 'DESC', 'DETACH', 'DISTINCT',
    'DROP', 'EACH', 'ELSE', 'END', 'ESCAPE', 'EXCEPT', 'EXCLUSIVE',
    'EXISTS', 'EXPLAIN', 'FAIL', 'FOR', 'FOREIGN', 'FROM', 'FULL', 'GLOB',
    'GROUP', 'HAVING', 'IF', 'IGNORE', 'IMMEDIATE', 'IN', 'INDEX',
    'INDEXED', 'INITIALLY', 'INNER', 'INSERT', 'INSTEAD', 'INTERSECT',
    'INTO', 'IS', 'ISNULL', 'JOIN', 'KEY', 'LEFT', 'LIKE', 'LIMIT', 'MATCH',
    'NATURAL', 'NO', 'NOT', 'NOTNULL', 'NULL', 'OF', 'OFFSET', 'ON', 'OR',
    'ORDER', 'OUTER', 'PLAN', 'PRAGMA', 'PRIMARY', 'QUERY', 'RAISE',
    'REFERENCES', 'REGEXP', 'REINDEX', 'RELEASE', 'RENAME', 'REPLACE',
    'RESTRICT', 'RIGHT', 'ROLLBACK', 'ROW', 'SAVEPOINT', 'SELECT', 'SET',
    'TABLE', 'TEMP', 'TEMPORARY', 'THEN', 'TO', 'TRANSACTION', 'TRIGGER',
    'UNION', 'UNIQUE', 'UPDATE', 'USING', 'VACUUM', 'VALUES', 'VIEW',
    'VIRTUAL', 'WHEN', 'WHERE'))
"""Copied from http://www.sqlite.org/lang_keywords.html"""


SQLITE_BARE_NAME_RE = re.compile(r'[_a-zA-Z]\w*\Z')


def escape_sqlite_name(name):
  if isinstance(name, unicode):
    name = name.decode('UTF-8')
  elif isinstance(name, str):
    name.encode('UTF-8')  # Just to generate UnicodeEncodeError.
  else:
    raise TypeError
  if '\0' in name:
    raise ValueError('NUL in SQLite name: %r' % name)
  if SQLITE_BARE_NAME_RE.match(name) and name.upper() not in SQLITE_KEYWORDS:
    return name
  else:
    return '"%s"' % name.replace('"', '""')


def usage(argv0):
  return ('Rebuilds (parts of) metadata.db from the metadata.opf files.\n'
          'Usage: %s [<calibre-library-dir>]' % argv0)


BOOK_TABLES = (
    'books', 'authors', 'books_authors_link', 'books_languages_link',
    'books_plugin_data', 'books_publishers_link', 'books_ratings_link',
    'books_series_link', 'books_tags_link', 'comments', 'conversion_options',
    'data', 'identifiers', 'languages', 'publishers', 'ratings', 'series',
    'tags', 'metadata_dirtied')
"""The order is irrelevant."""


def main(argv):
  if (len(argv) > 1 and argv[1] in ('--help', '-h')):
    print usage(argv[0])
    sys.exit(0)
  if len(argv) > 2:
    print >>sys.stderr, usage()
    sys.exit(1)
  dbdir = argv[1].rstrip(os.sep) if len(argv) > 1 else '.'
  if os.path.isfile(dbdir):
    dbname = dbdir
  else:
    dbname = os.path.join(dbdir, 'metadata.db')
  del dbdir
  dbpath = os.path.abspath(dbname)
  print >>sys.stderr, 'info: Rebuilding Calibre database: ' + dbpath
  if not os.path.isfile(dbname):
    raise AssertionError('Calibre database missing: %s' % dbpath)
  dbconn = sqlite3.connect(dbname, check_same_thread=False,
                         isolation_level='EXCLUSIVE')
  dbconn.text_factory = str
  dc = dbconn.cursor()
  dc.execute('PRAGMA synchronous=OFF')
  dc.execute('PRAGMA journal_mode=MEMORY')
  dc.execute('PRAGMA temp_store=MEMORY')
  dc.execute('PRAGMA cache_size=-16384')  # 16 MB.
  dc.execute('BEGIN EXCLUSIVE')  # Locks the file immediately.
  encoding = tuple(dc.execute('PRAGMA encoding'))[0][0].upper()
  if encoding not in ('UTF8', 'UTF-8'):
    # TODO(pts): Maybe UTF16-le etc. also work.
    raise RuntimeError('Unsupported database encoding: %s' % encoding)
  master_by_type = {'table': [], 'index': [], 'view': [], 'trigger': []}
  tables_to_copy = []
  # SELECT type, name, tbl_name, sql FROM sqlite_master;
  for row in dc.execute(
      'SELECT type, sql, name FROM sqlite_master ORDER BY tbl_name, name'):
    if row[0] not in ('index', 'table', 'trigger', 'view'):
      raise AssertionError('Bad type in master: %r' % row[0])
    if not row[2].startswith('sqlite_'):  # e.g. sqlite_sequence.
      # KeyError is deliberate if type (row[0]) is not known.
      master_by_type[row[0]].append(row[1])
    if (row[0] == 'table' and row[2] not in BOOK_TABLES and
        (not row[2].startswith('sqlite_') or row[2] == 'sqlite_sequence')):
      # Don't copy sqlite_stat1 etc.
      tables_to_copy.append(row[2])
  # tables_to_copy for Calibre 0.9.11 is ['custom_columns', 'feeds',
  # 'library_id', 'preferences'].

  newdbname = 'metare.db'
  print >>sys.stderr, 'info: Creating new database: %s' % newdbname
  # TODO(pts): Do full locking on this file.
  open(newdbname, 'w').truncate(0)
  conn = sqlite3.connect(newdbname, check_same_thread=False,
                         isolation_level='EXCLUSIVE')
  conn.text_factory = str
  c = conn.cursor()
  c.execute('PRAGMA synchronous=OFF')
  c.execute('PRAGMA journal_mode=OFF')
  c.execute('PRAGMA count_changes=OFF')
  c.execute('PRAGMA temp_store=MEMORY')
  c.execute('PRAGMA cache_size=-16384')  # 16 MB.
  c.execute('PRAGMA encoding=%s' % escape_sqlite_name(encoding))
  # TODO(pts): Make sure we don't release the lock before or after
  # 'CREATE TABLE' etc. Maybe lock the file descriptor manually. (Can we do
  # that in Python?).
  c.execute('BEGIN EXCLUSIVE')  # Locks the file immediately.
  for sql in master_by_type['table']:
    c.execute(sql)  # 'CREATE TABLE ...'.

  # Copy the non-book tables.
  for table in tables_to_copy:
    table_esc = escape_sqlite_name(table)
    dc.execute('SELECT * FROM %s' % table_esc)
    print table_esc
    if table == 'sqlite_sequence':
      if (len(dc.description) < 2 or
          dc.description[0][0] != 'name' or
          dc.description[1][0] != 'seq'):
        raise AssertionError
      rows = []
      for row in dc:
        if not isinstance(row[0], str):
          raise AssertionError
        if row[0] not in BOOK_TABLES:
          rows.append(row)
    else:
      rows = list(dc)  # Small amount of data, fits in memory.
    sql = 'INSERT INTO %s VALUES (%s)' % (
        table_esc, ','.join(['?'] * len(dc.description)))
    for row in rows:
      # TODO(pts): Rewrite all c.execute(...) to creating a .dump file
      # manually, and running it with c.executescript(...). Measure if it is
      # actually faster.
      c.execute(sql, row)

  # Add functions and aggregates needed by the indexes, views and triggers
  # below.
  #
  # TODO(pts): Automate these by catching `OperationalError: no such
  #   function: title_sort' etc.
  conn.create_aggregate('sortconcat', 2, None)
  conn.create_function('concat', 1, None)
  conn.create_function('books_list_filter', 1, None)
  conn.create_function('title_sort', 1, None)

  # Create indexes, views and triggers.
  #
  # It's important to create the indexes after the INSERT INTO operations, so
  # the INSERT INTO operations become fast.
  conn.commit()
  for sql in master_by_type['index']:
    c.execute(sql)  # 'CREATE INDEX ...'.
  for sql in master_by_type['view']:
    c.execute(sql)  # 'CREATE VIEW ...'.
  for sql in master_by_type['trigger']:
    c.execute(sql)  # 'CREATE TRIGGER ...'.

  print >>sys.stderr, 'info: Generating statistics.'
  c.execute('ANALYZE')
  conn.commit()
  conn.close()
  print >>sys.stderr, 'info: Done.'


if __name__ == '__main__':
  # SUXX: Original, byte argv not available.
  main(map(decode_unicode, sys.argv))
