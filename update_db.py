#! /bin/sh --
""":" # Updates metadata.db from the metadata.opf files.

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
from calibre.customize import builtins
from calibre.library import database2
from calibre.library import sqlite
from calibre.ebooks.metadata import opf2

# Please note that `assert' statements are ignored in this script.
# It's too late to change it back.


def encode_unicode(data):
  if isinstance(data, str):
    return data
  elif isinstance(data, unicode):
    if u'\xfffd' in data:
      raise AssertionError('Uknown character in %r. Please set up system '
                           'locale properly, or use ASCII only.' % data)
    return data.encode(calibre.preferred_encoding)
  else:
    raise TypeError(type(data))


def encode_utf8(data):
  if isinstance(data, str):
    return data
  elif isinstance(data, unicode):
    return data.encode('UTF-8')
  elif data is None:
    return data
  else:
    raise TypeError(type(data))


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


def get_extensions(builtins_module):
  mrp = builtins.MetadataReaderPlugin
  extensions = set()
  for name in dir(builtins_module):
    class_obj = getattr(builtins_module, name)
    if type(class_obj) == type(mrp) and issubclass(class_obj, mrp):
      for extension in class_obj.file_types:
        extensions.add(extension.lower().lstrip('.'))
  for extension in extensions:
    if '.' in extensions:
      raise AssertionError(repr(extension))
  extensions.discard('opf')
  # Example: 'rtf', 'prc', 'azw1', 'odt', 'cbr', 'pml', 'rar', 'cbz', 'snb',
  # 'htmlz', 'txt', 'updb', 'zip', 'oebzip', 'chm', 'lit', 'imp', 'html',
  # 'rb', 'fb2', 'docx', 'azw3', 'azw4', 'txtz', 'lrf', 'tpz', 'opf', 'lrx',
  # 'epub', 'mobi', 'pmlz', 'pobi', 'pdf', 'pdb', 'azw'.
  return extensions


EXTENSIONS = frozenset(get_extensions(builtins))


def is_correct_book_filename(filename, opf_dir):
  title = os.path.basename(opf_dir)
  author = os.path.basename(os.path.dirname(opf_dir))
  i = filename.rfind('.')
  if i < 0:
    raise AssertionError(repr(filename))
  j = title.rfind(' (')  # 'name (id)'.
  if j < 0:
    raise AssertionError(repr(title))
  filename = filename[:i]
  title = title[:j]
  expected_filename = '%s - %s' % (title, author)
  return filename == expected_filename


def noproxy_connect(dbpath, row_factory=None):
  """Will override sqlite.connect."""
  db_thread = sqlite.DBThread(dbpath, row_factory)
  db_thread.connect()
  db_thread.conn.isolation_level = 'EXCLUSIVE'
  db_thread.conn.execute('PRAGMA temp_store=MEMORY')
  db_thread.conn.execute('PRAGMA cache_size=-16384')  # 16 MB.
  return db_thread.conn


def _connection__create_dynamic_filter(self, name):
  f = sqlite.DynamicFilter(name)
  self.create_function(name, 1, f)
  return f


def _connection__commit(self):
  pass


def _connection__real_commit(self):
  return super(type(self), self).commit()


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
    dbdir = os.path.dirname(dbdir)
  else:
    dbname = os.path.join(dbdir, 'metadata.db')
  print >>sys.stderr, 'info: Updating Calibre database: ' + dbname
  if not os.path.isfile(dbname):
    raise AssertionError('Calibre database missing: %s' % dbname)

  sqlite.Connection.create_dynamic_filter = _connection__create_dynamic_filter
  sqlite.Connection.commit = _connection__commit
  sqlite.Connection.real_commit = _connection__real_commit
  database2.connect = sqlite.connect = noproxy_connect
  db = database2.LibraryDatabase2(dbdir)  # Slow, reads metadata.db.
  #print db  # <calibre.library.database2.LibraryDatabase2 object at 0x1e4db90>
  #print db.conn  # <calibre.library.sqlite.ConnectionProxy object at 0x1e4de90>
  #print db.conn.proxy  # <DBThread(Thread-1, started daemon 140138955478784)>
  #print db.conn.proxy.conn  # <calibre.library.sqlite.Connection object at 0x1ea7b50>
  #print db.data._data

  db.conn.real_commit()
  db.conn.close()
  print >>sys.stderr, 'info: Done.'


if __name__ == '__main__':
  # SUXX: Original, byte argv not available.
  main(map(encode_unicode, sys.argv))
