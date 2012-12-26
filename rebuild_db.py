#! /bin/sh --
""":" # Rebuilds (parts of) metadata.db from the metadata.opf files.

exec calibre-debug -e "$0" ${1+"$@"}

Don't run this script while somebody else (e.g. Calibre) is modifying the
library. If you do anyway, you may lose some data. To be safe, exit from
Calibre while this script is running.  !!
"""

# by pts@fazekas.hu at Wed Dec 26 12:54:02 CET 2012
__author__ = 'pts@fazekas.hu (Peter Szabo)'

import cStringIO
import os
import os.path
import sqlite3
import sys

import calibre
from calibre.library import database2
from calibre.ebooks.metadata import opf2

# Please note that `assert' statements are ignored in this script.


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


def usage(argv0):
  return ('Rebuilds (parts of) metadata.db from the metadata.opf files.\n'
          'Usage: %s [<calibre-library-dir>]' % argv0)


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
  conn = sqlite3.connect(dbname, check_same_thread=0,
                         isolation_level='EXCLUSIVE')
  conn.text_factory = str
  print conn.isolation_level
  c = conn.cursor()
  print dir(c)
  print dir(conn)
  #c.execute('BEGIN')
  c.execute('BEGIN EXCLUSIVE')  # Locks the file immediately.
  for row in c.execute('SELECT * FROM sqlite_master'):
    print row
  import time; time.sleep(42)

if __name__ == '__main__':
  # SUXX: Original, byte argv not available.
  main(map(decode_unicode, sys.argv))
