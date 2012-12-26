#! /bin/sh --
""":" Creates missing metadata.opf files from metadata.db

exec calibre-debug -e "$0" ${1+"$@"}

Don't run this script while somebody else (e.g. Calibre) is modifying the
library. If you do anyway, you may lose some data.
"""

# by pts@fazekas.hu at Sun Dec 23 11:14:15 CET 2012
__author__ = 'pts@fazekas.hu (Peter Szabo)'

import cStringIO
import os
import os.path
import sys

import calibre
from calibre.library import database2
from calibre.ebooks.metadata import opf2

# Please note that `assert' statements are ignored in this script.


def encode_unicode(data):
  if isinstance(data, str):
    return data
  elif isinstance(data, unicode):
    if u'\xfffd' in data:
      raise AssertionError('Uknown character in %r. Please set up system '
                           'locale properly, or use ASCII only.' % data)
    return data.encode(calibre.preferred_encoding)
  else:
    raise TypeError


def usage(argv0):
  return ('Creates missing metadata.opf files from metadata.db\n'
          'Usage: %s [<calibre-library-dir>]' % argv0)


def main(argv):
  if (len(argv) > 1 and argv[1] in ('--help', '-h')):
    print usage(argv[0])
    sys.exit(0)
  if len(argv) > 2:
    print >>sys.stderr, usage()
    sys.exit(1)
  dbdir = argv[1].rstrip(os.sep) if len(argv) > 1 else '.'
  dbname = os.path.join(dbdir, 'metadata.db')
  dbpath = os.path.abspath(dbname)
  print >>sys.stderr, 'info: Reading Calibre database: ' + dbpath
  if not os.path.isfile(dbname):
    raise AssertionError('Calibre database missing: %s' % dbpath)
  dbdirpath = os.path.dirname(dbpath)
  # Reads the whole metadata.db to memory.
  db = database2.LibraryDatabase2(dbdirpath)
  fm = db.FIELD_MAP
  fm_path = fm['path']
  # TODO(pts): Do we have to replace / with os.sep on Windows?
  book_path_items = [(i, decode_unicode(row[fm_path]))
     for i, row in enumerate(db.data._data) if row is not None]
  print >>sys.stderr, 'info: Found %d book%s in metadata.db.' % (
      len(book_path_items), 's' * (len(book_path_items) != 1))
  book_opf_items = []
  for i, book_path in book_path_items:
    opf_path = os.path.join(dbdirpath, book_path, 'metadata.opf')
    if not os.path.exists(opf_path):
      book_opf_items.append((i, opf_path))
  print >>sys.stderr, 'info: Found %d book%s with missing metadata.opf.' % (
      len(book_opf_items), 's' * (len(book_opf_items) != 1))
  # TODO(pts): Find directories not corresponding to any books in the database.
  for i, opf_path in book_opf_items:
    mi = db.get_metadata(i, index_is_id=True)
    print >>sys.stderr, 'info: Creating metadata.opf: %s' % opf_path
    # This creates a very different .opf.
    #   opf_creator = opf2.OPFCreator(os.path.dirname(opf_path), mi)
    #   sio = cStringIO.StringIO()
    #   opf_creator.render(sio)
    if mi.has_cover and not mi.cover:
      mi.cover = 'cover.jpg'
    data = opf2.metadata_to_opf(mi)
    with open(opf_path, 'wb') as f:
      f.write(data)
  print >>sys.stderr, 'info: metadata.opf creation done.'


if __name__ == '__main__':
  # SUXX: Original, byte argv not available.
  main(map(encode_unicode, sys.argv))
