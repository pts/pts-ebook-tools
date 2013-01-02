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
from calibre.ebooks.metadata import opf2
from calibre.library import database2
from calibre.library import sqlite
from calibre.utils import recycle_bin

# Please note that `assert' statements are ignored in this script.
# It's too late to change it back.


def encode_unicode(data):
  if isinstance(data, str):
    return data
  elif isinstance(data, unicode):
    if u'\xfffd' in data:
      raise ValueError('Uknown character in %r. Please set up system '
                       'locale properly, or use ASCII only.' % data)
    return data.encode(calibre.preferred_encoding)
  else:
    raise TypeError(type(data))


def encode_unicode_filesystem(data):
  if isinstance(data, str):
    return data
  elif isinstance(data, unicode):
    if u'\xfffd' in data:
      raise ValueError('Uknown character in file path %r. Please set up system '
                       'locale properly, or rename, or use ASCII only.' % data)
    return data.encode(calibre.filesystem_encoding)
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
  encoding = tuple(db_thread.conn.execute('PRAGMA encoding'))[0][0].upper()
  if encoding not in ('UTF8', 'UTF-8'):
    # TODO(pts): Maybe UTF16-le etc. also work.
    raise RuntimeError('Unsupported database encoding: %s' % encoding)
  db_thread.conn.execute('BEGIN EXCLUSIVE')
  return db_thread.conn


def _connection__create_dynamic_filter(self, name):
  f = sqlite.DynamicFilter(name)
  self.create_function(name, 1, f)
  return f


def _connection__commit(self):
  pass


def _connection__real_commit(self):
  return super(type(self), self).commit()


def get_db_opf(db, book_id):
  mi = db.get_metadata(book_id, index_is_id=True)
  if mi.has_cover and not mi.cover:
    mi.cover = 'cover.jpg'
  return opf2.metadata_to_opf(mi).replace('\r\n', '\n').rstrip('\r\n')


def delete_book_from_db(db, book_id):
  # The trigger books_delete_trg will remove from tables authors,
  # books_authors_link etc.
  db.conn.execute('DELETE FROM books WHERE id=?', (book_id,))
  db.data._data[book_id] = None
  try:
    db.data._map.remove(book_id)
  except ValueError:
    pass
  try:
    db.data._map_filtered.remove(book_id)
  except ValueError:
    pass


def delete_book(db, book_id, book_path, dbdir):
  delete_book_from_db(db, book_id)
  book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
  if os.path.exists(book_dir):
    recycle_bin.delete_tree(book_dir, permanent=True)
    try:
      os.rmdir(os.path.dirname(book_dir))  # Remove empty author dir.
    except OSError:
      pass


def add_book(db, opf_data, force_book_id=None, force_path=None, dbdir=None):
  """Adds a book to the database.

  Creates the directory if it doesn't exist, but doesn't create or modify
  any files.

  Args:
    force_book_id: A book ID to create the book as, or None to pick one
      automatically.
  Returns:
    new_book_id.
  """
  # Parsing .opf files is the slowest per-book step.
  mi = opf2.OPF(cStringIO.StringIO(opf_data)).to_book_metadata()
  cover_href = None
  for guide in mi._data['guide']:
    if guide.type == 'cover':
      cover_href = guide.href()
      break
  if cover_href and '/' in cover_href:
    raise AssertionError(repr(cover_href))
  # TODO(pts): rename cover_href to cover.jpg
  #print dir(mi._data['guide'])
  mi._data['guide']._resources[:] = [
     x for x in mi._data['guide']._resources if x.type != 'cover']
  mi.cover_data = (None, None)
  mi.has_cover = False
  mi.cover = None
  if force_book_id is not None:
    if force_book_id in db.data._map:
      raise ValueError('Book ID %d already exists.' % force_book_id)
    # TODO(pts): What if empty?
    last_book_id = db.conn.execute(
        'SELECT seq FROM sqlite_sequence WHERE name=?', ('books',)
        ).fetchone()[0]
    db.conn.execute('UPDATE sqlite_sequence SET seq=? WHERE name=?',
                    (force_book_id - 1, 'books'))
  # This also creates the directory (not the files).
  new_book_id = db.import_book(mi, [], preserve_uuid=True)
  if cover_href is not None:
    db.conn.execute('UPDATE books SET has_cover=? WHERE id=?',
                    (1, new_book_id))
    db.data._data[new_book_id][db.FIELD_MAP['cover']] = True
  if force_book_id is not None:
    if force_book_id != new_book_id:
      raise AssertionError((force_book_id, new_book_id))
    db.conn.execute('UPDATE sqlite_sequence SET seq=? WHERE name=?',
                    (max(last_book_id, new_book_id), 'books'))
  if force_path is not None:
    fm = db.FIELD_MAP
    fm_path = fm['path']
    book_path = encode_unicode_filesystem(db.data._data[new_book_id][fm_path])
    if book_path != force_path:
      if dbdir is None:
        raise ValueError
      set_book_path_and_rename(db, dbdir, new_book_id, force_path,
                               is_new_existing=True)
  return new_book_id


CALIBRE_CONTRIBUTOR_RE = re.compile(
    r'<dc:contributor opf:file-as="calibre" opf:role="bkp">[Cc]alibre\b[^<>]*'
    r'</dc:contributor>')
"""Matches the <dc:contributor tag for Calibre.

Example:

  ('<dc:contributor opf:file-as="calibre" opf:role="bkp">'
   'calibre (0.9.9) [http://calibre-ebook.com]</dc:contributor>')
"""


CALIBRE_IDENTIFIER_RE = re.compile(
    r'<dc:identifier opf:scheme="calibre" id="calibre_id">(\d+)</dc:identifier>'
    )
"""Matches the <dc:identifier (book ID) tag for Calibre.

Example:

  'dc:identifier opf:scheme="calibre" id="calibre_id">412</dc:identifier>'
"""


def replace_first_match(to_data, from_data, re_obj):
  """Returns the updated to_data."""
  to_match = re_obj.search(to_data)
  if to_match:
    from_match = re_obj.search(from_data)
    if from_match and from_match.group() != to_match.group():
      to_data = ''.join((to_data[:to_match.start()],
                         from_match.group(),
                         to_data[to_match.end():]))
  return to_data


TRAILING_BOOK_NUMBER_RE = re.compile(r' \((\d+)\)\Z')
"""Matches a book number in parens at the end of the string."""


def set_book_path_and_rename(db, dbdir, book_id, new_book_path, is_new_existing):
  fm = db.FIELD_MAP
  fm_path = fm['path']
  book_data = db.data._data[book_id]
  book_path = encode_unicode_filesystem(book_data[fm_path])
  if book_path == new_book_path:
    return
  book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
  if not os.path.isdir(book_dir):
    raise RuntimeError('Old book directory missing when renaming %r to %r.' %
                       (book_dir, new_book_dir))
  new_book_dir = os.path.join(dbdir, new_book_path.replace('/', os.sep))
  # os.path.exists(...)+os.rename(...) has a race condition, but we don't care.
  if is_new_existing:
    if not os.path.exists(new_book_dir):
      raise RuntimeError('New book directory missing renaming %r to %r.' %
                         (book_dir, new_book_dir))
  else:
    if os.path.exists(new_book_dir):
      raise RuntimeError('New book directory already exists when renaming '
                         '%r to %r.' %
                         (book_dir, new_book_dir))
  if not os.path.isdir(new_book_dir):
    os.makedirs(new_book_dir)
  if is_new_existing:
    os.rmdir(book_dir)
  else:
    os.rename(book_dir, new_book_dir)
  db.data._data[book_id][fm_path] = new_book_path.decode(
      calibre.filesystem_encoding)
  db.conn.execute('UPDATE books SET path=? WHERE id=?',
                  (new_book_path, book_id))
  book_path = book_path.replace('/', os.sep)
  book_path = os.path.dirname(book_path)
  while book_path:
    try:
      os.rmdir(os.path.join(dbdir, book_path))
    except OSError:
      break
    book_path = os.path.dirname(book_path)


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
  # This is O(n), it reads the whole book database to db.data.
  # TODO(pts): Calls dbdir.decode(filesystem_encoding), fix it.
  # TODO(pts): Why are there 4 commits in the beginning?
  db = database2.LibraryDatabase2(dbdir)  # Slow, reads metadata.db.

  # This is O(n), but fast, because generating .opf files is fast (as opposed
  # to parsing them).
  # TODO(pts): Ignore calibre version number changes etc.
  print >>sys.stderr, 'info: Finding changed books in: ' + dbname
  fm = db.FIELD_MAP
  fm_path = fm['path']
  ids_to_delete_from_db = set()
  books_to_update = {}
  path_to_ids = {}
  path_to_multiple_ids = {}
  file_ids_to_change = {}
  dirs_to_rename = {}
  # TODO(pts): Also write missing metadata.opf files.
  for book_id in db.data._map:
    book_data = db.data._data[book_id]
    # Should it be encode_unicode instead? Doesn't seem to matter, because
    # Calibre generates ASCII paths.
    book_path = encode_unicode_filesystem(book_data[fm_path])
    ids = path_to_ids.get(book_path)
    if ids is None:
      ids = path_to_ids[book_path] = []
    else:
      path_to_multiple_ids[book_path] = id
    ids.append(book_id)
    book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    #print (book_id, book_path, book_data[db.FIELD_MAP['title']])
    if os.path.isdir(book_dir):
      opf_name = os.path.join(book_dir, 'metadata.opf')
      if os.path.exists(opf_name):
        opf_data = open(opf_name).read().replace('\r\n', '\n').rstrip('\r\n')
        odb_data = get_db_opf(db, book_id)
        if opf_data != odb_data:
          # TODO(pts): Also ignore some other minor changes.
          # TODO(pts): If the UUID is different (e.g.
          #            <dc:identifier opf:scheme="uuid" id="uuid_id">),
          #            should we treat them as two different books?
          odb_data = replace_first_match(
              odb_data, opf_data, CALIBRE_CONTRIBUTOR_RE)
          if opf_data != odb_data:
            opf_data2 = replace_first_match(
                opf_data, odb_data, CALIBRE_IDENTIFIER_RE)
            if opf_data2 == odb_data:
              file_ids_to_change[book_id] = (book_path, opf_data2)
            else:
              books_to_update[book_path] = book_id
      match = TRAILING_BOOK_NUMBER_RE.search(book_path)
      if match:
        book_path2 = '%s (%d)' % (book_path[:match.start()], book_id)
      else:
        book_path2 = '%s (%d)' % (book_path, book_id)
      if book_path != book_path2:
        dirs_to_rename[book_id] = book_path2
    else:
      ids_to_delete_from_db.add(book_id)
  print >>sys.stderr, (
      'info: Found %d book row%s, %d book row%s to delete, %d book row%s to '
      'update, %d file ID%s to change and %d director%s to rename.' %
      (len(path_to_ids), 's' * (len(path_to_ids) != 1),
       len(ids_to_delete_from_db), 's' * (len(ids_to_delete_from_db) != 1),
       len(books_to_update), 's' * (len(books_to_update) != 1),
       len(file_ids_to_change), 's' * (len(file_ids_to_change) != 1),
       len(dirs_to_rename), ('y', 'ies')[len(dirs_to_rename) != 1]))
  if path_to_multiple_ids:
    raise RuntimeError('The same book path in the database is used for '
                       'multiple book IDs: %r' % path_to_multiple_ids)

  print >>sys.stderr, 'info: Finding new books.'
  book_dirs = frozenset(os.path.join(dbdir, book_path.replace('/', os.sep))
                        for book_path in path_to_ids)
  new_book_paths = set()
  dbprefix = dbdir + os.sep
  for dirpath, dirnames, filenames0 in os.walk(dbdir):
    if 'metadata.opf' in filenames0 and dirpath != dbdir:
      if dirpath in book_dirs:
        continue
      if not dirpath.startswith(dbprefix):
        raise AssertionError(dirpath, dbprefix)
      book_path = dirpath[len(dbprefix):].replace(os.sep, '/')
      # !! TODO(pts): Try to match it with an existing book by UUID.
      new_book_paths.add(book_path)
  del book_dirs
  print 'info: Found %d new book director%s.' % (
      len(new_book_paths), ('y', 'ies')[len(new_book_paths) != 1])

  # No database or filesystem changes up to this point.
  # TODO(pts): Verify this claim.
  print >>sys.stderr, 'info: Applying database and filesystem changes.'

  for book_id in sorted(file_ids_to_change):
    book_path, opf_data2 = file_ids_to_change[book_id]
    book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    # TODO(pts): Open all files in binary mode?
    with open(os.path.join(book_dir, 'metadata.opf'), 'w') as f:
      f.write(opf_data2)
      f.write('\n')

  # We must do this after having processed file_ids_to_change.
  for book_id in sorted(dirs_to_rename):
    new_book_path = dirs_to_rename[book_id]
    # TODO(pts): Do a `git mv'?
    set_book_path_and_rename(db, dbdir, book_id, new_book_path,
                             is_new_existing=False)

  if ids_to_delete_from_db:
    # Type error in Python sqlite3:
    # db.conn.execute('DELETE FROM books WHERE id IN (?)', (xids,))
    # The trigger books_delete_trg will remove from tables authors,
    # books_authors_link etc.
    # This works for very long (e.g. 1000000) elements ids_to_delete_from_db.
    # TODO(pts): Verify that this doesn't require several sequential scans.
    db.conn.execute('DELETE FROM books WHERE id IN (%s)' % ','.join(map(
        str, ids_to_delete_from_db)))

  if books_to_update:
    # Without changing db.set_path to no-op, db.set_matadata would call
    # db.set_path, which would move the rename the book and move a few
    # files (mentioned in `data') to a different directory, and it wouldn't
    # create metadata.opf.
    old_set_path = db.__dict__.get('set_path')  # Doesn't work with __slots__.
    try:
      db.set_path = lambda *args, **kwargs: None
      for book_path in sorted(books_to_update):
        book_id = books_to_update[book_path]
        book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
        opf_data = open(os.path.join(book_dir, 'metadata.opf')).read()
        # Parsing .opf files is the slowest per-book step.
        mi = opf2.OPF(cStringIO.StringIO(opf_data)).to_book_metadata()
        # TODO(pts): Make this and all other database updates faster by using
        # an in-memmory database here, and dumping it to the real database at the
        # end (db.conn.real_commit).
        db.set_metadata(book_id, mi, force_changes=True)
        # !! TODO(pts): Regenerate opf_data and compare. When updating the
        # author (<dc:creator), it seems to be different.
    finally:
      if old_set_path is None:
        db.__dict__.pop('set_path', None)
      else:
        db.__dict__['set_path'] = old_set_path

  # We are doing this after having processed ids_to_delte_from_db, so we have
  # some IDs still available.
  for book_path in sorted(new_book_paths):
    book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    opf_data = open(os.path.join(book_dir, 'metadata.opf')).read()
    id_match = CALIBRE_IDENTIFIER_RE.search(opf_data)
    if id_match is None:
      opf_book_id = None
    else:
      opf_book_id = int(id_match.group(1))
    if opf_book_id is not None and opf_book_id not in db.data._map:
      force_book_id = opf_book_id
    else:
      force_book_id = None  # If ID is already used, we'll pick a new one.
    book_id = add_book(db, opf_data, force_book_id, book_path, dbdir)
    match = TRAILING_BOOK_NUMBER_RE.search(book_path)
    if match:
      force_book_path = '%s (%d)' % (book_path[:match.start()], book_id)
    else:
      force_book_path = '%s (%d)' % (book_path, book_id)
    if force_book_path != book_path:
      set_book_path_and_rename(db, dbdir, book_id, force_book_path,
                               is_new_existing=False)
      book_path = force_book_path
      book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    if opf_book_id != book_id:
      opf_data2 = '%s%d%s' % (opf_data[:id_match.start(1)], book_id,
                              opf_data[id_match.end(1):])
      with open(os.path.join(book_dir, 'metadata.opf'), 'w') as f:
        f.write(opf_data2)
        f.write('\n')

  # TODO(pts): Import book files to data.

  # The alternative, db.dump_metadata() (also known as write_dirtied(db)) would
  # create the metadata.opf files for books listed in metadata_dirtied.
  # metadata_dirtied is populated by mutation methods of db.
  db.conn.execute('DELETE FROM metadata_dirtied')
  db.conn.real_commit()
  db.conn.close()
  # !! send_message()
  print >>sys.stderr, 'info: Done.'
  # !! TODO(pts): Add books in place, i.e. without copying files.
  # !! TODO(pts): Try to insert a book with its original id.
  # !! TODO(pts): Update the data table.
  # !! TODO(pts): Add missing books (calibredb add).
  # !! TODO(pts): Notify GUI.


if __name__ == '__main__':
  # SUXX: Original, byte argv not available.
  main(map(encode_unicode, sys.argv))
