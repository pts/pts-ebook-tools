#! /bin/sh --
""":" # Updates metadata.db from the metadata.opf files.

exec calibre-debug -e "$0" ${1+"$@"}

Don't run this script while somebody else (e.g. Calibre) is modifying the
library. If you do anyway, you may lose some data. To be safe, exit from
Calibre while this script is running.
!! TODO(pts): Verify this claim, use EXCLUSIVE locking.

TODO(pts): Add documentation when to exit from Calibre. Is an exit needed?
TODO(pts): Disallow processing books with an older version of Calibre.
TODO(pts): Add rebuild_db.py to another repository, indicate that it's
  incorrect.
"""

# by pts@fazekas.hu at Wed Dec 26 12:54:02 CET 2012
__author__ = 'pts@fazekas.hu (Peter Szabo)'

import cStringIO
import errno
import multiprocessing.connection
import os
import os.path
import re
import socket
import sqlite3
import sys
import traceback

import calibre
from calibre.customize import builtins
from calibre.ebooks.metadata import opf2
from calibre.library import database2
from calibre.library import sqlite
from calibre.utils import ipc
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


BOOKS_INSERT_RE = re.compile(
    r'(?i)\s*INSERT\s+INTO\s+books\s*\(([^-)\'"`]*)\)\s*VALUES\s*\(')
"""Matches the beginning of INSERT INTO books."""


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
    old_execute = db.conn.execute
    try:
      def custom_execute(query, args=()):
        match = BOOKS_INSERT_RE.match(query)
        if match:
          query = 'INSERT INTO books (id, %s) VALUES (?, %s' % (
              match.group(1), query[match.end():])
          return old_execute(query, (force_book_id,) + tuple(args))
        else:
          return old_execute(query, args)
      db.conn.execute = custom_execute
      new_book_id = db.import_book(mi, [], preserve_uuid=True)
    finally:
      old_execute = None
      del db.conn.__dict__['execute']
  else:
    new_book_id = db.import_book(mi, [], preserve_uuid=True)
  # This also creates the directory (not the files).
  if cover_href is not None:
    db.conn.execute('UPDATE books SET has_cover=? WHERE id=?',
                    (1, new_book_id))
    db.data._data[new_book_id][db.FIELD_MAP['cover']] = True
  if force_book_id is not None and force_book_id != new_book_id:
    raise AssertionError((force_book_id, new_book_id, force_path))
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


def send_message(msg='', timeout=3):
  print >>sys.stderr, 'info: Notifying the Calibre GUI of the change.'
  try:
    t = multiprocessing.connection.Client(ipc.gui_socket_address())
  except socket.error, e:
    if e[0] != errno.ENOENT:  # TODO(pts): Make this work on Windows.
      raise
    # We ignore the exception if Calibre is not running.
    return
  try:
    t.send('refreshdb:' + msg)
  except Exception, e:
    print type(e)
  finally:
    t.close()


def get_max_book_id(db, used_book_ids, max_book_id=None):
  if max_book_id is None:
    for row in db.conn.execute(
        'SELECT seq FROM sqlite_sequence WHERE name=?', ('books',)):
      max_book_id = row[0]
      break
    else:
      max_book_id = 0
    if used_book_ids:
      max_book_id = max(max_book_id, max(used_book_ids))
  return max_book_id


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
  dbdir_sep = dbdir + os.sep
  fs_ext_dict = {}
  dotexts = frozenset('.' + extension for extension in EXTENSIONS)
  unknown_book_file_count = 0
  dbdir_git = os.path.join(dbdir, '.git')
  dbdir_git_sep = dbdir_git + os.sep
  dirpaths_to_ignore = (dbdir, dbdir_git)
  file_sizes = {}
  for dirpath, dirnames, filenames in os.walk(dbdir):
    if dirpath in dirpaths_to_ignore or dirpath.startswith(dbdir_git_sep):
      continue
    if not dirpath.startswith(dbdir_sep):
      raise AssertionError(dirpath, dbdir_sep)
    book_path = dirpath[len(dbdir_sep):].replace(os.sep, '/')
    is_mo = 'metadata.opf' in filenames
    if is_mo:
      if dirpath not in book_dirs:
        # TODO(pts): Try to match it with an existing book by UUID.
        new_book_paths.add(book_path)

    for filename in filenames:
      preext, ext = os.path.splitext(filename)
      if (ext in dotexts and filename not in ('cover.jpg', 'metadata.opf') and
          (is_mo or book_path in path_to_ids)):
        pathname = os.path.join(dirpath, filename)
        file_sizes[pathname] = os.stat(pathname).st_size
        fs_preexts = fs_ext_dict.get(dirpath)
        if fs_preexts is None:
          fs_preexts = fs_ext_dict[dirpath] = {}
        exts = fs_preexts.get(preext)
        if exts is None:
          exts = fs_preexts[preext] = []
        exts.append(ext)
      elif (filename not in ('cover.jpg', 'metadata.opf') and
            not filename.endswith('~')):
        print >>sys.stderr, 'error: unknown book file: %s' % (
            os.path.join(dirpath, filename))
        unknown_book_file_count += 1
  del book_dirs
  for dirpath in sorted(fs_ext_dict):
    fs_preexts = fs_ext_dict[dirpath]
    if len(fs_preexts) <= 1:
      continue
    correct_preexts = [preext for preext in fs_preexts if
        is_correct_book_filename(preext + '.mobi', dirpath)]
    if len(correct_preexts) == 1:
      fs_ext_dict[dirpath] = {
          correct_preexts[0]: fs_preexts[correct_preexts[0]]}
      del fs_preexts[correct_preexts[0]]
    else:
      fs_ext_dict[dirpath] = {}
    # Now fs_preexts contains the incorrect preexts.
    for preext in sorted(fs_preexts):
      for ext in fs_preexts[preext]:
        print >>sys.stderr, 'error: unknown book file: %s' % (
            os.path.join(dirpath, preext + ext))
        unknown_book_file_count += 1
  fs_filename_dict = {}
  for dirpath in sorted(fs_ext_dict):
    fs_preexts = fs_ext_dict[dirpath]
    book_path = dirpath[len(dbdir_sep):].replace(os.sep, '/')
    for preext in sorted(fs_preexts):
      for ext in fs_preexts[preext]:
        filename = preext + ext
        format = ext[1:].upper()
        uncompressed_size = file_sizes[os.path.join(dirpath, filename)]
        data_id = None
        key = (book_path, format)
        value = [preext, data_id, uncompressed_size]
        if key in fs_filename_dict:
          raise AssertionError(key)
        fs_filename_dict[key] = value
  del file_sizes
  print >>sys.stderr, (
      'info: Found %d new book director%s, %d unknown book file%s and '
      '%d book file%s.' % (
      len(new_book_paths), ('y', 'ies')[len(new_book_paths) != 1],
      unknown_book_file_count, 's' * (unknown_book_file_count != 1),
      len(fs_filename_dict), 's' * (fs_filename_dict != 1)))

  print >>sys.stderr, 'info: Reading book filename rows.'
  db_filename_dict = {}
  c = db.conn.execute('SELECT * FROM data')
  fields = tuple(x[0] for x in c.description)
  expected_fields = ('id', 'book', 'format', 'uncompressed_size', 'name')
  if fields != expected_fields:
    raise RuntimeError('Unexpected fie;d names in the data table: '
                       'got=%s expected=%s' %
                       (','.join(fields), ','.join(expected_fields)))
  for row in c:
    book_id = row[1]
    # TODO(pts): Don't call encode_unicode_filesystem this often.
    book_path = encode_unicode_filesystem(db.data._data[book_id][fm_path])
    key = (book_path, encode_unicode_filesystem(row[2]))  # (book_path, format)
    value = [encode_unicode_filesystem(row[4]), row[0], row[3]]   #  (name, data_id, uncompressed_size)
    if key in db_filename_dict:
      # TODO(pts): Collect all. Make it useful (e.g. print author and title).
      raise RuntimeError('Duplicate book file: book=%d format=%s' %
                         (row[1], row[2]))
    db_filename_dict[key] = value
  book_data_updates = []
  book_data_inserts = []
  book_data_deletes = []
  for key in sorted(fs_filename_dict):
    fs_value = fs_filename_dict[key]
    db_value = db_filename_dict.get(key)
    if db_value:
      fs_value[1] = db_value[1]  # Copy data_id.
      if fs_value != db_value:
        book_data_updates.append(key + tuple(fs_value))  # (book_path, format, name, data_id, uncompressed_size).
    else:
      book_data_inserts.append(key + (fs_value[0], fs_value[2]))  # (book_path, format, name, uncompressed_size).
  for key in sorted(db_filename_dict):
    if key not in fs_filename_dict:
      # TODO(pts): As a speed optimization, don't delete files if the whole book
      # is deleted, because the trigger deletes the files.
      book_data_deletes.append(key + (db_filename_dict[key][1],))  # (book_path, format, data_id).
  print >>sys.stderr, (
      'info: Found %s book filename row%s; '
      'will update %d, insert %d, delete %d.' %
      (len(db_filename_dict), 's' * (len(db_filename_dict) != 1),
       len(book_data_updates), len(book_data_inserts), len(book_data_deletes)))

  # No database or filesystem changes up to this point.
  if not (ids_to_delete_from_db or books_to_update or file_ids_to_change or
          dirs_to_rename or new_book_paths or book_data_updates or
          book_data_inserts or book_data_deletes):
    print >>sys.stderr, 'info: Nothing to modify.'
    return

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
  old_path_to_ids = dict(path_to_ids)
  for book_id in sorted(dirs_to_rename):
    new_book_path = dirs_to_rename[book_id]
    if new_book_path in path_to_ids:
      raise AssertionError(new_book_path)
    path_to_ids[new_book_path] = path_to_ids.pop(book_path)
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
        opf_data = opf_data.replace('\r\n', '\n').rstrip('\r\n')
        odb_data = get_db_opf(db, book_id)
        opf_data = replace_first_match(
            opf_data, odb_data, CALIBRE_CONTRIBUTOR_RE)
        # This can happen e.g. if the author (<dc:creator) is changed, then
        # Calibre replaces name="calibre:author_link_map".
        if opf_data != odb_data:
          with open(os.path.join(book_dir, 'metadata.opf'), 'w') as f:
            f.write(odb_data)
            f.write('\n')
    finally:
      if old_set_path is None:
        db.__dict__.pop('set_path', None)
      else:
        db.__dict__['set_path'] = old_set_path

  # We are adding new books after having processed ids_to_delte_from_db, so
  # we have some IDs still available.
  #
  # First we preallocate the book IDs to the new paths. We do it to avoid
  # bugs causing conflicts within existing directories in new_book_paths.
  new_book_paths_without_id = []
  new_book_path_to_id = {}
  used_book_ids = set(db.data._map)
  max_book_id = None
  # Assign free book IDs for books which request a specific one.
  for book_path in sorted(new_book_paths):
    book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    opf_data = open(os.path.join(book_dir, 'metadata.opf')).read()
    id_match = CALIBRE_IDENTIFIER_RE.search(opf_data)
    if id_match is None:
      book_id = None
    else:
      book_id = int(id_match.group(1))
      if book_id in used_book_ids:
        book_id = None
      else:
        new_book_path_to_id[book_path] = book_id
        used_book_ids.add(book_id)
    if book_id is None:
      new_book_paths_without_id.append(book_path)
  # Assign free book IDs to the remaining books.
  if new_book_paths_without_id:
    max_book_id = get_max_book_id(db, used_book_ids, max_book_id)
    for book_path in new_book_paths_without_id:
      max_book_id += 1
      new_book_path_to_id[book_path] = max_book_id
      used_book_ids.add(max_book_id)
  # Make sure that conflicting book directories don't exist.
  for book_path in sorted(new_book_paths):
    book_id = new_book_path_to_id[book_path]
    match = TRAILING_BOOK_NUMBER_RE.search(book_path)
    if match:
      force_book_path = '%s (%d)' % (book_path[:match.start()], book_id)
    else:
      force_book_path = '%s (%d)' % (book_path, book_id)
    if book_path != force_book_path:
      force_book_dir = os.path.join(dbdir, force_book_path.replace('/', os.sep))
      if os.path.exists(force_book_dir):
        # Pick a larger ID if force_book_path (i.e. the final book_path)
        # already exists, i.e. occupied by something else.
        used_book_ids.remove(book_id)
        max_book_id = get_max_book_id(db, used_book_ids, max_book_id)
        while True:
          max_book_id += 1
          if match:
            force_book_path = '%s (%d)' % (book_path[:match.start()], book_id)
          else:
            force_book_path = '%s (%d)' % (book_path, book_id)
          force_book_dir = os.path.join(
              dbdir, force_book_path.replace('/', os.sep))
          if not os.path.exists(force_book_dir):
            break
        new_book_path_to_id[book_path] = max_book_id
        used_book_ids.add(max_book_id)
  del max_book_id
  for book_path in sorted(new_book_paths):
    book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    opf_data = open(os.path.join(book_dir, 'metadata.opf')).read()
    force_book_id = new_book_path_to_id[book_path]
    book_id = add_book(db, opf_data, force_book_id, book_path, dbdir)
    old_path_to_ids[book_path] = ids = [book_id]
    match = TRAILING_BOOK_NUMBER_RE.search(book_path)
    if match:
      force_book_path = '%s (%d)' % (book_path[:match.start()], force_book_id)
    else:
      force_book_path = '%s (%d)' % (book_path, force_book_id)
    if force_book_path != book_path:
      set_book_path_and_rename(db, dbdir, book_id, force_book_path,
                               is_new_existing=False)
      book_path = force_book_path
      book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    path_to_ids[force_book_path] = ids
    odb_data = get_db_opf(db, book_id)
    opf_data = opf_data.replace('\r\n', '\n').rstrip('\r\n')
    opf_data2 = replace_first_match(  # get_db_opf returns correct book_id.
        opf_data, odb_data, CALIBRE_IDENTIFIER_RE)
    opf_data3 = replace_first_match(
        opf_data2, odb_data, CALIBRE_CONTRIBUTOR_RE)
    # This can happen e.g. whern <dc:language is changed from en to eng because
    # of a Calibre version upgrade.
    if opf_data != odb_data:
      if opf_data3 == odb_data:
        odb_data = opf_data2  # Keep the original Calibre contributor version.
      with open(os.path.join(book_dir, 'metadata.opf'), 'w') as f:
        f.write(odb_data)
        f.write('\n')

  # Modify the data table.
  for book_path, format, name, data_id, uncompressed_size in book_data_updates:
    book_id = old_path_to_ids[book_path][0]
    #print 'UPDATE', (book_id, data_id, book_path, format, name, uncompressed_size)
    #print list(db.conn.execute('SELECT * FROM data WHERE id=?', (data_id,)))
    db.conn.execute('UPDATE data SET uncompressed_size=?, name=? WHERE id=?',
                    (uncompressed_size, name, data_id))
    # TODO(pts): Shouldn't we convert format to unicode?
    #db.conn.execute('UPDATE data SET uncompressed_size=? AND name=? '
    #                'WHERE book=? AND format=?',
    #                (uncompressed_size, name, book_id, format))
  for book_path, format, name, uncompressed_size in book_data_inserts:
    book_id = old_path_to_ids[book_path][0]
    #print 'INSERT', (book_id, book_path, format, name, uncompressed_size)
    # TODO(pts): Shouldn't we convert format etc. to unicode?
    db.conn.execute('INSERT INTO data (uncompressed_size, name, book, format) '
                    'VALUES (?, ?, ?, ?)',
                    (uncompressed_size, name, book_id, format))
  if book_data_deletes:
    # TODO(pts): Test this.
    db.conn.execute('DELETE FROM data WHERE id IN (%s)' % ','.join(map(
        str, (data_id for _, _, data_id in book_data_deletes))))

  # The alternative, db.dump_metadata() (also known as write_dirtied(db)) would
  # create the metadata.opf files for books listed in metadata_dirtied.
  # metadata_dirtied is populated by mutation methods of db.
  db.conn.execute('DELETE FROM metadata_dirtied')
  db.conn.real_commit()
  db.conn.close()
  send_message()  # Notify the Calibre GUI of the change.
  # !! TODO(pts): Do a a full database rebuild and then compare correctness.
  # !! TODO(pts): Write the missing metadata.opf files.
  print >>sys.stderr, 'info: Done.'


if __name__ == '__main__':
  try:
    # SUXX: Original, byte argv not available.
    sys.exit(main(map(encode_unicode, sys.argv)))
  except SystemExit:
    pass
  except:
    try:
      traceback.print_exc()
    finally:
      sys.exit(1)
