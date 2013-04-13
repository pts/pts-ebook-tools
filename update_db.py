#! /bin/sh --
""":" # Updates metadata.db from the metadata.opf files.

exec calibre-debug -e "$0" ${1+"$@"}

Run this script after a `git pull' or `git fetch' to resolve conflicts in
metadata.db by updating it from individual metadata.opf files.

Don't run this script while somebody else (e.g. Calibre) is modifying the
library. If you do anyway, you may lose some data. To be safe, exit from
Calibre while this script is running.

This script modifies the filesystem as well in addition to the database,
e.g. it renames book directories so that they include the book ID, changes
the book ID in metadata.opf if there is a conflict etc. It tries to make as
few changes as necessary.

This script runs `git add' on all files (metadata.db, metadata.opf,
cover.jpg and book data files) in the Calibre library if it finds that the
library is in a Git repository.

TODO(pts): Detect renames (Ubik and Valis), add `deleted'.
  `git commit -a -m' detects the rename.
TODO(pts): `git rm' the deletion of cover.jpg (happened in Calibre).
TODO(pts): How does Calibre handle unknown extensions (e.g. .doc)? Our script
  just ignores those files.
TODO(pts): Verify that Calibre is not running in this directory, possibly stop
  it or lock it?
TODO(pts): Add command-line flags to rename directories (e.g. _ -> ,).
TODO(pts): Verify the locking claims, use EXCLUSIVE locking.
TODO(pts): Do a a full database rebuild and then compare correctness.
TODO(pts): This script is faster than expected at a full rebuild (after
  DELETE FROM books; DELETE FROM authors; VACUUM) -- why? Why is the
  metadata.db >= 128K even then?
TODO(pts): Add documentation when to exit from Calibre. Is an exit needed?
TODO(pts): Disallow processing books with an older version of Calibre.
TODO(pts): Add rebuild_db.py to another repository, indicate that it's
  incorrect.
TODO(pts): What if metadata.opf contains a nonexisting cover.jpg? We already
  have fix_cover_jpg(...), but we don't call it often, e.g. we don't call it
  when both the db and the metadata.opf has the incorrect <guide> tag.
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
import subprocess
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
  return ('Updates (parts of) metadata.db from the metadata.opf files.\n'
          'Usage: %s [--half-dry-run] [<calibre-library-dir>]\n'
          '--half-dry-run only saves dirty db books to metadata.opf' % argv0)


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


def add_book(db, opf_data, is_git,
             force_book_id=None, force_path=None, dbdir=None):
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
  if getattr(mi._data, 'guide', None):  # Not in Calibre 0.9.13 anymore.
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
  else:
    if mi.cover_data[0]:  # TODO(pts): Is it good enough?
      cover_href = '.'
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
                               is_new_existing=True, is_git=is_git)
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


def set_book_path_and_rename(db, dbdir, book_id, new_book_path,
                             is_new_existing, is_git):
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
    if is_git:
      os.rmdir(new_book_dir)
      # `git mv' needs at least one file added.
      git_add(os.path.join(book_dir, 'metadata.opf'))
      git_mv(book_dir, new_book_dir)
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


def call_xargs(xargs, cmd, **kwargs):
  """Call subprocess cmd with many command-line arguments (xargs)."""
  if not isinstance(xargs, (tuple, list)):
    raise TypeError
  if not isinstance(cmd, (tuple, list)):
    raise TypeError
  xargs = list(xargs)
  cmd = list(cmd)
  if not getattr(errno, 'E2BIG', None):  # 'Argument list too long'.
    return subprocess.call(cmd + xargs, **kwargs)
  try:
    return subprocess.call(cmd + xargs, **kwargs)
  except OSError, e:
    if e[0] != errno.E2BIG:
      raise
  i = 0
  size = len(xargs) >> 1
  while 1:
    if not size:
      raise RuntimeError('Even a single-arg command is too long.')
    try:
      status = subprocess.call(cmd + xargs[i : i + size], **kwargs)
      if status:
        return status
      i += size
      size = len(xargs) - i
      if not size:  # Everything processed.
        return status
    except OSError, e:
      if e[0] != errno.E2BIG:
        raise
      size >>= 1  # Try passing half as many arguments.


def is_dir_in_git(dir_name):
  """This function assumes that os.environ['GIT_DIR'] is not set."""
  p = subprocess.Popen(('git', 'rev-parse', '--show-toplevel'),
                       preexec_fn=lambda: os.chdir(dir_name),
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  try:
    stdout, stderr = p.communicate()
  finally:
    status = p.wait()
  return not status


def git_add(filename):
  status = subprocess.call(
      ('git', 'add', '--', os.path.basename(filename)),
      preexec_fn=lambda: os.chdir(os.path.dirname(filename)))
  if status:
    raise RuntimeError('git-add failed with status=0x%x' % status)


def git_checkout(filename, preargs=()):
  basename = os.path.basename(filename)
  if preargs:
    cmd = ('git', 'checkout') + tuple(preargs) + ('--', basename)
  else:
    cmd = ('git', 'checkout', '--', basename)
  status = subprocess.call(
      cmd, preexec_fn=lambda: os.chdir(os.path.dirname(filename)))
  if status:
    raise RuntimeError('git-checkout failed with status=0x%x' % status)


def git_status_s(filename):
  p = subprocess.Popen(
      ('git', 'status', '-s', '--', os.path.basename(filename)),
      stdout=subprocess.PIPE,
      preexec_fn=lambda: os.chdir(os.path.dirname(filename)))
  try:
    stdout, stderr = p.communicate()
  finally:
    status = p.wait()
  if status:
    raise RuntimeError('git-status failed with status=0x%x' % status)
  if stdout:
    if len(stdout) < 3 or stdout[2] != ' ':
      raise RuntimeError('Unexpectd git-status output: %r' % stdout)
    return stdout[:2]
  else:
    return ''  # Unchanged.


def is_git_status_unmerged_conflict(status_s):
  """Takes the return value of git_status_s."""
  # See `git help status' for all possible `unmerged' combinations.
  return ('U' in status_s and status_s != 'DU') or status_s == 'AA'


def git_mv(oldname, newname):
  olds = oldname.split(os.sep)
  news = newname.split(os.sep)
  i = 0
  limit = min(len(olds), len(news))
  while i < limit and olds[i] == news[i]:
    i += 1
  if i:
    status = subprocess.call(
        ('git', 'mv', '--', os.sep.join(olds[i:]), os.sep.join(news[i:])),
        preexec_fn=lambda: os.chdir(os.sep.join(olds[:i])))
  else:
    status = subprocess.call(('git', 'mv', '--', oldname, newname))
  if status:
    raise RuntimeError('git-mv failed with status=0x%x' % status)


def setup_git():
  os.environ.pop('GIT_DIR', None)  # Let git find .git on the filesystem.
  calibre_path = calibre.__path__[0]
  if not isinstance(calibre_path, str):
    raise AssertionError(repr(calibre.__path__))
  calibre_path = calibre_path.replace(os.sep, '/')
  j = calibre_path.rfind('/lib/python')
  if j < 0:
    raise AssertionError
  calibre_path = calibre_path[:j + 4].replace('/', os.sep)
  ld_library_path = os.environ.get('LD_LIBRARY_PATH', '')
  if ld_library_path:
    # Remove the /lib, because git may be linked to a different zlib etc.
    ld_library_path = os.pathsep.join(
        dir_name for dir_name in ld_library_path.split(os.pathsep)
        if dir_name != calibre_path)
    os.environ['LD_LIBRARY_PATH'] = ld_library_path


def open_db(dbdir):
  sqlite.Connection.create_dynamic_filter = _connection__create_dynamic_filter
  sqlite.Connection.commit = _connection__commit
  sqlite.Connection.real_commit = _connection__real_commit
  database2.connect = sqlite.connect = noproxy_connect
  # This is O(n), it reads the whole book database to db.data.
  # TODO(pts): Calls dbdir.decode(filesystem_encoding), fix it.
  # TODO(pts): Why are there 4 commits in the beginning?
  db = database2.LibraryDatabase2(dbdir)  # Slow, reads metadata.db.
  db.dirtied = lambda *args, **kwargs: None  # We save .opf files manually.
  return db


OPF_DC_SUBJECT_RE = re.compile(r'(?sm)^ *<dc:subject>.*</dc:subject>(?=\n)')
"""Matches the block of <dc:subject>... lines (multiline) in an .opf."""


def sort_dc_subject(opf_data):
  """Sorts the <dc:subject>... lines on an .opf file content."""
  match = OPF_DC_SUBJECT_RE.search(opf_data)
  if not match:
    return opf_data
  return '%s%s%s' % (
      opf_data[:match.start()],
      '\n'.join(sorted(match.group().split('\n'))),  # Sorted <dc:subject>s.
      opf_data[match.end():])


OPF_GUIDE_COVER_JPG_RE = re.compile(r'(?s)<guide>\s*<reference href="cover[.]jpg"[^>]*>\s*</guide>')
"""Matches the <guide> tag with only cover[.].jpg."""

OPF_EMPTY_GUIDE_RE = re.compile(r'<guide(?:/>|>\s*</guide>)')
"""Matches an empty <guide> tag. <guide/> is the most common."""

OPF_COVER_JPG_GUIDE_STR = ('<guide>\n        <reference href="cover.jpg" '
                           'type="cover" title="Cover"/>\n    </guide>')


def fix_cover_jpg(opf_data, book_dir):
  """Fixes the <guide> tag based on the existence of cover.jpg."""
  has_cover_jpg = os.path.isfile(os.path.join(book_dir, 'cover.jpg'))
  match = OPF_GUIDE_COVER_JPG_RE.search(opf_data)
  if match:
    if not has_cover_jpg:
      return '%s<guide/>%s' % (opf_data[:match.start()], opf_data[match.end():])
  else:  # TODO(pts): Test this.
    if has_cover_jpg:
      match = OPF_EMPTY_GUIDE_RE.search(opf_data)
      # TODO(pts): What if our OPF_COVER_JPG_GUIDE_STR gets out-of-date?
      if match:
        return '%s%s%s' % (
            opf_data[:match.start()], OPF_COVER_JPG_GUIDE_STR,
            opf_data[match.end():])
      else:
        if not opf_data.endswith('\n</package>'):
          raise AssertionError('opf data does not end with package: %r' %
                                opf_data[:-16])
        i = opf_data.rfind('\n') + 1
        return '%s%s%s' % (
            opf_data[:i], OPF_COVER_JPG_GUIDE_STR, opf_data[i:])
  return opf_data


def figure_out_what_to_change(db, dbdir, is_git):
  """Figures out what to change, but keeps the files and the database intact."""

  # This is O(n), but fast, because generating .opf files is fast (as opposed
  # to parsing them).
  print >>sys.stderr, 'info: Finding changed books in: ' + os.path.join(
      dbdir, 'metadata.db')
  fm = db.FIELD_MAP
  fm_path = fm['path']
  ids_to_delete_from_db = set()
  books_to_update = {}
  path_to_ids = {}
  path_to_multiple_ids = {}
  file_ids_to_change = {}
  dirs_to_rename = {}
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
        opf_data = sort_dc_subject(
            open(opf_name).read().replace('\r\n', '\n').rstrip('\r\n'))
        odb_data = sort_dc_subject(get_db_opf(db, book_id))
        if opf_data != odb_data:
          # TODO(pts): Also ignore some other minor changes.
          # TODO(pts): If the UUID is different (e.g.
          #            <dc:identifier opf:scheme="uuid" id="uuid_id">),
          #            should we treat them as two different books?
          # We call sort_dc_subject so that a difference in the tag order won't
          # make the books different.
          odb_data = replace_first_match(
              odb_data, opf_data, CALIBRE_CONTRIBUTOR_RE)
          if opf_data != odb_data:
            opf_data2 = replace_first_match(
                opf_data, odb_data, CALIBRE_IDENTIFIER_RE)
            if opf_data2 != odb_data:
              opf_data2 = fix_cover_jpg(opf_data2, book_dir)
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
  unknown_book_files = set()
  dbdir_git = os.path.join(dbdir, '.git')
  dbdir_git_sep = dbdir_git + os.sep
  dirpaths_to_ignore = (dbdir, dbdir_git)
  file_sizes = {}
  book_paths_without_opf = []
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
    elif dirpath in book_dirs:
      book_paths_without_opf.append(book_path)
    for filename in filenames:
      preext, ext = os.path.splitext(filename)
      dirpathfile = os.path.join(dirpath, filename)
      if (ext in dotexts and filename not in ('cover.jpg', 'metadata.opf') and
          (is_mo or book_path in path_to_ids)):
        file_sizes[dirpathfile] = os.stat(dirpathfile).st_size
        fs_preexts = fs_ext_dict.get(dirpath)
        if fs_preexts is None:
          fs_preexts = fs_ext_dict[dirpath] = {}
        exts = fs_preexts.get(preext)
        if exts is None:
          exts = fs_preexts[preext] = []
        exts.append(ext)
      elif (filename not in ('cover.jpg', 'metadata.opf') and
            not filename.endswith('~')):
        print >>sys.stderr, 'error: unknown book file: %s' % dirpathfile
        unknown_book_files.add(dirpathfile)
  del book_dirs, dirpaths_to_ignore
  book_paths_without_opf.sort()
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
        dirpathfile = os.path.join(dirpath, preext + ext)
        print >>sys.stderr, 'error: unknown book file: %s' % dirpathfile
        unknown_book_files.add(dirpathfile)
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
      'info: Found %d new book director%s, %d old book director%s with '
      'metadata.opf missing, %d unknown book file%s and '
      '%d book file%s.' % (
      len(new_book_paths), ('y', 'ies')[len(new_book_paths) != 1],
      len(book_paths_without_opf),
      ('y', 'ies')[len(book_paths_without_opf) != 1],
      len(unknown_book_files), 's' * (len(unknown_book_files) != 1),
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
  del fields, expected_fields
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
  del c
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
  del db_filename_dict

  # TODO(pts): Return a helper object instead. Introduce a class.
  return (book_data_deletes, book_data_inserts, book_data_updates,
          book_paths_without_opf, books_to_update, dirs_to_rename,
          file_ids_to_change, ids_to_delete_from_db, new_book_paths,
          path_to_ids, unknown_book_files)


def apply_db_and_fs_changes(
    dbdir, is_git, db, book_data_deletes, book_data_inserts, book_data_updates,
    book_paths_without_opf, books_to_update, dirs_to_rename, file_ids_to_change,
    ids_to_delete_from_db, new_book_paths, path_to_ids):
  dirtied_count = list(db.conn.execute(
      'SELECT COUNT(*) FROM metadata_dirtied'))[0][0]
  if dirtied_count:
    raise RuntimeError('%d book%s initially dirty.' % (
        dirtied_count, 's' * (dirtied_count != 1)))

  # Create missing metadata.opf files.
  for book_path in book_paths_without_opf:  # Already sorted.
    book_id = path_to_ids[book_path][0]
    book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    opf_data = get_db_opf(db, book_id)
    opf_filename = os.path.join(book_dir, 'metadata.opf')
    with open(opf_filename, 'w') as f:
      f.write(opf_data)
      f.write('\n')
    if is_git:
      git_add(opf_filename)

  for book_id in sorted(file_ids_to_change):
    book_path, opf_data2 = file_ids_to_change[book_id]
    book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    # TODO(pts): Open all files in binary mode?
    opf_filename = os.path.join(book_dir, 'metadata.opf')
    with open(opf_filename, 'w') as f:
      f.write(opf_data2)
      f.write('\n')
    if is_git:
      git_add(opf_filename)

  # We must do this after having processed file_ids_to_change.
  old_path_to_ids = dict(path_to_ids)
  for book_id in sorted(dirs_to_rename):
    new_book_path = dirs_to_rename[book_id]
    if new_book_path in path_to_ids:
      raise AssertionError(new_book_path)
    path_to_ids[new_book_path] = path_to_ids.pop(book_path)
    # TODO(pts): Do a `git mv'?
    set_book_path_and_rename(db, dbdir, book_id, new_book_path,
                             is_new_existing=False, is_git=is_git)

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
        opf_filename = os.path.join(book_dir, 'metadata.opf')
        # This can happen e.g. if the author (<dc:creator) is changed, then
        # Calibre replaces name="calibre:author_link_map".
        if opf_data != odb_data:
          with open(opf_filename, 'w') as f:
            f.write(odb_data)
            f.write('\n')
        if is_git:
          git_add(opf_filename)
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
    book_id = add_book(db, opf_data, is_git, force_book_id, book_path, dbdir)
    old_path_to_ids[book_path] = ids = [book_id]
    match = TRAILING_BOOK_NUMBER_RE.search(book_path)
    if match:
      force_book_path = '%s (%d)' % (book_path[:match.start()], force_book_id)
    else:
      force_book_path = '%s (%d)' % (book_path, force_book_id)
    if force_book_path != book_path:
      set_book_path_and_rename(db, dbdir, book_id, force_book_path,
                               is_new_existing=False, is_git=is_git)
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
    if is_git:
      # Other files will be added by add_files_to_git.
      git_add(os.path.join(book_dir, 'metadata.opf'))

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
  # TODO(pts): Modify the corresponding in-memory fields in db.data._data.

  dirtied_count = list(db.conn.execute(
      'SELECT COUNT(*) FROM metadata_dirtied'))[0][0]
  if dirtied_count:
    raise RuntimeError('%d book%s still dirty.' % (
        dirtied_count, 's' * (dirtied_count != 1)))
  db.conn.real_commit()


def add_files_to_git(dbdir, unknown_book_files):
  print >>sys.stderr, 'info: Adding all files to Git.'
  git_add(os.path.join(dbdir, 'metadata.db'))
  dbdir_sep = dbdir + os.sep
  dotexts = frozenset('.' + extension for extension in EXTENSIONS)
  dbdir_git = os.path.join(dbdir, '.git')
  dbdir_git_sep = dbdir_git + os.sep
  dirpaths_to_ignore = (dbdir, dbdir_git)
  files_to_add = ['metadata.db']
  total_added_file_size = os.stat(os.path.join(dbdir, 'metadata.db')).st_size
  for dirpath, dirnames, filenames in os.walk(dbdir):
    if dirpath in dirpaths_to_ignore or dirpath.startswith(dbdir_git_sep):
      continue
    if not dirpath.startswith(dbdir_sep):
      raise AssertionError(dirpath, dbdir_sep)
    book_path = dirpath[len(dbdir_sep):].replace(os.sep, '/')
    if 'metadata.opf' in filenames:
      for filename in filenames:
        preext, ext = os.path.splitext(filename)
        dirpathfile = os.path.join(dirpath, filename)
        if ((ext in dotexts and dirpathfile not in unknown_book_files) or
            filename in ('cover.jpg', 'metadata.opf')):
          files_to_add.append(dirpathfile[len(dbdir_sep):])
          total_added_file_size += os.stat(dirpathfile).st_size
  files_to_add.sort()
  # Some file may be up to data, but we count them anyway for simplicity.
  # `git add' is very fast on them (based on timestamp, and then SHA-1).
  print >>sys.stderr, 'info: Found %d file%s (%d byte%s) to (re)add to Git.' % (
       len(files_to_add), 's' * (len(files_to_add) != 1),
       total_added_file_size, 's' * (total_added_file_size != 1))
  if files_to_add:  # Always true because of metadata.db.
    status = call_xargs(
        files_to_add, ('git', 'add', '--'), preexec_fn=lambda: os.chdir(dbdir))
    if status:
      raise RuntimeError('git-add failed with status=0x%x' % status)


def safe_write_dirtied(db, dbdir, is_git):
  """Write metadata.opf files (based on the database) for dirty books."""
  # This method is like db.dump_metadata() or write_dirtied(...) in the
  # calibredb tool, but will try to be smarter on conflicts.
  dirty_book_ids = sorted(book_id for book_id, in
                          db.conn.execute('SELECT book FROM metadata_dirtied'))
  if not dirty_book_ids:
    return
  print >>sys.stderr, (
      'info: Found %d dirty book%s, generating their metadata.' %
      (len(dirty_book_ids), 's' * (len(dirty_book_ids) != 1)))
  files_to_write = []
  files_modified_in_git = []
  fm = db.FIELD_MAP
  fm_path = fm['path']
  for book_id in dirty_book_ids:
    assert book_id < len(db.data._data), (book_id, len(db.data._data))
    if db.data._data[book_id] is None:
      continue
    book_path = encode_unicode_filesystem(db.data._data[book_id][fm_path])
    book_dir = os.path.join(dbdir, book_path.replace('/', os.sep))
    opf_data = get_db_opf(db, book_id)
    opf_filename = os.path.join(book_dir, 'metadata.opf')
    # TODO(pts): Calling git_status_s so many times is very slow. Use
    # call_xargs to speed it up.
    if is_git and git_status_s(opf_filename):
      files_modified_in_git.append(opf_filename)
    else:
      files_to_write.append((opf_filename, opf_data))
  if files_modified_in_git:
    # TODO(pts): Do something smarter here in the most common case (git
    # merge).
    # TODO(pts): Do something smarter here after a long calibre run which has
    #   made many modifications. At least add an option to skip this error.
    #   This is an important show-stopper bug.
    msg = 'Found %d dirty book%s also modified in Git' % (
        len(files_modified_in_git), 's' * (len(files_modified_in_git) != 1))
    print >>sys.stderr, 'info: %s: %r' % (msg, sorted(files_modified_in_git))
    raise RuntimeError(msg + ', see above.')
  if files_to_write:
    print >>sys.stderr, (
        'info: Found %d dirty book%s, writing their metadata.opf.' %
        (len(files_to_write), 's' * (len(files_to_write) != 1)))
    for opf_filename, opf_data in files_to_write:
      with open(opf_filename, 'w') as f:
        f.write(opf_data)
        f.write('\n')
      if is_git:
        git_add(opf_filename)
    db.conn.execute('DELETE FROM metadata_dirtied')
    # Commit because we don't want to roll back the DELETE above later,
    # because that would make the filesystem and the database more
    # inconsistent, and it would make conflicts harder to resolve.
    #
    # TODO(pts): Do we still keep the database locked after the commit?
    db.conn.real_commit()


def main(argv):
  """Main entry point for the script.

  Don't call this function in a multithreaded application (such as Calibre),
  it's not thread safe, because it makes non-thread-safe changes to some global
  variables.
  """
  if (len(argv) > 1 and argv[1] in ('--help', '-h')):
    print usage(argv[0])
    sys.exit(0)
  i = 1
  do_half_dry_run = False
  while i < len(argv):
    arg = argv[i]
    if arg == '--':
      i += 1
      break
    if arg == '-' or not arg.startswith('-'):
      break
    if arg == '--half-dry-run':
      do_half_dry_run = True
    else:
      print >>sys.stderr, '%s\n\nerror: Unknown flag: %s' % (
          usage(argv[0]), arg)
      sys.exit(1)
    i += 1
  if i == len(argv):
    dbdir = '.'
  elif i == len(argv) - 1:
    dbdir = argv[i]
  else:
    print >>sys.stderr, '%s\n\nerror: Too many command-line arguments.' % (
        usage(argv[0]))
    sys.exit(1)
  if os.path.isfile(dbdir):
    dbname = dbdir
    dbdir = os.path.dirname(dbdir)
    if os.path.basename(dbname) != 'metadata.db':
      # We require this because of the database2.LibraryDatabase2(dbdir)
      # constructor.
      raise RuntimeError('The basename of the Calibre database must be '
                         'metadata.db, got %r' % dbname)
  else:
    dbname = os.path.join(dbdir, 'metadata.db')
  print >>sys.stderr, 'info: Updating Calibre database: ' + dbname
  if not os.path.isfile(dbname):
    raise RuntimeError('Calibre database missing: ' + dbname)

  setup_git()
  # TODO(pts): Fail if there is .git, but the 'git' command doesn't work.
  is_git = is_dir_in_git(dbdir)
  if is_git:
    print >>sys.stderr, (
        'info: Found Git repository, scanning working tree for file changes.')
    status_s = git_status_s(dbname)
    if is_git_status_unmerged_conflict(status_s):
      print >>sys.stderr, (
          'info: Resolving conflict in metadata.db by ignoring their changes.')
      git_checkout(dbname, ('HEAD',))

  db = open_db(dbdir)

  # We have to do it in case lots of metadata changes were done in Calibre, but
  # Calibre was closed before it could write all metadata.opf files to disk.
  # (It's writing them very slowly, because that's throttled.) The
  # metadata_dirtied table contains the book IDs to write.
  safe_write_dirtied(db, dbdir, is_git)

  (book_data_deletes, book_data_inserts, book_data_updates,
   book_paths_without_opf, books_to_update, dirs_to_rename,
   file_ids_to_change, ids_to_delete_from_db, new_book_paths,
   path_to_ids, unknown_book_files) = figure_out_what_to_change(
       db, dbdir, is_git)

  # No database or filesystem changes up to this point.
  if (ids_to_delete_from_db or books_to_update or file_ids_to_change or
      dirs_to_rename or new_book_paths or book_data_updates or
      book_data_inserts or book_data_deletes or book_paths_without_opf):
    if do_half_dry_run:
      print >>sys.stderr, 'info: Dry run, not applying changes.'
    else:
      print >>sys.stderr, 'info: Applying database and filesystem changes.'
      apply_db_and_fs_changes(
          dbdir, is_git, db, book_data_deletes, book_data_inserts,
          book_data_updates, book_paths_without_opf, books_to_update,
          dirs_to_rename, file_ids_to_change, ids_to_delete_from_db,
          new_book_paths, path_to_ids)
      db.conn.close()
      if is_git:
        add_files_to_git(dbdir, unknown_book_files)
      send_message()  # Notify the Calibre GUI of the change.
  else:
    print >>sys.stderr, 'info: Calibre database is up-to-date, nothing to do.'
    db.conn.close()
    if is_git:
      add_files_to_git(dbdir, unknown_book_files)
  # TODO(pts): Occasionally we get MM (not added to the index) changes on
  #   metadata.db -- why? Probably when a book is removed in Calibre.
  if is_git:
    print >>sys.stderr, (
        'info: Done, do not forget to run: git commit -a -m update')
  else:
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
