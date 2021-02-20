import sys

# Fix the description tag in a metadata.opf file.
# If the description contains newlines, it is messed up
# by older versions of Calibre.
desc = []
for line in sys.stdin:
  line=line.rstrip()
  if '<dc:description>' in line:
    assert not desc, desc
    if '</dc:description>' in line:
      print(line)
      desc = None
    else:
      desc.append(line)
  elif '</dc:description>' in line:
    assert desc, line
    desc.append(line)
    print('&lt;br&gt;'.join(desc))
    desc = None
  elif '<' in line:
    print(line)
  elif not line.startswith(' ' * 8):
    assert desc, line
    desc.append(line)

sys.exit(0)
