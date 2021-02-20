#!/bin/bash

# Filter all metadata.opf files through fixopf.py.
find "." -type f -name 'metadata.opf' -print0 |
while IFS= read -r -d '' file; do
  echo "$file"
  python3 fixopf.py <"$file" >"$file.new"
  mv "$file.new" "$file"
done
