CREATE TABLE IF NOT EXISTS file_attrs (
  ino INTEGER PRIMARY KEY,
  size INTEGER,
  blocks INTEGER,
  atime INTEGER,
  mtime INTEGER,
  ctime INTEGER,
  crtime INTEGER,
  kind INTEGER,
  perm INTEGER,
  nlink INTEGER,
  uid INTEGER,
  gid INTEGER,
  rdev INTEGER,
  blksize INTEGER,
  flags INTEGER
);
