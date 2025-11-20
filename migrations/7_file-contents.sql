CREATE TABLE IF NOT EXISTS file_contents (
  ino INTEGER,
  page INTEGER,
  bytes BLOB,
  PRIMARY KEY (ino, page),
  FOREIGN KEY (ino) REFERENCES file_attrs(ino)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);
