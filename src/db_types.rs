use std::time::{Duration, SystemTime};

use fuser::{FileAttr, FileType};
use sqlx::FromRow;

#[macro_export]
macro_rules! ins_attrs {
    ($q: expr, $a: expr, $extra_args: expr) => {
        $q(format!(
            "INSERT INTO file_attrs VALUES (NULL, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) {}",
            $extra_args
        ).as_str())
        .bind($a.size as i64) // size INTEGER,
        .bind($a.blocks as i64) // blocks INTEGER,
        .bind(from_systime($a.atime) as i64) // atime INTEGER,
        .bind(from_systime($a.mtime) as i64) // mtime INTEGER,
        .bind(from_systime($a.ctime) as i64) // ctime INTEGER,
        .bind(from_systime($a.crtime) as i64) // crtime INTEGER,
        .bind(from_filetype($a.kind)) // kind INTEGER,
        .bind($a.perm) // perm INTEGER,
        .bind($a.nlink) // nlink INTEGER,
        .bind($a.uid) // uid INTEGER,
        .bind($a.gid) // gid INTEGER,
        .bind($a.rdev) // rdev INTEGER,
        .bind($a.blksize) // blksize INTEGER,
        .bind($a.flags) // flags INTEGER,
    };
    ($q: expr, $a: expr) => {
        ins_attrs!($q, $a, "")
    };
}

pub fn from_filetype(ft: FileType) -> u8 {
    match ft {
        FileType::NamedPipe => 0,
        FileType::CharDevice => 1,
        FileType::BlockDevice => 2,
        FileType::Directory => 3,
        FileType::RegularFile => 4,
        FileType::Symlink => 5,
        FileType::Socket => 6,
    }
}

pub fn to_filetype(n: u8) -> Result<FileType, ()> {
    Ok(match n.into() {
        0 => FileType::NamedPipe,
        1 => FileType::CharDevice,
        2 => FileType::BlockDevice,
        3 => FileType::Directory,
        4 => FileType::RegularFile,
        5 => FileType::Symlink,
        6 => FileType::Socket,
        _ => return Err(()),
    })
}

pub fn mode_to_filetype(mut mode: u32) -> Result<FileType, ()> {
    mode &= libc::S_IFMT;
    Ok(match mode {
        libc::S_IFSOCK => FileType::Socket,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFCHR => FileType::CharDevice,
        _ => return Err(()),
    })
}

pub fn from_systime(st: SystemTime) -> u64 {
    st.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()
}

pub fn to_systime(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

#[derive(FromRow, Debug)]
pub struct FileAttrRow {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub crtime: u64,
    pub kind: u8,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

impl Into<FileAttr> for FileAttrRow {
    fn into(self) -> FileAttr {
        FileAttr {
            ino: self.ino,
            size: self.size,
            blocks: self.blocks,
            atime: to_systime(self.atime),
            mtime: to_systime(self.mtime),
            ctime: to_systime(self.ctime),
            crtime: to_systime(self.crtime),
            kind: to_filetype(self.kind).unwrap(),
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: self.blksize,
            flags: self.flags,
        }
    }
}

#[derive(FromRow, Debug)]
pub struct ReadDirRow {
    #[sqlx(flatten)]
    pub attr: FileAttrRow,
    pub name: String,
}

#[derive(FromRow, Debug)]
pub struct AssociatedTagsRow {
    pub ino: u64,
    pub tid: u64,
}
