use std::time::{Duration, SystemTime};

use fuser::{FileAttr, FileType};
use sqlx::FromRow;

fn to_filetype(n: u8) -> Result<FileType, ()> {
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

fn to_systime(secs: u64) -> SystemTime {
    SystemTime::now() + Duration::from_secs(secs)
}

#[derive(FromRow)]
pub struct FileAttrRow {
    ino: u64,
    size: u64,
    blocks: u64,
    atime: u64,
    mtime: u64,
    ctime: u64,
    crtime: u64,
    kind: u8,
    perm: u16,
    nlink: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
    blksize: u32,
    flags: u32,
}

impl TryInto<FileAttr> for FileAttrRow {
    type Error = ();

    fn try_into(self) -> Result<FileAttr, Self::Error> {
        return Ok(FileAttr {
            ino: self.ino,
            size: self.size,
            blocks: self.blocks,
            atime: to_systime(self.atime),
            mtime: to_systime(self.mtime),
            ctime: to_systime(self.ctime),
            crtime: to_systime(self.crtime),
            kind: to_filetype(self.kind)?,
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: self.blksize,
            flags: self.flags,
        });
    }
}
