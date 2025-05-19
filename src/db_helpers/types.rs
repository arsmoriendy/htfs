use fuser::{FileAttr, FileType};
use libc::c_int;
use sqlx::{
    query::{Query, QueryAs, QueryScalar},
    Database, Encode, FromRow, Type,
};
use std::{
    num::TryFromIntError,
    time::{Duration, SystemTime, SystemTimeError},
};

pub trait Bindable<'q, DB: Database, Q> {
    /// Bind for a general `Bindable` type
    fn gbind<T>(self, value: T) -> Self
    where
        T: 'q + Encode<'q, DB> + Type<DB>;

    fn inner(self) -> Q;
}

impl<'q, DB> Bindable<'q, DB, Query<'q, DB, <DB as Database>::Arguments<'q>>>
    for Query<'q, DB, <DB as Database>::Arguments<'q>>
where
    DB: Database,
{
    fn gbind<T>(self, value: T) -> Self
    where
        T: 'q + Encode<'q, DB> + Type<DB>,
    {
        self.bind(value)
    }

    fn inner(self) -> Query<'q, DB, <DB as Database>::Arguments<'q>> {
        self
    }
}

impl<'q, DB, A> Bindable<'q, DB, QueryAs<'q, DB, A, <DB as Database>::Arguments<'q>>>
    for QueryAs<'q, DB, A, <DB as Database>::Arguments<'q>>
where
    DB: Database,
{
    fn gbind<T>(self, value: T) -> Self
    where
        T: 'q + Encode<'q, DB> + Type<DB>,
    {
        self.bind(value)
    }

    fn inner(self) -> QueryAs<'q, DB, A, <DB as Database>::Arguments<'q>> {
        self
    }
}

impl<'q, DB, A> Bindable<'q, DB, QueryScalar<'q, DB, A, <DB as Database>::Arguments<'q>>>
    for QueryScalar<'q, DB, A, <DB as Database>::Arguments<'q>>
where
    DB: Database,
{
    fn gbind<T>(self, value: T) -> Self
    where
        T: 'q + Encode<'q, DB> + Type<DB>,
    {
        self.bind(value)
    }

    fn inner(self) -> QueryScalar<'q, DB, A, <DB as Database>::Arguments<'q>> {
        self
    }
}

pub enum DBError {
    SQLX(sqlx::Error),
    Conv(ConvError),
}

/// libc error code map for database errors
pub const EDB: c_int = libc::EIO;

impl DBError {
    /// Maps to a tuple of libc error and a string
    pub fn map_db_err(&self) -> (c_int, String) {
        match &self {
            DBError::SQLX(e) => (
                match e {
                    sqlx::Error::RowNotFound => libc::ENOENT,
                    _ => EDB,
                },
                e.to_string(),
            ),
            DBError::Conv(e) => (
                match e {
                    _ => EDB,
                },
                // TODO: impl display for ConvError
                "Conversion error".to_string(),
            ),
        }
    }
}

impl From<ConvError> for DBError {
    fn from(value: ConvError) -> Self {
        DBError::Conv(value)
    }
}

impl From<sqlx::Error> for DBError {
    fn from(value: sqlx::Error) -> Self {
        DBError::SQLX(value)
    }
}

impl From<SystemTimeError> for DBError {
    fn from(value: SystemTimeError) -> Self {
        DBError::Conv(ConvError::SystemTimeToU64(value))
    }
}

impl From<TryFromIntError> for DBError {
    fn from(value: TryFromIntError) -> Self {
        DBError::Conv(ConvError::ToI64(value))
    }
}

pub enum ConvError {
    U8ToFiletype,
    ModeToFiletype,
    SystemTimeToU64(SystemTimeError),
    ToI64(TryFromIntError),
}

impl From<SystemTimeError> for ConvError {
    fn from(value: SystemTimeError) -> Self {
        ConvError::SystemTimeToU64(value)
    }
}

impl From<TryFromIntError> for ConvError {
    fn from(value: TryFromIntError) -> Self {
        ConvError::ToI64(value)
    }
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

pub fn to_filetype(n: u8) -> Result<FileType, ConvError> {
    Ok(match n.into() {
        0 => FileType::NamedPipe,
        1 => FileType::CharDevice,
        2 => FileType::BlockDevice,
        3 => FileType::Directory,
        4 => FileType::RegularFile,
        5 => FileType::Symlink,
        6 => FileType::Socket,
        _ => return Err(ConvError::U8ToFiletype),
    })
}

pub fn mode_to_filetype(mut mode: u32) -> Result<FileType, ConvError> {
    mode &= libc::S_IFMT;
    Ok(match mode {
        libc::S_IFSOCK => FileType::Socket,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFCHR => FileType::CharDevice,
        _ => return Err(ConvError::ModeToFiletype),
    })
}

pub fn from_systime(st: SystemTime) -> Result<u64, ConvError> {
    Ok(st.duration_since(SystemTime::UNIX_EPOCH)?.as_secs())
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

impl TryFrom<&FileAttrRow> for FileAttr {
    type Error = ConvError;

    fn try_from(value: &FileAttrRow) -> Result<Self, Self::Error> {
        Ok(FileAttr {
            ino: value.ino,
            size: value.size,
            blocks: value.blocks,
            atime: to_systime(value.atime),
            mtime: to_systime(value.mtime),
            ctime: to_systime(value.ctime),
            crtime: to_systime(value.crtime),
            kind: to_filetype(value.kind)?,
            perm: value.perm,
            nlink: value.nlink,
            uid: value.uid,
            gid: value.gid,
            rdev: value.rdev,
            blksize: value.blksize,
            flags: value.flags,
        })
    }
}

#[derive(FromRow, Debug)]
pub struct ReadDirRow {
    #[sqlx(flatten)]
    pub attr: FileAttrRow,
    pub name: String,
}
