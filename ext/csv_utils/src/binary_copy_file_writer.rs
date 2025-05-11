use std::io::{self, Write, BufWriter};
use byteorder::{BigEndian, WriteBytesExt};
use postgres::types::{Type, ToSql, IsNull};
use bytes::BytesMut;

const HEADER_MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// A writer for PostgreSQL binary-copy streams
pub struct BinaryCopyFileWriter<W: Write> {
    types: Vec<Type>,
    buf: BytesMut,
    writer: BufWriter<W>,
}

impl<W: Write> BinaryCopyFileWriter<W> {
    pub fn new<I>(types: I, writer: W) -> Self
    where
        I: IntoIterator<Item = Type>,
    {
        BinaryCopyFileWriter {
            types: types.into_iter().collect(),
            buf: BytesMut::new(),
            writer: BufWriter::with_capacity(5 * 1024 * 1024, writer),
        }
    }

    pub fn write_header(&mut self) -> io::Result<()> {
        self.writer.write_all(HEADER_MAGIC)?;
        self.writer.write_i32::<BigEndian>(0)?; // flags
        self.writer.write_i32::<BigEndian>(0)?; // header extension area length
        Ok(())
    }

    pub fn write_row(&mut self, row: &[&(dyn ToSql + Sync)]) -> io::Result<()> {
        if row.len() != self.types.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "row length mismatch"));
        }

        self.writer.write_u16::<BigEndian>(row.len() as u16)?;

        for (i, val) in row.iter().enumerate() {
            self.buf.clear();
            let is_null = val
                .to_sql_checked(&self.types[i], &mut self.buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            match is_null {
                IsNull::Yes => {
                    self.writer.write_i32::<BigEndian>(-1)?;
                }
                IsNull::No => {
                    self.writer.write_i32::<BigEndian>(self.buf.len() as i32)?;
                    self.writer.write_all(&self.buf)?;
                }
            }
        }

        Ok(())
    }

    pub fn write_footer(&mut self) -> io::Result<()> {
        self.writer.write_i16::<BigEndian>(-1)?;
        self.writer.flush()?;
        Ok(())
    }
}
