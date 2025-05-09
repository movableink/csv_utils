use std::io::{self, Write};
use byteorder::{BigEndian, WriteBytesExt};
use postgres::types::{Type, ToSql, IsNull};
use bytes::BytesMut;

const HEADER_MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// A writer for PostgreSQL binary-copy streams with runtime-defined column types.
pub struct BinaryCopyFileWriter {
    types: Vec<Type>,
    buf: BytesMut,
}

impl BinaryCopyFileWriter {
    /// Create a new writer, consuming any iterator of `Type`.
    pub fn new<I>(types: I) -> Self
    where
        I: IntoIterator<Item = Type>,
    {
        BinaryCopyFileWriter {
            types: types.into_iter().collect(),
            buf: BytesMut::new(),
        }
    }

    /// Write the 19-byte header plus two zero fields.
    pub fn write_header<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(HEADER_MAGIC)?;
        w.write_i32::<BigEndian>(0)?; // flags
        w.write_i32::<BigEndian>(0)?; // header extension area length
        Ok(())
    }

    /// Write a single row. `row.len()` must equal the number of types.
    pub fn write_row<W: Write>(
        &mut self,
        w: &mut W,
        row: &[&(dyn ToSql + Sync)],
    ) -> io::Result<()> {
        if row.len() != self.types.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "row length mismatch"));
        }

        w.write_u16::<BigEndian>(row.len() as u16)?;

        for (i, val) in row.iter().enumerate() {
            self.buf.clear();
            let is_null = val
                .to_sql_checked(&self.types[i], &mut self.buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            match is_null {
                IsNull::Yes => {
                    w.write_i32::<BigEndian>(-1)?;
                }
                IsNull::No => {
                    w.write_i32::<BigEndian>(self.buf.len() as i32)?;
                    w.write_all(&self.buf)?;
                }
            }
        }

        Ok(())
    }

    /// Write the end-of-data marker (`-1` as a signed 16-bit int).
    pub fn write_footer<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_i16::<BigEndian>(-1)?;
        Ok(())
    }
}
