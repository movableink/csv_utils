// geometry.rs
use postgres::types::{Type, Kind, Oid, IsNull, ToSql, to_sql_checked};
use bytes::BytesMut;
use std::error::Error;
use std::io::Write;
use postgis::ewkb::EwkbWrite;
use postgis::ewkb::Point;
use postgis::error::Error as PostgisError;
use byteorder::{LittleEndian, WriteBytesExt};

#[derive(Debug)]
pub struct Geometry {
    srid: Option<i32>,
    x: f64,
    y: f64,
    z: Option<f64>,
    m: Option<f64>,
}

impl Geometry {
    pub fn new(x: f64, y: f64, srid: Option<i32>) -> Self {
        let point = Point { x, y, srid };

        Self {
            srid,
            x,
            y,
            z: None,
            m: None,
        }
    }

    pub fn as_type() -> Type {
        Type::new(
            "geometry".to_string(),
            Type::POINT.oid(),
            Kind::Simple,
            "public".to_string()
        )
    }
}

impl<'a> ToSql for Geometry {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.write_ewkb(&mut BytesMutWriter(out))?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.name() {
            "geography" | "geometry" => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

impl EwkbWrite for Geometry {
    fn type_id(&self) -> u32 {
        let mut type_id = 0x01;  // Base Point type
        if self.z.is_some() {
            type_id |= 0x80000000;  // Z flag
        }
        if self.m.is_some() {
            type_id |= 0x40000000;  // M flag
        }
        if self.srid.is_some() {
            type_id |= 0x20000000;  // SRID flag
        }
        type_id
    }

    fn opt_srid(&self) -> Option<i32> {
        self.srid
    }

    fn write_ewkb_body<W: Write + ?Sized>(&self, writer: &mut W) -> Result<(), PostgisError> {
        writer.write_f64::<LittleEndian>(self.x)?;
        writer.write_f64::<LittleEndian>(self.y)?;
        if let Some(z) = self.z {
            writer.write_f64::<LittleEndian>(z)?;
        }
        if let Some(m) = self.m {
            writer.write_f64::<LittleEndian>(m)?;
        }
        Ok(())
    }
}

struct BytesMutWriter<'a>(&'a mut BytesMut);

impl<'a> Write for BytesMutWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}