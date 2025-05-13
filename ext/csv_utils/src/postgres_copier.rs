use crate::binary_copy_file_writer::BinaryCopyFileWriter;
use crate::sorter::SortRecord;
use postgis::ewkb::Point;
use postgres::types::Kind;
use postgres::types::ToSql;
use postgres::types::Type;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::time::SystemTime;
pub type GeoIndexes = (usize, usize);

const BUFFER_CAPACITY: usize = 5 * 1024 * 1024;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

pub struct PostgresCopier {
    reader: BufReader<File>,
    geo_indexes: Option<GeoIndexes>,
    source_key: String,
}

impl PostgresCopier {
    pub fn new(
        input_file: &Path,
        geo_indexes: Option<GeoIndexes>,
        source_key: String,
    ) -> Result<Self, std::io::Error> {
        let reader = BufReader::with_capacity(BUFFER_CAPACITY, File::open(input_file)?);

        Ok(Self {
            reader,
            geo_indexes,
            source_key,
        })
    }

    fn iter_records(
        &mut self,
    ) -> impl Iterator<Item = Result<([u8; 20], Option<Point>, Vec<String>), std::io::Error>> + '_
    {
        std::iter::from_fn(move || {
            let mut len_bytes = [0u8; 4];
            if self.reader.read_exact(&mut len_bytes).is_err() {
                return None; // EOF
            }
            let len = u32::from_le_bytes(len_bytes) as usize;

            let mut bytes = vec![0u8; len];
            if let Err(e) = self.reader.read_exact(&mut bytes) {
                return Some(Err(e));
            }

            let record: SortRecord =
                match bincode::decode_from_slice(&bytes, bincode::config::legacy()) {
                    Ok((record, _)) => record,
                    Err(e) => {
                        return Some(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
                    }
                };

            let target_key = record.key.value;
            let geo_key = if self.geo_indexes.is_some() {
                self.generate_geo_key(&record.record)
            } else {
                None
            };

            Some(Ok((target_key, geo_key, record.record)))
        })
    }

    fn generate_geo_key(&self, row: &[String]) -> Option<Point> {
        let latitude_str = row.get(self.geo_indexes.unwrap().0)?;
        let longitude_str = row.get(self.geo_indexes.unwrap().1)?;

        let latitude = latitude_str.parse::<f64>().ok()?;
        let longitude = longitude_str.parse::<f64>().ok()?;

        Some(Point::new(longitude, latitude, Some(4326)))
    }

    pub fn copy(&mut self, output_file_path: &Path) -> Result<(), std::io::Error> {
        let now = SystemTime::now();
        let source_key = self.source_key.clone();

        let geometry_type = Self::make_geometry_type();

        let types = vec![
            Type::VARCHAR,
            Type::VARCHAR,
            geometry_type,
            Type::VARCHAR_ARRAY,
            Type::TIMESTAMP,
            Type::TIMESTAMP,
        ];
        let output_file = File::create(output_file_path)?;
        let mut writer = BinaryCopyFileWriter::new(types, output_file);

        writer.write_header()?;

        for result in self.iter_records() {
            let (target_key, geo_key, record) = result?;
            let mut hex = String::with_capacity(40);
            for &byte in &target_key {
                hex.push(HEX_DIGITS[(byte >> 4) as usize] as char);
                hex.push(HEX_DIGITS[(byte & 0xf) as usize] as char);
            }
            let row: Vec<&(dyn ToSql + Sync)> =
                vec![&source_key, &hex, &geo_key, &record, &now, &now];
            writer.write_row(&row)?;
        }

        writer.write_footer()?;
        Ok(())
    }

    fn make_geometry_type() -> Type {
        Type::new(
            "geometry".to_string(),
            Type::POINT.oid(),
            Kind::Simple,
            "public".to_string(),
        )
    }
}
