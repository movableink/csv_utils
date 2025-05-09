use std::io::{BufReader, Read};
use crate::binary_copy_file_writer::BinaryCopyFileWriter;
use std::fs::File;
use postgres::types::Type;
use sha1::{Digest, Sha1};
use std::time::SystemTime;
use std::path::Path;
use postgres::types::ToSql;
use crate::geometry::Geometry;
pub type GeoIndexes = (usize, usize);

pub struct PostgresCopier {
  reader: BufReader<File>,
  targeting_indexes: Vec<usize>,
  geo_indexes: Option<GeoIndexes>,
  source_key: String,
}

impl PostgresCopier {
  pub fn new(input_file: &Path, targeting_indexes: Vec<usize>, geo_indexes: Option<GeoIndexes>, source_key: String) -> Result<Self, std::io::Error> {
    let reader = BufReader::new(File::open(input_file)?);
    
    Ok(Self { reader, targeting_indexes, geo_indexes, source_key })
  }

  fn iter_records(&mut self) -> impl Iterator<Item = Result<(String, Option<Geometry>, Vec<String>), std::io::Error>> + '_ {
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
      
      let record: Vec<String> = match bincode::deserialize(&bytes) {
        Ok(r) => r,
        Err(e) => return Some(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))),
      };

      let target_key = self.generate_targeting_key(&record);
      let geo_key = if self.geo_indexes.is_some() {
        self.generate_geo_key(&record)
      } else {
        None
      };

      Some(Ok((target_key, geo_key, record)))
    })
  }

  fn generate_targeting_key(&self, row: &[String]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(self.source_key.as_bytes());
    hasher.update(b",");
    
    for (i, &col) in self.targeting_indexes.iter().enumerate() {
        if let Some(val) = row.get(col) {
            hasher.update(val.as_bytes());
            if i < self.targeting_indexes.len() - 1 {
                hasher.update(b",");
            }
        }
    }

    let digest = hasher.finalize();
    format!("{:x}", digest)
  }

  fn generate_geo_key(&self, row: &[String]) -> Option<Geometry> {
    let latitude_str = row.get(self.geo_indexes.unwrap().0)?;
    let longitude_str = row.get(self.geo_indexes.unwrap().1)?;

    let latitude = latitude_str.parse::<f64>().ok()?;
    let longitude = longitude_str.parse::<f64>().ok()?;

    Some(Geometry::new(longitude, latitude, Some(4326)))
  }

  pub fn copy(&mut self, output_file_path: &Path) -> Result<(), std::io::Error> {
    let now = SystemTime::now();
    let source_key = self.source_key.clone();

    let types = vec![
      Type::VARCHAR,
      Type::VARCHAR,
      Geometry::as_type(),
      Type::VARCHAR_ARRAY,
      Type::TIMESTAMP,
      Type::TIMESTAMP,
    ];
    let mut writer = BinaryCopyFileWriter::new(types);
    let mut output_file = File::create(output_file_path)?;
    writer.write_header(&mut output_file)?;

    for result in self.iter_records() {
      let (target_key, geo_key, record) = result?;
      let row: Vec<&(dyn ToSql + Sync)> = vec![
        &source_key,
        &target_key,
        &geo_key,
        &record,
        &now,
        &now
      ];
      writer.write_row(&mut output_file, &row)?;
    }

    writer.write_footer(&mut output_file)?;
    Ok(())
  }
}