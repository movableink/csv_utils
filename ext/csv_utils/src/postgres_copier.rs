use crate::binary_copy_file_writer::BinaryCopyFileWriter;
use crate::sorter::SortRecord;
use faster_hex::hex_encode;
use log::{debug, error, info, trace, warn};
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

pub struct PostgresCopier {
    reader: BufReader<File>,
    geo_indexes: Option<GeoIndexes>,
    source_key: String,
}

type RowItem = ([u8; 20], Option<Point>, Vec<String>);

impl PostgresCopier {
    pub fn new(
        input_file: File,
        geo_indexes: Option<GeoIndexes>,
        source_key: String,
    ) -> Result<Self, std::io::Error> {
        let reader = BufReader::with_capacity(BUFFER_CAPACITY, input_file);

        info!(
            target: "csv_utils::postgres_copier",
            "Created PostgresCopier for source key: {}",
            source_key
        );

        if let Some(indexes) = geo_indexes {
            debug!(
                target: "csv_utils::postgres_copier",
                "Geo indexes configured: latitude={}, longitude={}",
                indexes.0, indexes.1
            );
        } else {
            debug!(
                target: "csv_utils::postgres_copier",
                "No geo indexes configured"
            );
        }

        Ok(Self {
            reader,
            geo_indexes,
            source_key,
        })
    }

    fn iter_records(&mut self) -> impl Iterator<Item = Result<RowItem, std::io::Error>> + '_ {
        debug!(
            target: "csv_utils::postgres_copier",
            "Starting to iterate through records"
        );

        std::iter::from_fn(move || {
            let mut len_bytes = [0u8; 4];
            if self.reader.read_exact(&mut len_bytes).is_err() {
                debug!(
                    target: "csv_utils::postgres_copier",
                    "Reached end of input file"
                );
                return None; // EOF
            }
            let len = u32::from_le_bytes(len_bytes) as usize;

            let mut bytes = vec![0u8; len];
            if let Err(e) = self.reader.read_exact(&mut bytes) {
                error!(
                    target: "csv_utils::postgres_copier",
                    "Error reading record bytes: {}", e
                );
                return Some(Err(e));
            }

            let record: SortRecord =
                match bincode::decode_from_slice(&bytes, bincode::config::legacy()) {
                    Ok((record, _)) => record,
                    Err(e) => {
                        error!(
                            target: "csv_utils::postgres_copier",
                            "Error decoding record: {}", e
                        );
                        return Some(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)));
                    }
                };

            let target_key = record.key.value;
            let geo_key = if self.geo_indexes.is_some() {
                self.generate_geo_key(&record.record)
            } else {
                None
            };

            trace!(
                target: "csv_utils::postgres_copier",
                "Processed record with key: {}",
                std::str::from_utf8(&target_key).unwrap_or("<invalid utf8>")
            );

            Some(Ok((target_key, geo_key, record.record)))
        })
    }

    fn generate_geo_key(&self, row: &[String]) -> Option<Point> {
        let (lat_idx, lon_idx) = self.geo_indexes.unwrap();

        let latitude_str = match row.get(lat_idx) {
            Some(s) => s,
            None => {
                warn!(
                    target: "csv_utils::postgres_copier",
                    "Missing latitude value at index {}", lat_idx
                );
                return None;
            }
        };

        let longitude_str = match row.get(lon_idx) {
            Some(s) => s,
            None => {
                warn!(
                    target: "csv_utils::postgres_copier",
                    "Missing longitude value at index {}", lon_idx
                );
                return None;
            }
        };

        let latitude = match latitude_str.parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    target: "csv_utils::postgres_copier",
                    "Invalid latitude value '{}': {}", latitude_str, e
                );
                return None;
            }
        };

        let longitude = match longitude_str.parse::<f64>() {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    target: "csv_utils::postgres_copier",
                    "Invalid longitude value '{}': {}", longitude_str, e
                );
                return None;
            }
        };

        let point = Point::new(longitude, latitude, Some(4326));
        trace!(
            target: "csv_utils::postgres_copier",
            "Generated geo point: lon={}, lat={}", longitude, latitude
        );

        Some(point)
    }

    pub fn copy(&mut self, output_file_path: &Path) -> Result<(), std::io::Error> {
        info!(
            target: "csv_utils::postgres_copier",
            "Starting COPY to PostgreSQL binary format: {}",
            output_file_path.display()
        );

        let now = SystemTime::now();
        let source_key = self.source_key.clone();

        let geometry_type = Self::make_geometry_type();
        debug!(
            target: "csv_utils::postgres_copier",
            "Using geometry type: {}", geometry_type
        );

        let types = vec![
            Type::VARCHAR,
            Type::VARCHAR,
            geometry_type,
            Type::VARCHAR_ARRAY,
            Type::TIMESTAMP,
            Type::TIMESTAMP,
        ];

        let output_file = match File::create(output_file_path) {
            Ok(f) => f,
            Err(e) => {
                error!(
                    target: "csv_utils::postgres_copier",
                    "Failed to create output file {}: {}",
                    output_file_path.display(), e
                );
                return Err(e);
            }
        };

        let mut writer = BinaryCopyFileWriter::new(types, output_file);
        debug!(
            target: "csv_utils::postgres_copier",
            "Created binary copy file writer"
        );

        writer.write_header()?;
        let mut row_count = 0;
        let mut hex_target_key = [0u8; 40];

        for result in self.iter_records() {
            match result {
                Ok((target_key, geo_key, record)) => {
                    hex_encode(&target_key, &mut hex_target_key).unwrap();
                    let hex_target_key_str = String::from_utf8(hex_target_key.to_vec()).unwrap();

                    let row: Vec<&(dyn ToSql + Sync)> = vec![
                        &source_key,
                        &hex_target_key_str,
                        &geo_key,
                        &record,
                        &now,
                        &now,
                    ];
                    writer.write_row(&row)?;
                    row_count += 1;

                    if row_count % 10000 == 0 {
                        debug!(
                            target: "csv_utils::postgres_copier",
                            "Processed {} rows", row_count
                        );
                    }
                }
                Err(e) => {
                    error!(
                        target: "csv_utils::postgres_copier",
                        "Error processing row: {}", e
                    );
                    return Err(e);
                }
            }
        }

        writer.write_footer()?;
        info!(
            target: "csv_utils::postgres_copier",
            "Completed PostgreSQL binary copy with {} rows", row_count
        );

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
