use magnus::{Error, Ruby};

// Create a module for the generator functionality
mod validator;
mod sorter;
mod postgres_copier;
mod binary_copy_file_writer;
mod geometry;

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("CsvUtils")?;

    sorter::register(ruby, &module)?;
    validator::register(ruby, &module)?;

    Ok(())
}