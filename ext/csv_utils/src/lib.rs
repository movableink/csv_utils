use magnus::{Error, Ruby};

mod validator;
mod sorter;
mod postgres_copier;
mod binary_copy_file_writer;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("CsvUtils")?;

    sorter::register(ruby, &module)?;
    validator::register(ruby, &module)?;

    Ok(())
}