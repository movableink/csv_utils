use magnus::{Error, Ruby};

mod binary_copy_file_writer;
mod postgres_copier;
mod sorter;
mod validator;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("CsvUtils")?;

    sorter::register(ruby, &module)?;
    validator::register(ruby, &module)?;

    Ok(())
}
