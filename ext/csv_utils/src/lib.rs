use magnus::{Error, Ruby};

// Create a module for the generator functionality
mod validator;
mod sorter;

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // Register the Sorter class
    sorter::register(ruby)?;
    validator::register(ruby)?;
    Ok(())
}