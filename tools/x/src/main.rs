#![forbid(unsafe_code)]

use std::process::exit;
use aptos_x_tool::Cli;
use clap::Parser;

fn main() {
    let result = Cli::parse().execute();

    // At this point, we'll want to print and determine whether to exit for an error code
    match result {
        Ok(_) => println!("Done"),
        Err(inner) => {
            println!("{}", inner);
            exit(1);
        },
    }
}