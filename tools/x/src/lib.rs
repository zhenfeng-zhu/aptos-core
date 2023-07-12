mod common;

use clap::Parser;
use common::ChangeSet;
use guppy::graph::DependencyDirection;

#[derive(Parser)]
#[clap(name = "x", author, version)]
pub enum Cli {
    Test(TestCommand),
}

impl Cli {
    pub fn execute(&self) -> anyhow::Result<()> {
        match self {
            Cli::Test(inner) => inner.execute(),
        }
    }
}

#[derive(Parser, Debug)]
pub struct TestCommand {}

impl TestCommand {
    pub fn execute(&self) -> anyhow::Result<()> {
        let change_set = ChangeSet::init()?;
        let determinator_set = change_set.determine_changed_packages();

        // determinator_set.affected_set contains the workspace packages directly or indirectly affected
        // by the change.
        for package in determinator_set
            .affected_set
            .packages(DependencyDirection::Forward)
        {
            println!("affected: {}", package.name());
        }

        Ok(())
    }
}
