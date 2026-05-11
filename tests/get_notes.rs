use anyhow::Result;

use crate::common::TestBase;

mod common;

#[tokio::test]
async fn get_notes() -> Result<()> {
    let test_base = TestBase::new()?;

    let notes: Vec<_> = test_base
        .get_notes()
        .await?
        .into_iter()
        .map(|record| record.fields.note)
        .collect();

    println!("{notes:?}");

    Ok(())
}
