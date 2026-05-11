use airdatable::airtable::GetRecordsParams;
use anyhow::Result;

use crate::common::TestBase;

mod common;

#[tokio::test]
async fn get_non_empty_notes() -> Result<()> {
    let test_base = TestBase::new()?;

    let params = GetRecordsParams::builder()
        .filter_by_formula("NOT(Note = BLANK())".to_string())
        .build();

    let notes = test_base.get_notes(params).await?;

    println!("{notes:?}");

    let empty_notes = notes.iter().filter(|record| {
        record
            .fields
            .note
            .as_ref()
            .is_none_or(|note| note.is_empty())
    });

    assert_eq!(empty_notes.count(), 0);

    Ok(())
}
