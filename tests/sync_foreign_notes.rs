use airdatable::airtable::GetRecordsParams;
use anyhow::Result;

use crate::common::{ForeignNote, TestBase};

mod common;

#[tokio::test]
async fn sync_foreign_notes() -> Result<()> {
    let base = TestBase::new()?;

    let foreign_notes_db = &[
        ForeignNote {
            id: 0,
            content: "hello :3".to_string(),
        },
        ForeignNote {
            id: 1,
            content: "hi :3".to_string(),
        },
        ForeignNote {
            id: 2,
            content: "hey :3".to_string(),
        },
    ];

    let only_foreign_notes_params = || {
        GetRecordsParams::builder()
            .filter_by_formula("{Foreign ID} != BLANK()".to_string())
            .build()
    };

    for _ in 0..2 {
        let existing_foreign_note_records = base.get_notes(only_foreign_notes_params()).await?;

        base.sync_foreign_notes(foreign_notes_db, &existing_foreign_note_records)
            .await?;

        let new_foreign_note_records = base.get_notes(only_foreign_notes_params()).await?;

        assert!(foreign_notes_db.iter().all(|foreign_note| {
            new_foreign_note_records
                .iter()
                .filter(|record| {
                    record.fields.foreign_id == foreign_note.id
                        && record
                            .fields
                            .note
                            .as_ref()
                            .is_some_and(|note_content| *note_content == foreign_note.content)
                })
                .count()
                == 1
        }));
    }

    Ok(())
}
