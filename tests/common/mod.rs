#![allow(dead_code)]

use std::env;

use airdatable::{
    airtable::{AirtableClient, AirtableError, GetRecordsParams, Record},
    sync::{DataMirror, DataObject, SyncObjectsError, SyncOutput},
};
use anyhow::Result;
use serde::{Deserialize, Serialize};

pub struct TestBase {
    client: AirtableClient,
    notes_table_id: String,
}

impl TestBase {
    pub fn new() -> Result<Self> {
        dotenvy::dotenv()?;

        let client = AirtableClient::new(env::var("BASE_ID")?, env::var("AIRTABLE_PAT")?);
        let notes_table_id = env::var("NOTES_TABLE_ID")?;

        Ok(Self {
            client,
            notes_table_id,
        })
    }

    pub async fn get_notes(
        &self,
        params: GetRecordsParams,
    ) -> Result<Vec<Record<NoteFields>>, AirtableError> {
        self.client.get_records(&self.notes_table_id, params).await
    }

    pub async fn create_notes(
        &self,
        notes_fields: &[NoteFields],
    ) -> Result<Vec<Record<NoteFields>>, AirtableError> {
        self.client
            .create_records(&self.notes_table_id, notes_fields)
            .await
    }

    pub async fn sync_foreign_notes(
        &self,
        foreign_notes: &[ForeignNote],
        existing_notes: &[Record<NoteFields>],
    ) -> Result<SyncOutput<ForeignNote>, SyncObjectsError> {
        self.client
            .sync_objects(
                foreign_notes,
                existing_notes,
                &self.notes_table_id,
                |foreign_note| {
                    Ok(NoteFields {
                        foreign_id: foreign_note.id,
                        note: Some(foreign_note.content.clone()),
                    })
                },
            )
            .await
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct NoteFields {
    #[serde(rename = "Foreign ID")]
    pub foreign_id: u32,

    #[serde(rename = "Note")]
    pub note: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ForeignNote {
    pub id: u32,
    pub content: String,
}

impl DataObject for ForeignNote {
    type Id = u32;

    fn get_id(&self) -> u32 {
        self.id
    }
}

impl DataMirror for NoteFields {
    type Object = ForeignNote;

    fn get_mirror_id(&self) -> u32 {
        self.foreign_id
    }
}
