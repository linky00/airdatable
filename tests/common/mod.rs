use std::env;

use airdatable::airtable::{AirtableClient, AirtableError, Record};
use anyhow::Result;
use serde::Deserialize;

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

    pub async fn get_notes(&self) -> Result<Vec<Record<NoteFields>>, AirtableError> {
        self.client.get_records(&self.notes_table_id).await
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct NoteFields {
    #[serde(rename = "Note")]
    pub note: String,
}
