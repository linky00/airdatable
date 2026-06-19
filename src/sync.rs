use std::{collections::HashMap, error::Error, fmt::Display, hash::Hash};

use itertools::{Either, Itertools};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::airtable::{AirtableClient, AirtableError, ExistingRecord, Record};

pub trait DataObject {
    type Id: PartialEq + Eq + Hash;

    fn get_id(&self) -> Self::Id;
}

pub trait DataMirror {
    type Object: DataObject;

    fn get_mirror_id(&self) -> <<Self as DataMirror>::Object as DataObject>::Id;
}

pub struct SyncOutput<O: DataObject> {
    pub map: HashMap<O::Id, String>,
    pub updated_count: usize,
    pub created_count: usize,
    pub skipped_count: usize,
}

impl<O: DataObject> Display for SyncOutput<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} updated + {} created + {} skipped = {} total",
            self.updated_count,
            self.created_count,
            self.skipped_count,
            self.updated_count + self.created_count + self.skipped_count
        )
    }
}

#[derive(Error, Debug)]
pub enum SyncObjectsError {
    #[error("airtable error: {source}")]
    Airtable {
        #[from]
        source: AirtableError,
    },
    #[error("create fields error: {source}")]
    CreateFields {
        #[from]
        source: CreateFieldsError,
    },
}

#[derive(Error, Debug)]
#[error("{source}")]
pub struct CreateFieldsError {
    source: Box<dyn Error + Send + Sync + 'static>,
}

impl CreateFieldsError {
    pub fn new<E>(error: E) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync + 'static>>,
    {
        Self {
            source: error.into(),
        }
    }
}

impl AirtableClient {
    pub async fn sync_objects<'a, O, F, C>(
        &self,
        objects: impl IntoIterator<Item = &'a O>,
        existing_airtable_records: impl IntoIterator<Item = &'a Record<F>> + Clone,
        airtable_table_id: &str,
        create_fields: C,
    ) -> Result<SyncOutput<O>, SyncObjectsError>
    where
        O: DataObject + 'a,
        F: Serialize + DeserializeOwned + Eq + DataMirror<Object = O> + 'a,
        C: Fn(&O) -> Result<F, CreateFieldsError>,
    {
        // split update/create

        let (records_to_update, records_to_create): (Vec<_>, Vec<_>) =
            objects.into_iter().partition_map(|data_object| {
                if let Some(airtable_record) =
                    existing_airtable_records
                        .clone()
                        .into_iter()
                        .find(|airtable_record| {
                            airtable_record.fields.get_mirror_id() == data_object.get_id()
                        })
                {
                    Either::Left((data_object, airtable_record))
                } else {
                    Either::Right(data_object)
                }
            });

        // split update/skip, then update

        let (updated_airtable_records, skipped_airtable_records): (Vec<_>, Vec<_>) =
            records_to_update
                .iter()
                .map(
                    |(data_object, airtable_record)| -> Result<_, CreateFieldsError> {
                        let updated_airtable_record_result = Ok(ExistingRecord {
                            id: airtable_record.id.to_string(),
                            fields: create_fields(data_object)?,
                        });

                        updated_airtable_record_result.map(|updated_airtable_record| {
                            if updated_airtable_record.fields != airtable_record.fields {
                                Either::Left(updated_airtable_record)
                            } else {
                                Either::Right(updated_airtable_record)
                            }
                        })
                    },
                )
                .filter_map(Result::ok)
                .partition_map(|updated_airtable_record| updated_airtable_record);

        self.update_records(airtable_table_id, &updated_airtable_records)
            .await?;

        // create new

        let created_airtable_records = {
            let creating_airtable_records: Vec<_> = records_to_create
                .iter()
                .map(|data_object| create_fields(*data_object))
                .filter_map(Result::ok)
                .collect();

            self.create_records(airtable_table_id, &creating_airtable_records)
                .await?
        };

        // make data id -> airtable id map

        let updated_data_to_airtable_ids: Vec<(O::Id, String)> = updated_airtable_records
            .iter()
            .map(|updated_record| {
                (
                    updated_record.fields.get_mirror_id(),
                    updated_record.id.clone(),
                )
            })
            .collect();

        let skipped_data_to_airtable_ids: Vec<(O::Id, String)> = skipped_airtable_records
            .iter()
            .map(|skipped_record| {
                (
                    skipped_record.fields.get_mirror_id(),
                    skipped_record.id.clone(),
                )
            })
            .collect();

        let created_data_to_airtable_ids: Vec<(O::Id, String)> = created_airtable_records
            .iter()
            .map(|created_record| {
                (
                    created_record.fields.get_mirror_id(),
                    created_record.id.clone(),
                )
            })
            .collect();

        let map = updated_data_to_airtable_ids
            .into_iter()
            .chain(skipped_data_to_airtable_ids.into_iter())
            .chain(created_data_to_airtable_ids.into_iter())
            .collect();

        Ok(SyncOutput {
            map,
            updated_count: updated_airtable_records.len(),
            created_count: created_airtable_records.len(),
            skipped_count: skipped_airtable_records.len(),
        })
    }
}
