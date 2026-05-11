use bon::Builder;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use url::Url;

use crate::airtable::client::HttpClient;

mod client;

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Record<F> {
    pub id: String,
    pub created_time: String,
    pub fields: F,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExistingRecord<F> {
    pub id: String,
    pub fields: F,
}

#[derive(Error, Debug)]
pub enum AirtableError {
    #[error("can't deserialize successful Airtable response (likely fields object is misformed)")]
    DeserializeSuccessResponse { response: String },

    #[error(r#"error from Airtable api for url "{url}" with status "{status}": {response}"#)]
    Airtable {
        status: StatusCode,
        response: String,
        url: Url,
    },

    #[error("url parsing error: {source}")]
    ParseUrl {
        #[from]
        source: url::ParseError,
    },

    #[error("http error: {source}")]
    Http {
        #[from]
        source: reqwest::Error,
    },
}

type Result<T> = std::result::Result<T, AirtableError>;

#[derive(Builder, Default)]
pub struct GetRecordsParams {
    filter_by_formula: Option<String>,
}

impl GetRecordsParams {
    fn url_params(&self) -> Vec<(&str, &str)> {
        let mut url_params = vec![];

        if let Some(filter_by_formula) = self.filter_by_formula.as_deref() {
            url_params.push(("filterByFormula", filter_by_formula));
        }

        url_params
    }
}

#[derive(Clone)]
pub struct AirtableClient {
    http_client: HttpClient,
}

impl AirtableClient {
    pub fn new(base_id: String, airtable_pat: String) -> Self {
        Self {
            http_client: HttpClient::new(base_id, airtable_pat),
        }
    }

    pub async fn get_record<F: DeserializeOwned>(
        &self,
        table_id: &str,
        id: &str,
    ) -> Result<Record<F>> {
        self.http_client
            .get(&format!("/{table_id}/{id}"), None)
            .await
    }

    pub async fn get_records<F: DeserializeOwned>(
        &self,
        table_id: &str,
        params: GetRecordsParams,
    ) -> Result<Vec<Record<F>>> {
        #[derive(Debug, Clone, Deserialize)]
        struct ListRecordsResponse<F> {
            records: Vec<Record<F>>,
            offset: Option<String>,
        }

        let mut all_records = vec![];
        let mut offset: Option<String> = None;

        loop {
            let params = {
                let mut base_params = params.url_params();
                if let Some(offset) = offset.as_deref() {
                    base_params.push(("offset", offset));
                }
                base_params
            };

            let response = self
                .http_client
                .get::<ListRecordsResponse<F>>(&format!("/{table_id}"), Some(params))
                .await?;

            all_records.extend(response.records);

            match response.offset {
                Some(next_offset) => offset = Some(next_offset),
                None => break,
            }
        }

        Ok(all_records)
    }

    pub async fn update_record<F: Serialize + DeserializeOwned>(
        &self,
        table_id: &str,
        record_id: &str,
        fields: &F,
    ) -> Result<Record<F>> {
        #[derive(Serialize, Deserialize, Clone, Debug)]
        struct UpdatedRecord<F> {
            pub fields: F,
        }

        self.http_client
            .request::<_, Record<F>>(
                &format!("/{table_id}/{record_id}"),
                None,
                Some(&UpdatedRecord { fields }),
                Method::PATCH,
            )
            .await
    }

    pub async fn update_records<'a, F, I>(
        &self,
        table_id: &str,
        records: I,
    ) -> Result<Vec<Record<F>>>
    where
        F: Serialize + DeserializeOwned + 'a,
        I: IntoIterator<Item = &'a ExistingRecord<F>>,
    {
        #[derive(Serialize, Clone, Debug)]
        struct UpdateRecordsRequest<'a, F> {
            records: Vec<&'a ExistingRecord<F>>,
        }

        #[derive(Deserialize, Clone, Debug)]
        struct UpdateRecordsResponse<F> {
            records: Vec<Record<F>>,
        }

        let records: Vec<_> = records.into_iter().collect();
        let mut returned_records = vec![];
        for chunk in records.chunks(10) {
            let body = UpdateRecordsRequest {
                records: chunk.to_vec(),
            };

            let response = self
                .http_client
                .request::<_, UpdateRecordsResponse<F>>(
                    &format!("/{table_id}"),
                    None,
                    Some(&body),
                    Method::PATCH,
                )
                .await?;

            returned_records.extend(response.records);
        }

        Ok(returned_records)
    }

    pub async fn create_records<'a, F, I>(
        &self,
        table_id: &str,
        new_records: I,
    ) -> Result<Vec<Record<F>>>
    where
        F: Serialize + DeserializeOwned + 'a,
        I: IntoIterator<Item = &'a F>,
    {
        #[derive(Clone, Serialize, Debug)]
        struct CreateRecordsRequest<'a, F> {
            records: Vec<NewRecord<&'a F>>,
        }

        #[derive(Serialize, Deserialize, Clone, Debug)]
        struct NewRecord<F> {
            pub fields: F,
        }

        #[derive(Clone, Deserialize, Debug)]
        struct CreateRecordsResponse<F> {
            records: Vec<Record<F>>,
        }

        let new_records: Vec<_> = new_records.into_iter().collect();
        let mut returned_records = vec![];
        for chunk in new_records.chunks(10) {
            let body = CreateRecordsRequest {
                records: chunk
                    .iter()
                    .map(|new_record| NewRecord { fields: new_record })
                    .collect(),
            };

            let response = self
                .http_client
                .request::<_, CreateRecordsResponse<F>>(
                    &format!("/{table_id}"),
                    None,
                    Some(&body),
                    Method::POST,
                )
                .await?;

            returned_records.extend(response.records);
        }

        Ok(returned_records)
    }
}
