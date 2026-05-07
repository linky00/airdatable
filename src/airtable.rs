use std::{fmt::Debug, num::NonZero, sync::Arc};

use anyhow::{Context, Ok, Result, anyhow};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use reqwest::{Client, Method, Response, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const MAX_REQUESTS_PER_SECOND: u32 = 5;
const API_BASE_URL: &str = "https://api.airtable.com/v0";

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

#[derive(Clone)]
pub struct AirtableClient {
    airtable_pat: String,
    base_url: String,
    client: Client,
    rate_limiter: Arc<DefaultDirectRateLimiter>,
}

impl AirtableClient {
    pub fn new(airtable_pat: String, base_id: String) -> Self {
        Self {
            airtable_pat,
            base_url: format!("{API_BASE_URL}/{base_id}"),
            client: Client::new(),
            rate_limiter: Arc::new(RateLimiter::direct(Quota::per_second(
                NonZero::new(MAX_REQUESTS_PER_SECOND)
                    .expect("MAX_REQUESTS_PER_SECOND should not be zero"),
            ))),
        }
    }

    pub async fn get_record<F: DeserializeOwned>(
        &self,
        table_id: &str,
        id: &str,
    ) -> Result<Record<F>> {
        self.get(&format!("/{table_id}/{id}"), None)
            .await
            .context("Failed to get record")
    }

    pub async fn get_records<F: DeserializeOwned>(
        &self,
        table_id: &str,
        add_params: Option<&[(&str, &str)]>,
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
                let mut params = add_params.map(|params| params.to_owned());

                if let Some(params) = &mut params
                    && let Some(offset) = &offset
                {
                    params.push(("offset", offset));
                }

                params
            };

            let response = self
                .get::<ListRecordsResponse<F>>(&format!("/{table_id}"), params.as_deref())
                .await
                .context("Failed to get records")?;

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
        self.patch::<_, Record<F>>(&format!("/{table_id}/{record_id}"), &NewRecord { fields })
            .await
            .context("Failed to update record")
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
                .patch::<_, UpdateRecordsResponse<F>>(&format!("/{table_id}"), &body)
                .await
                .context("Failed to update records")?;

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
                .post::<_, CreateRecordsResponse<F>>(&format!("/{table_id}"), &body)
                .await?;

            returned_records.extend(response.records);
        }

        Ok(returned_records)
    }

    async fn get<F: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: Option<&[(&str, &str)]>,
    ) -> Result<F> {
        self.rate_limiter.until_ready().await;

        let base_url = &format!("{}{}", self.base_url, endpoint);

        let url = match params {
            Some(params) => Url::parse_with_params(base_url, params)?,
            None => Url::parse(base_url)?,
        };

        let response = self
            .client
            .get(url)
            .bearer_auth(self.airtable_pat.clone())
            .send()
            .await?;

        Ok(response_result_with_error_body(response)
            .await?
            .json()
            .await?)
    }

    async fn post<Req: Serialize, Res: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &Req,
    ) -> Result<Res> {
        self.write_method(endpoint, body, Method::POST).await
    }

    async fn patch<Req: Serialize, Res: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &Req,
    ) -> Result<Res> {
        self.write_method(endpoint, body, Method::PATCH).await
    }

    async fn write_method<Req: Serialize, Res: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &Req,
        method: Method,
    ) -> Result<Res> {
        self.rate_limiter.until_ready().await;

        let response = self
            .client
            .request(method, format!("{}{}", self.base_url, endpoint))
            .bearer_auth(self.airtable_pat.clone())
            .json(body)
            .send()
            .await?;

        Ok(response_result_with_error_body(response)
            .await?
            .json()
            .await?)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct NewRecord<F> {
    pub fields: F,
}

async fn response_result_with_error_body(response: Response) -> Result<Response> {
    if !(response.status().is_client_error() || response.status().is_server_error()) {
        Ok(response)
    } else {
        let status = response.status();
        let error_body = response.text().await?;

        Err(anyhow!(
            "Airtable API error ({status}) with body: {error_body}"
        ))
    }
}
