use std::{num::NonZero, sync::Arc};

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use reqwest::{Client, Method, Url};
use serde::{Serialize, de::DeserializeOwned};

use crate::airtable::{AirtableError, Result};

const MAX_REQUESTS_PER_SECOND: u32 = 5;
const QUOTA: Quota = Quota::per_second(NonZero::new(MAX_REQUESTS_PER_SECOND).unwrap());
const API_BASE_URL: &str = "https://api.airtable.com/v0";

#[derive(Clone)]
pub struct HttpClient {
    client: Client,
    rate_limiter: Arc<DefaultDirectRateLimiter>,
    base_url: String,
    airtable_pat: String,
}

impl HttpClient {
    pub fn new(base_id: String, airtable_pat: String) -> Self {
        Self {
            client: Client::new(),
            rate_limiter: Arc::new(RateLimiter::direct(QUOTA)),
            base_url: format!("{API_BASE_URL}/{base_id}"),
            airtable_pat,
        }
    }

    pub async fn get<Res: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: Option<Vec<(&str, &str)>>,
    ) -> Result<Res> {
        self.request(endpoint, params, None::<&()>, Method::GET)
            .await
    }

    pub async fn request<Req: Serialize, Res: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: Option<Vec<(&str, &str)>>,
        body: Option<&Req>,
        method: Method,
    ) -> Result<Res> {
        self.rate_limiter.until_ready().await;

        let url = {
            let url_without_params = format!("{}{}", self.base_url, endpoint);

            if let Some(params) = params {
                Url::parse_with_params(&url_without_params, params)
            } else {
                Url::parse(&url_without_params)
            }
        }?;

        let request = {
            let request_without_body = self
                .client
                .request(method, url)
                .bearer_auth(self.airtable_pat.clone());

            if let Some(body) = body {
                request_without_body.json(body)
            } else {
                request_without_body
            }
        };

        let response = request.send().await?;
        let status = response.status();

        if status.is_success() {
            let body = response.text().await?;
            serde_json::from_str(&body)
                .map_err(|_| AirtableError::DeserializeSuccessResponse { response: body })
        } else {
            let body = response.text().await?;

            Err(AirtableError::Airtable {
                status,
                response: body,
            })
        }
    }
}
