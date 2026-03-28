/// http.rs — HTTP client wrappers for querying Heimdall and Nominatim.

use std::time::{Duration, Instant};

use crate::types::QueryResult;

/// A geocoder HTTP client with configurable URLs and rate limiting.
pub struct GeocoderClient {
    client: reqwest::Client,
    pub heimdall_url: String,
    pub nominatim_url: String,
    rps: f64,
    last_nominatim: Instant,
}

impl GeocoderClient {
    pub fn new(heimdall_url: &str, nominatim_url: &str, rps: f64) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent("Heimdall-Geocoder-Test/1.0 (https://geoheim.com; comparison testing)")
            .timeout(Duration::from_secs(10))
            .build()?;

        Ok(Self {
            client,
            heimdall_url: heimdall_url.to_owned(),
            nominatim_url: nominatim_url.to_owned(),
            rps,
            last_nominatim: Instant::now() - Duration::from_secs(2),
        })
    }

    /// Enforce rate limit before Nominatim requests.
    async fn rate_limit(&mut self) {
        if self.rps <= 0.0 {
            return;
        }
        let interval = Duration::from_secs_f64(1.0 / self.rps);
        let elapsed = self.last_nominatim.elapsed();
        if elapsed < interval {
            tokio::time::sleep(interval - elapsed).await;
        }
        self.last_nominatim = Instant::now();
    }

    // -----------------------------------------------------------------------
    // Forward geocoding
    // -----------------------------------------------------------------------

    /// Query Heimdall forward search.
    pub async fn search_heimdall(
        &self,
        query: &str,
        country: Option<&str>,
    ) -> QueryResult {
        let start = Instant::now();
        let mut params: Vec<(&str, &str)> = vec![
            ("q", query),
            ("format", "json"),
            ("limit", "1"),
        ];
        let cc = country.map(|c| c.to_lowercase());
        if let Some(ref cc) = cc {
            params.push(("countrycodes", cc));
        }

        let result = self
            .client
            .get(format!("{}/search", self.heimdall_url))
            .query(&params)
            .send()
            .await;

        parse_search_response(result, start).await
    }

    /// Query Nominatim forward search with backoff/retry.
    pub async fn search_nominatim(
        &mut self,
        query: &str,
        country: Option<&str>,
    ) -> QueryResult {
        self.rate_limit().await;

        let cc = country.map(|c| c.to_lowercase());
        let mut backoff = Duration::from_millis(1000);

        for attempt in 0..5u32 {
            let start = Instant::now();
            let mut params: Vec<(&str, &str)> = vec![
                ("q", query),
                ("format", "json"),
                ("limit", "1"),
            ];
            if let Some(ref cc) = cc {
                params.push(("countrycodes", cc));
            }

            let result = self
                .client
                .get(format!("{}/search", self.nominatim_url))
                .query(&params)
                .send()
                .await;

            let latency_ms = start.elapsed().as_millis() as u64;

            match result {
                Ok(resp) => {
                    let status = resp.status();
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                    {
                        if attempt < 4 {
                            eprintln!(
                                "  Nominatim {} -- backing off {}ms",
                                status,
                                backoff.as_millis()
                            );
                            tokio::time::sleep(backoff).await;
                            backoff *= 2;
                            continue;
                        }
                        return QueryResult {
                            lat: None,
                            lon: None,
                            display_name: None,
                            latency_ms,
                            is_error: true,
                        };
                    }
                    return parse_json_response(resp, latency_ms).await;
                }
                Err(_) => {
                    if attempt < 4 {
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    } else {
                        return QueryResult {
                            lat: None,
                            lon: None,
                            display_name: None,
                            latency_ms: 0,
                            is_error: true,
                        };
                    }
                }
            }
        }

        QueryResult {
            lat: None,
            lon: None,
            display_name: None,
            latency_ms: 0,
            is_error: true,
        }
    }

    // -----------------------------------------------------------------------
    // Reverse geocoding
    // -----------------------------------------------------------------------

    /// Query Heimdall reverse geocode.
    pub async fn reverse_heimdall(&self, lat: f64, lon: f64) -> QueryResult {
        let start = Instant::now();
        let lat_s = lat.to_string();
        let lon_s = lon.to_string();

        let result = self
            .client
            .get(format!("{}/reverse", self.heimdall_url))
            .query(&[
                ("lat", lat_s.as_str()),
                ("lon", lon_s.as_str()),
                ("format", "json"),
                ("zoom", "18"),
            ])
            .send()
            .await;

        let latency_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(resp) => parse_reverse_response(resp, latency_ms).await,
            Err(_) => QueryResult {
                lat: None,
                lon: None,
                display_name: None,
                latency_ms,
                is_error: true,
            },
        }
    }

    /// Query Nominatim reverse geocode with backoff/retry.
    pub async fn reverse_nominatim(&mut self, lat: f64, lon: f64) -> QueryResult {
        self.rate_limit().await;

        let lat_s = lat.to_string();
        let lon_s = lon.to_string();
        let mut backoff = Duration::from_millis(1000);

        for attempt in 0..5u32 {
            let start = Instant::now();
            let result = self
                .client
                .get(format!("{}/reverse", self.nominatim_url))
                .query(&[
                    ("lat", lat_s.as_str()),
                    ("lon", lon_s.as_str()),
                    ("format", "json"),
                    ("zoom", "18"),
                ])
                .send()
                .await;

            let latency_ms = start.elapsed().as_millis() as u64;

            match result {
                Ok(resp) => {
                    let status = resp.status();
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                    {
                        if attempt < 4 {
                            eprintln!(
                                "  Nominatim {} -- backing off {}ms",
                                status,
                                backoff.as_millis()
                            );
                            tokio::time::sleep(backoff).await;
                            backoff *= 2;
                            continue;
                        }
                        return QueryResult {
                            lat: None,
                            lon: None,
                            display_name: None,
                            latency_ms,
                            is_error: true,
                        };
                    }
                    return parse_reverse_response(resp, latency_ms).await;
                }
                Err(_) => {
                    if attempt < 4 {
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                    } else {
                        return QueryResult {
                            lat: None,
                            lon: None,
                            display_name: None,
                            latency_ms: 0,
                            is_error: true,
                        };
                    }
                }
            }
        }

        QueryResult {
            lat: None,
            lon: None,
            display_name: None,
            latency_ms: 0,
            is_error: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Response parsing
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct SearchResult {
    lat: String,
    lon: String,
    display_name: String,
}

#[derive(serde::Deserialize)]
struct ReverseResult {
    lat: Option<String>,
    lon: Option<String>,
    display_name: Option<String>,
}

async fn parse_search_response(
    result: Result<reqwest::Response, reqwest::Error>,
    start: Instant,
) -> QueryResult {
    let latency_ms = start.elapsed().as_millis() as u64;
    match result {
        Ok(resp) => parse_json_response(resp, latency_ms).await,
        Err(_) => QueryResult {
            lat: None,
            lon: None,
            display_name: None,
            latency_ms,
            is_error: true,
        },
    }
}

async fn parse_json_response(resp: reqwest::Response, latency_ms: u64) -> QueryResult {
    match resp.json::<Vec<SearchResult>>().await {
        Ok(results) => {
            if let Some(r) = results.first() {
                QueryResult {
                    lat: r.lat.parse().ok(),
                    lon: r.lon.parse().ok(),
                    display_name: Some(r.display_name.clone()),
                    latency_ms,
                    is_error: false,
                }
            } else {
                QueryResult {
                    lat: None,
                    lon: None,
                    display_name: None,
                    latency_ms,
                    is_error: false,
                }
            }
        }
        Err(_) => QueryResult {
            lat: None,
            lon: None,
            display_name: None,
            latency_ms,
            is_error: true,
        },
    }
}

async fn parse_reverse_response(resp: reqwest::Response, latency_ms: u64) -> QueryResult {
    // Nominatim reverse returns a single object, not an array
    // Try single object first, then array
    let text = match resp.text().await {
        Ok(t) => t,
        Err(_) => {
            return QueryResult {
                lat: None,
                lon: None,
                display_name: None,
                latency_ms,
                is_error: true,
            }
        }
    };

    // Try as single object (Nominatim standard reverse response)
    if let Ok(r) = serde_json::from_str::<ReverseResult>(&text) {
        let lat = r.lat.as_deref().and_then(|s| s.parse().ok());
        let lon = r.lon.as_deref().and_then(|s| s.parse().ok());
        return QueryResult {
            lat,
            lon,
            display_name: r.display_name,
            latency_ms,
            is_error: false,
        };
    }

    // Try as array (Heimdall might return array for reverse too)
    if let Ok(results) = serde_json::from_str::<Vec<SearchResult>>(&text) {
        if let Some(r) = results.first() {
            return QueryResult {
                lat: r.lat.parse().ok(),
                lon: r.lon.parse().ok(),
                display_name: Some(r.display_name.clone()),
                latency_ms,
                is_error: false,
            };
        }
    }

    // Check for error response (Nominatim returns {"error": "..."} for no results)
    if text.contains("\"error\"") {
        return QueryResult {
            lat: None,
            lon: None,
            display_name: None,
            latency_ms,
            is_error: false, // Not an error — just no result at that location
        };
    }

    QueryResult {
        lat: None,
        lon: None,
        display_name: None,
        latency_ms,
        is_error: true,
    }
}
