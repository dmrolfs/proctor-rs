use crate::error::IncompatibleSourceSettingsError;
use crate::serde::{deserialize_duration_secs, deserialize_from_str, serialize_duration_secs, serialize_to_str};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, Url};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceSetting {
    RestApi(HttpQuery),
    Csv { path: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpQuery {
    #[serde(
        rename = "interval_secs",
        serialize_with = "serialize_duration_secs",
        deserialize_with = "deserialize_duration_secs"
    )]
    pub interval: Duration,

    #[serde(serialize_with = "serialize_to_str", deserialize_with = "deserialize_from_str")]
    pub method: Method,

    #[serde(serialize_with = "serialize_to_str", deserialize_with = "deserialize_from_str")]
    pub url: Url,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,

    #[serde(default = "HttpQuery::default_max_retries")]
    pub max_retries: u32,
}

impl HttpQuery {
    pub fn default_max_retries() -> u32 {
        3
    }

    pub fn header_map(&self) -> Result<HeaderMap, IncompatibleSourceSettingsError> {
        let mut map = HeaderMap::with_capacity(self.headers.len());
        for (k, v) in self.headers.iter() {
            let name = HeaderName::from_str(k.as_str()); //.map_err::<IncompatibleSourceSettingsError, _>(|err| err.into());
            let value = HeaderValue::from_str(v.as_str()); //.map_err::<SettingsError, _>(|err| err.into());
            map.insert(name?, value?);
        }
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header;
    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_serde_http_query() {
        let mut header_map = HeaderMap::new();
        header_map.insert(header::AUTHORIZATION, "Basic Zm9vOmJhcg==".parse().unwrap());
        header_map.insert(header::HOST, "example.com".parse().unwrap());
        let header_vec = header_map
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect();

        let endpoint = HttpQuery {
            interval: Duration::from_secs(33),
            method: Method::GET,
            url: Url::parse(
                "https://httpbin.org/get?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z",
            )
            .unwrap(),
            headers: header_vec,
            max_retries: 3,
        };

        assert_tokens(
            &endpoint,
            &[
                Token::Struct { name: "HttpQuery", len: 5 },
                Token::Str("interval_secs"),
                Token::U64(33),
                Token::Str("method"),
                Token::Str("GET"),
                Token::Str("url"),
                Token::Str("https://httpbin.org/get?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z"),
                Token::Str("headers"),
                Token::Seq { len: Some(2) },
                Token::Tuple { len: 2 },
                Token::Str("authorization"),
                Token::Str("Basic Zm9vOmJhcg=="),
                Token::TupleEnd,
                Token::Tuple { len: 2 },
                Token::Str("host"),
                Token::Str("example.com"),
                Token::TupleEnd,
                Token::SeqEnd,
                Token::Str("max_retries"),
                Token::U32(3),
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_local_source_settings() {
        let path = PathBuf::from("tests/resources/base.csv");
        let local = SourceSetting::Csv { path: path.clone() };

        assert_tokens(&path, &[Token::Str("tests/resources/base.csv")]);

        assert_tokens(
            &local,
            &[
                Token::Struct { name: "SourceSetting", len: 2 },
                Token::Str("type"),
                Token::Str("csv"),
                Token::Str("path"),
                Token::Str("tests/resources/base.csv"),
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_rest_api_source_setting() {
        let cluster = SourceSetting::RestApi(HttpQuery {
            interval: Duration::from_secs(37),
            method: Method::POST,
            url: Url::parse(
                "https://httpbin.org/post?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z",
            )
            .unwrap(),
            headers: vec![],
            max_retries: 3,
        });

        assert_tokens(
            &cluster,
            &[
                Token::Struct { name: "HttpQuery", len: 5 },
                Token::Str("type"),
                Token::Str("rest_api"),
                Token::Str("interval_secs"),
                Token::U64(37),
                Token::Str("method"),
                Token::Str("POST"),
                Token::Str("url"),
                Token::Str(
                    "https://httpbin.org/post?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z",
                ),
                Token::Str("max_retries"),
                Token::U32(3),
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_settings() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Basic Zm9vOmJhcg==".parse().unwrap());
        headers.insert(header::HOST, "example.com".parse().unwrap());
        let _headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
            .collect();

        let settings = maplit::btreemap! {
            "httpbin".to_string() => SourceSetting::RestApi(HttpQuery {
                interval: Duration::from_secs(10),
                method: Method::HEAD,
                url: Url::parse("https://httpbin.org/head?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z").unwrap(),
                headers: vec![
                    ("authorization".to_string(), "Basic Zm9vOmJhcg==".to_string()),
                    ("host".to_string(), "example.com".to_string()),
                ],
                max_retries: 3,
            }),
            "local".to_string() => SourceSetting::Csv{path: PathBuf::from("examples/data/eligibility.csv")},
        };

        assert_tokens(
            &settings,
            &[
                Token::Map { len: Some(2) },
                Token::Str("httpbin"),
                Token::Struct { name: "HttpQuery", len: 6 },
                Token::Str("type"),
                Token::Str("rest_api"),
                Token::Str("interval_secs"),
                Token::U64(10),
                Token::Str("method"),
                Token::Str("HEAD"),
                Token::Str("url"),
                Token::Str(
                    "https://httpbin.org/head?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z",
                ),
                Token::Str("headers"),
                Token::Seq { len: Some(2) },
                Token::Tuple { len: 2 },
                Token::Str("authorization"),
                Token::Str("Basic Zm9vOmJhcg=="),
                Token::TupleEnd,
                Token::Tuple { len: 2 },
                Token::Str("host"),
                Token::Str("example.com"),
                Token::TupleEnd,
                Token::SeqEnd,
                Token::Str("max_retries"),
                Token::U32(3),
                Token::StructEnd,
                // "local" => Csv
                Token::Str("local"),
                Token::Struct { name: "SourceSetting", len: 2 },
                Token::Str("type"),
                Token::Str("csv"),
                Token::Str("path"),
                Token::Str("examples/data/eligibility.csv"),
                Token::StructEnd,
                Token::MapEnd,
            ],
        )
    }
}
