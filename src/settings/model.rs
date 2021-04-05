use crate::error::ConfigError;
use crate::ProctorResult;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Method, Url,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settings {
    pub sources: BTreeMap<String, SourceSetting>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceSetting {
    RestApi(HttpQuery),
    Csv { path: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpQuery {
    #[serde(
        rename = "interval_secs",
        serialize_with = "crate::serde::serialize_duration_secs",
        deserialize_with = "crate::serde::deserialize_duration_secs"
    )]
    pub interval: Duration,
    #[serde(
        serialize_with = "crate::serde::serialize_to_str",
        deserialize_with = "crate::serde::deserialize_from_str"
    )]
    pub method: Method,
    #[serde(
        serialize_with = "crate::serde::serialize_to_str",
        deserialize_with = "crate::serde::deserialize_from_str"
    )]
    pub url: Url,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
}

impl HttpQuery {
    pub fn header_map(&self) -> ProctorResult<HeaderMap> {
        let mut map = HeaderMap::with_capacity(self.headers.len());
        for (k, v) in self.headers.iter() {
            let name = HeaderName::from_str(k.as_str()).map_err::<ConfigError, _>(|err| err.into());
            let value = HeaderValue::from_str(v.as_str()).map_err::<ConfigError, _>(|err| err.into());
            map.insert(name?, value?);
        }
        Ok(map)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EligibilitySettings {
    pub policy_path: PathBuf,
}

impl crate::elements::PolicySettings for EligibilitySettings {
    fn specification_path(&self) -> PathBuf {
        self.policy_path.clone()
    }
}

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// pub struct Settings {
//     pub eligibility: EligibilitySettings,
// }
//
// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// pub struct GatherSettings {
//     pub strategy: GatherStrategy,
// }
//
// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// pub enum GatherStrategy {
//     Local { path: PathBuf, keys: Vec<String> },
//     Distributed(HttpEndpoint),
// }

//// serialize_with = "serialize_header_to_str",
//// deserialize_with = "deserialize_header_from_str"
// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// pub struct HttpEndpoint {
//     #[serde(serialize_with = "serialize_to_str", deserialize_with = "deserialize_from_str")]
//     pub method: Method,
//     #[serde(serialize_with = "serialize_to_str", deserialize_with = "deserialize_from_str")]
//     pub url: Url,
//     #[serde(default)]
//     headers: Vec<(String, String)>,
// }
//
// impl HttpEndpoint {
//     pub fn headers(&self) -> SpringlineResult<HeaderMap> {
//         let mut map = HeaderMap::with_capacity(self.headers.len());
//         for (k, v) in self.headers.iter() {
//             let name = HeaderName::from_str(k.as_str()).map_err::<ConfigError, _>(|err| err.into());
//             let value = HeaderValue::from_str(v.as_str()).map_err::<ConfigError, _>(|err| err.into());
//             map.insert(name?, value?);
//         }
//         Ok(map)
//     }
// }

// #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
// #[serde(remote = "HeaderName")]
// struct HeaderNameDef {
//     #[serde(
//     serialize_with = "serialize_to_str",
//     deserialize_with = "deserialize_from_str"
//     )]
//     name: String,
// }

// impl From<HeaderNameDef> for HeaderName {
//     fn from(that: HeaderNameDef) -> Self {
//         HeaderName::from_str(that.name.as_str()).expect("invalid header name")
//     }
// }

// #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
// #[serde(remote = "HeaderValue")]
// struct HeaderValueDef {
//     #[serde(
//     serialize_with = "serialize_to_str",
//     deserialize_with = "deserialize_from_str"
//     )]
//     value: String,
// }

// impl From<HeaderValueDef> for HeaderValue {
//     fn from(that: HeaderValueDef) -> Self {
//         HeaderValue::from_str(that.value.as_str()).expect("invalid header value")
//     }
// }

// #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
//// #[serde(remote = "HeaderMap")]
// struct HeaderMapDef {
//     headers: Vec<(HeaderNameDef, HeaderValueDef)>,
// }

// impl From<HeaderMapDef> for HeaderMap {
//     fn from(that: HeaderMapDef) -> Self {
//         let mut map = HeaderMap::with_capacity(that.headers.len());
//         for ((k,v)) in that.headers {
//             map.insert(k.into(), v.into());
//         }
//         map
//     }
// }

// fn serialize_header_to_str<S>(that: &Option<HeaderMap>, serializer: S) -> Result<S::Ok, S::Error>
// where
//     S: Serializer,
// {
//     match that {
//         None => serializer.serialize_none(),
//         Some(that) => {
//             let mut headers = std::collections::BTreeMap::new();
//             for (k,v) in that {
//                 headers.insert(
//                     k.as_str(),
//                     v.to_str().map_err(|err| serde::ser::Error::custom(err.to_string()))?
//                 );
//             }
//
//             serializer.serialize_some(&headers)
//         }
//     }
//     let mut map = serializer.serialize_map(Some(that.len()))?;
// for (k,v) in that {
//     let k_str = k.as_str();
//     let v_str = v.to_str().map_err(|err| serde::ser::Error::custom(err.to_string()))?;
//     map.serialize_entry(k_str, v_str)?;
// }
// map.end()
// }

// use std::marker::PhantomData;
// use serde::de::{Visitor, MapAccess};
//
// struct MyHeaderMapVisitor {
//     marker: PhantomData<fn() -> HeaderMap>
// }
//
// impl MyHeaderMapVisitor {
//     fn new() -> Self {
//         MuHeaderMapVisitor {
//             marker: PhantomData,
//         }
//     }
// }
//
// impl<'de> Visitor<'de> for MyHeaderMapVisitor {
//     type Value = HeaderMap;
//
//     fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//         formatter.write_str("HTTP Headers")
//     }
//
//     fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
//     where
//         M: MapAccess<'de>,
//     {
//         let mut map = HeaderMap::with_capacity(access.size_hint().unwrap_or(0));
//
//         while let Some((k,v)) = access.next_entry()? {
//             let key = HeaderName::from_str(k)?;
//             let value = HeaderValue::from_str(v)?;
//             map.insert(key, value);
//         }
//
//         Ok(map)
//     }
//
//     impl<'de> Deserialize<'de> for MyH
// }

// fn deserialize_header_from_str<'de, D>(deserializer: D) -> Result<Option<HeaderMap>, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     struct OptionalHeaderMapVisitor;
//     impl<'de> de::Visitor<'de> for OptionalHeaderMapVisitor {
//         type Value = Option<HeaderMap>;
//
//         fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//             write!(formatter, "null or a HeaderMap")
//         }
//
//         fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> { Ok(None) }
//
//         fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
//         where
//             D: de::Deserializer<'de>,
//         {
//
//             Ok(Some(
//
//             ))
//         }
//     }
//
//     deserializer.deserialize_option(OptionalHeaderMapVisitor)
//
// }

// impl Serialize for HeaderMap {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         let mut map = serializer.serialize_map(Some(self.len()))?;
//         for (k, v) in self {
//             let k_str = k.as_str();
//             let v_str = v.to_str().map_err(|err| serde::ser::Error::custom(err.to_string()))?;
//             map.serialize_entry(k_str, v_str)?;
//         }
//         map.end()
//     }
// }
// /////////////////////////////////////////////////////
// // Unit Tests ///////////////////////////////////////

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
        let header_vec = header_map.iter().map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string())).collect();

        let endpoint = HttpQuery {
            interval: Duration::from_secs(33),
            method: Method::GET,
            url: Url::parse("https://httpbin.org/get?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z").unwrap(),
            headers: header_vec,
        };

        assert_tokens(
            &endpoint,
            &[
                Token::Struct { name: "HttpQuery", len: 4 },
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
                Token::StructEnd,
            ],
        );
    }

    // #[test]
    // fn test_serde_eligibility_settings() {
    //     let eligibility = EligibilitySettings {
    //         task_status: GatherSettings {
    //             strategy: GatherStrategy::Local {
    //                 path: PathBuf::from("foo/bar.csv"),
    //                 keys: vec![],
    //             },
    //         },
    //         cluster_status: GatherSettings {
    //             strategy: GatherStrategy::Distributed(HttpEndpoint {
    //                 method: Method::HEAD,
    //                 url: Url::parse("https://httpbin.org/head").unwrap(),
    //                 headers: vec![],
    //             }),
    //         },
    //     };
    //
    //     assert_tokens(
    //         &eligibility,
    //         &[
    //             Token::Struct {
    //                 name: "EligibilitySettings",
    //                 len: 2,
    //             },
    //             Token::Str("task_status"),
    //             Token::Struct {
    //                 name: "GatherSettings",
    //                 len: 1,
    //             },
    //             Token::Str("strategy"),
    //             Token::Enum { name: "GatherStrategy" },
    //             Token::Str("Local"),
    //             Token::Map { len: Some(2) },
    //             Token::Str("path"),
    //             Token::Str("foo/bar.csv"),
    //             Token::Str("keys"),
    //             Token::Seq { len: Some(0) },
    //             Token::SeqEnd,
    //             Token::MapEnd,
    //             Token::StructEnd,
    //             Token::Str("cluster_status"),
    //             Token::Struct {
    //                 name: "GatherSettings",
    //                 len: 1,
    //             },
    //             Token::Str("strategy"),
    //             Token::Enum { name: "GatherStrategy" },
    //             Token::Str("Distributed"),
    //             Token::Struct {
    //                 name: "HttpEndpoint",
    //                 len: 3,
    //             },
    //             Token::Str("method"),
    //             Token::Str("HEAD"),
    //             Token::Str("url"),
    //             Token::Str("https://httpbin.org/head"),
    //             Token::Str("headers"),
    //             Token::Seq { len: Some(0) },
    //             Token::SeqEnd,
    //             Token::StructEnd,
    //             Token::StructEnd,
    //             Token::StructEnd,
    //         ],
    //     )
    // }
    //
    // #[test]
    // fn test_serde_gather_settings() {
    //     let gather = GatherSettings {
    //         strategy: GatherStrategy::Local {
    //             path: PathBuf::from("tests/resources/base.csv"),
    //             keys: vec!["is_redeploying".to_string(), "last_deployment".to_string()],
    //         },
    //     };
    //
    //     assert_tokens(
    //         &gather,
    //         &[
    //             Token::Struct {
    //                 name: "GatherSettings",
    //                 len: 1,
    //             },
    //             Token::Str("strategy"),
    //             Token::Enum { name: "GatherStrategy" },
    //             Token::Str("Local"),
    //             Token::Map { len: Some(2) },
    //             Token::Str("path"),
    //             Token::Str("tests/resources/base.csv"),
    //             Token::Str("keys"),
    //             Token::Seq { len: Some(2) },
    //             Token::Str("is_redeploying"),
    //             Token::Str("last_deployment"),
    //             Token::SeqEnd,
    //             Token::MapEnd,
    //             Token::StructEnd,
    //         ],
    //     )
    // }
    //
    #[test]
    fn test_serde_local_source_settings() {
        let path = PathBuf::from("tests/resources/base.csv");
        let local = SourceSetting::Csv { path: path.clone() };

        assert_tokens(&path, &[Token::Str("tests/resources/base.csv")]);

        assert_tokens(
            &local,
            &[
                Token::Struct {
                    name: "SourceSetting",
                    len: 2,
                },
                Token::Str("type"),
                Token::Str("Csv"),
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
            url: Url::parse("https://httpbin.org/post?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z").unwrap(),
            headers: vec![],
        });

        assert_tokens(
            &cluster,
            &[
                Token::Struct { name: "HttpQuery", len: 5 },
                Token::Str("type"),
                Token::Str("RestApi"),
                Token::Str("interval_secs"),
                Token::U64(37),
                Token::Str("method"),
                Token::Str("POST"),
                Token::Str("url"),
                Token::Str("https://httpbin.org/post?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z"),
                Token::Str("headers"),
                Token::Seq { len: Some(0) },
                Token::SeqEnd,
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_settings() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Basic Zm9vOmJhcg==".parse().unwrap());
        headers.insert(header::HOST, "example.com".parse().unwrap());
        let _headers: Vec<(String, String)> = headers.iter().map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string())).collect();

        let settings = Settings {
            sources: maplit::btreemap! {
                "httpbin".to_string() => SourceSetting::RestApi(HttpQuery {
                    interval: Duration::from_secs(10),
                    method: Method::HEAD,
                    url: Url::parse("https://httpbin.org/head?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z").unwrap(),
                    headers: vec![
                        ("authorization".to_string(), "Basic Zm9vOmJhcg==".to_string()),
                        ("host".to_string(), "example.com".to_string()),
                    ],
                }),
                "local".to_string() => SourceSetting::Csv{path: PathBuf::from("examples/data/eligibility.csv")},
            },
        };

        assert_tokens(
            &settings,
            &[
                Token::Struct { name: "Settings", len: 1 },
                Token::Str("sources"),
                Token::Map { len: Some(2) },
                // "httpbin" => RestApi
                Token::Str("httpbin"),
                Token::Struct { name: "HttpQuery", len: 5 },
                Token::Str("type"),
                Token::Str("RestApi"),
                Token::Str("interval_secs"),
                Token::U64(10),
                Token::Str("method"),
                Token::Str("HEAD"),
                Token::Str("url"),
                Token::Str("https://httpbin.org/head?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z"),
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
                Token::StructEnd,
                // "local" => Csv
                Token::Str("local"),
                Token::Struct {
                    name: "SourceSetting",
                    len: 2,
                },
                Token::Str("type"),
                Token::Str("Csv"),
                Token::Str("path"),
                Token::Str("examples/data/eligibility.csv"),
                Token::StructEnd,
                Token::MapEnd,
                Token::StructEnd,
                // Token::Enum { name: "SourceSetting" },
                // Token::Str("RestApi"),
                // Token::Struct { name: "HttpQuery", len: 4 },
                // Token::Str("interval_secs"),
                // Token::U64(37),
                // Token::Str("method"),
                // Token::Str("HEAD"),
                // Token::Str("url"),
                // Token::Str("https://httpbin.org/get?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z"),
                // Token::Str("headers"),
                // Token::Seq { len: Some(2) },
                // Token::Tuple { len: 2 },
                // Token::Str("authorization"),
                // Token::Str("Basic Zm9vOmJhcg=="),
                // Token::TupleEnd,
                // Token::Tuple { len: 2 },
                // Token::Str("host"),
                // Token::Str("example.com"),
                // Token::TupleEnd,
                // Token::SeqEnd,
                // Token::StructEnd,

                // Token::Enum { name: "SourceSetting" },
                // Token::Str("Local"),
                // Token::Str("examples/data/eligibility.csv"),
                // Token::MapEnd,
                // Token::StructEnd,
                // Token::Struct {
                //     name: "EligibilitySettings",
                //     len: 2,
                // },
                // Token::Str("task_status"),
                // Token::Struct {
                //     name: "GatherSettings",
                //     len: 1,
                // },
                // Token::Str("strategy"),
                // Token::Enum { name: "GatherStrategy" },
                // Token::Str("Local"),
                // Token::Map { len: Some(2) },
                // Token::Str("path"),
                // Token::Str("foo/bar.csv"),
                // Token::Str("keys"),
                // Token::Seq { len: Some(0) },
                // Token::SeqEnd,
                // Token::MapEnd,
                // Token::StructEnd,
                // Token::Str("cluster_status"),
                // Token::Struct {
                //     name: "GatherSettings",
                //     len: 1,
                // },
                // Token::Str("strategy"),
                // Token::Enum { name: "GatherStrategy" },
                // Token::Str("Distributed"),
                // Token::Struct {
                //     name: "HttpEndpoint",
                //     len: 3,
                // },
                // Token::Str("method"),
                // Token::Str("HEAD"),
                // Token::Str("url"),
                // Token::Str("https://httpbin.org/head"),
                // Token::Str("headers"),
                // Token::Seq { len: Some(2) },
                // Token::Tuple { len: 2 },
                // Token::Str("authorization"),
                // Token::Str("Basic Zm9vOmJhcg=="),
                // Token::TupleEnd,
                // Token::Tuple { len: 2 },
                // Token::Str("host"),
                // Token::Str("example.com"),
                // Token::TupleEnd,
                // Token::SeqEnd,
                // Token::StructEnd,
                // Token::StructEnd,
                // Token::StructEnd,
                // Token::StructEnd,
            ],
        )
    }

    // #[test]
    // fn test_serde_http_endpoint() {
    //     let mut headers = HeaderMap::new();
    //     headers.insert(header::AUTHORIZATION, "Basic Zm9vOmJhcg==".parse().unwrap());
    //     headers.insert(header::HOST, "example.com".parse().unwrap());
    //     let headers = headers.iter().map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string())).collect();
    //
    //     let endpoint = HttpEndpoint {
    //         method: Method::GET,
    //         url: Url::parse("https://httpbin.org/get?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z").unwrap(),
    //         headers,
    //     };
    //
    //     assert_tokens(
    //         &endpoint,
    //         &[
    //             Token::Struct {
    //                 name: "HttpEndpoint",
    //                 len: 3,
    //             },
    //             Token::Str("method"),
    //             Token::Str("GET"),
    //             Token::Str("url"),
    //             Token::Str("https://httpbin.org/get?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z"),
    //             Token::Str("headers"),
    //             Token::Seq { len: Some(2) },
    //             Token::Tuple { len: 2 },
    //             Token::Str("authorization"),
    //             Token::Str("Basic Zm9vOmJhcg=="),
    //             Token::TupleEnd,
    //             Token::Tuple { len: 2 },
    //             Token::Str("host"),
    //             Token::Str("example.com"),
    //             Token::TupleEnd,
    //             Token::SeqEnd,
    //             Token::StructEnd,
    //         ],
    //     );
    // }
}
