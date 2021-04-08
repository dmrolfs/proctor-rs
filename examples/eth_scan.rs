#[macro_use]
extern crate enum_display_derive;

use anyhow::anyhow;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use proctor::elements::{Collect, TelemetryData};
use proctor::graph::{stage, Connect, Graph, SinkShape, SourceShape};
use proctor::telemetry::{get_subscriber, init_subscriber};
use reqwest::Url;
use serde::de;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::time::Duration;

#[derive(Debug, Display, PartialEq)]
pub enum CurrencyCode {
    ETH,
    USD,
}

impl CurrencyCode {
    fn label(code: &CurrencyCode) -> String {
        format!("{}", code).to_lowercase()
    }
}

impl CurrencyCode {
    pub fn from_str<S>(code: S) -> Result<CurrencyCode, anyhow::Error>
    where
        S: AsRef<str>,
    {
        match code.as_ref() {
            "ETH" => Ok(CurrencyCode::ETH),
            "USD" => Ok(CurrencyCode::USD),
            c => Err(anyhow!("unrecognized currency code: {}", c)),
        }
    }
}

// for anything beyond an example, I'd consider using a crate such as `rusty_money`.
#[derive(Debug, PartialEq)]
pub struct Currency {
    pub code: CurrencyCode,
    pub name: String,
}

impl Currency {
    pub fn new<S>(code: CurrencyCode, name: S) -> Self
    where
        S: Into<String>,
    {
        Self { code, name: name.into() }
    }
}

#[derive(Debug, PartialEq)]
pub struct ExchangeRate {
    pub from_currency: Currency,
    pub to_currency: Currency,
    pub rate: f32,
    pub timestamp: DateTime<Utc>,
}

impl<'de> de::Deserialize<'de> for ExchangeRate {
    #[tracing::instrument(level = "trace", name = "deserialize into ExchangeRate", skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        #[derive(Debug)]
        enum Field {
            FromCurrencyCode,
            FromCurrencyName,
            ToCurrencyCode,
            ToCurrencyName,
            ExchangeRate,
            DateTime,
            TimeZone,
            Ignored,
        }

        impl<'de> de::Deserialize<'de> for Field {
            // #[tracing::instrument(level="trace", name="Field deserialize",skip(deserializer))]
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: de::Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> de::Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("exchange rate fields")
                    }

                    // #[tracing::instrument(level="trace", name="visit_str", skip(self))]
                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        let r = match value {
                            "1. From_Currency Code" => Ok(Field::FromCurrencyCode),
                            "2. From_Currency Name" => Ok(Field::FromCurrencyName),
                            "3. To_Currency Code" => Ok(Field::ToCurrencyCode),
                            "4. To_Currency Name" => Ok(Field::ToCurrencyName),
                            "5. Exchange Rate" => Ok(Field::ExchangeRate),
                            "6. Last Refreshed" => Ok(Field::DateTime),
                            "7. Time Zone" => Ok(Field::TimeZone),
                            _ => Ok(Field::Ignored),
                        };

                        // tracing::trace!(field=?r, %value, "matched field");
                        r
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct ExchangeRateVisitor;

        impl<'de> de::Visitor<'de> for ExchangeRateVisitor {
            type Value = ExchangeRate;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("exchange rate between two currencies")
            }

            // #[tracing::instrument(level="trace", name="Visitor visit_map",skip(self, map))]
            fn visit_map<V>(self, mut map: V) -> Result<ExchangeRate, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut from_currency_code: Option<String> = None;
                let mut from_currency_name = None;
                let mut to_currency_code: Option<String> = None;
                let mut to_currency_name = None;
                let mut rate: Option<&str> = None;
                let mut datetime = None;
                let mut timezone = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::FromCurrencyCode => {
                            if from_currency_code.is_some() {
                                return Err(de::Error::duplicate_field("from_currency_code"));
                            }
                            from_currency_code = Some(map.next_value()?);
                            // tracing::trace!(?from_currency_code, "element-str");
                        }
                        Field::FromCurrencyName => {
                            if from_currency_name.is_some() {
                                return Err(de::Error::duplicate_field("from_currency_name"));
                            }
                            from_currency_name = Some(map.next_value()?);
                            // tracing::trace!(?from_currency_name, "element");
                        }
                        Field::ToCurrencyCode => {
                            if to_currency_code.is_some() {
                                return Err(de::Error::duplicate_field("to_currency_code"));
                            }
                            to_currency_code = Some(map.next_value()?);
                            // tracing::trace!(?to_currency_code, "element-str");
                        }
                        Field::ToCurrencyName => {
                            if to_currency_name.is_some() {
                                return Err(de::Error::duplicate_field("to_currency_name"));
                            }
                            to_currency_name = Some(map.next_value()?);
                            // tracing::trace!(?to_currency_name, "element");
                        }
                        Field::ExchangeRate => {
                            if rate.is_some() {
                                return Err(de::Error::duplicate_field("rate"));
                            }
                            rate = Some(map.next_value()?);
                            // tracing::trace!(?rate, "element-str");
                        }
                        Field::DateTime => {
                            if datetime.is_some() {
                                return Err(de::Error::duplicate_field("datetime"));
                            }
                            datetime = Some(map.next_value()?);
                            // tracing::trace!(?datetime, "element-str");
                        }
                        Field::TimeZone => {
                            if timezone.is_some() {
                                return Err(de::Error::duplicate_field("timezone"));
                            }
                            timezone = Some(map.next_value()?);
                            // tracing::trace!(?timezone, "element-str");
                        }
                        Field::Ignored => {
                            let _ignored: &str = map.next_value()?;
                            // tracing::trace!("ignoring element-str");
                        }
                    }
                }

                let from_currency_code = from_currency_code.ok_or_else(|| de::Error::missing_field("from_currency_code"))?;
                let from_currency_name = from_currency_name.ok_or_else(|| de::Error::missing_field("from_currency_name"))?;
                let to_currency_code = to_currency_code.ok_or_else(|| de::Error::missing_field("to_currency_code"))?;
                let to_currency_name = to_currency_name.ok_or_else(|| de::Error::missing_field("to_currency_name"))?;
                let rate = rate.ok_or_else(|| de::Error::missing_field("exchange rate"))?;
                let datetime = datetime.ok_or_else(|| de::Error::missing_field("datetime"))?;
                let timezone = timezone.ok_or_else(|| de::Error::missing_field("tiemzone"))?;

                let ts = NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S")
                    .map_err(|err| de::Error::custom(format!("failed to parse datetime: {:?}", err)))?;
                let tz = match timezone {
                    "UTC" => Ok(Utc),
                    z => Err(anyhow!("unrecognized timezone: {}", z)),
                }
                .map_err(|err| de::Error::custom(format!("failed to parse timezone: {:?}", err)))?;

                Ok(ExchangeRate {
                    from_currency: Currency {
                        code: CurrencyCode::from_str(from_currency_code)
                            .map_err(|err| de::Error::custom(format!("failed to parse currency code: {}", err)))?,
                        name: from_currency_name,
                    },
                    to_currency: Currency {
                        code: CurrencyCode::from_str(to_currency_code)
                            .map_err(|err| de::Error::custom(format!("failed to parse currency code: {}", err)))?,
                        name: to_currency_name,
                    },
                    rate: rate
                        .parse::<f32>()
                        .map_err(|err| de::Error::custom(format!("failed to parse rate: {}", err)))?,
                    timestamp: DateTime::from_utc(ts, tz),
                })
            }
        }

        const FIELDS: &'static [&'static str] = &[
            "1. From_Currency Code",
            "2. From_Currency Name",
            "3. To_Currency Code",
            "4. To_Currency Name",
            "5. Exchange Rate",
            "6. Last Refreshed",
            "7. Time Zone",
        ];

        deserializer.deserialize_struct("Realtime Currency Exchange Rate", FIELDS, ExchangeRateVisitor)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = get_subscriber("eth_scan", "trace");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    test_exchange_rate_deser();

    let url = Url::parse_with_params(
        "https://alpha-vantage.p.rapidapi.com/query",
        &[("from_currency", "ETH"), ("to_currency", "USD"), ("function", "CURRENCY_EXCHANGE_RATE")],
    )
    .expect("failed to create valid ETH query URL.");

    let tick = stage::Tick::with_constraint(
        "tick",
        Duration::from_secs(10),
        Duration::from_secs(30),
        (),
        stage::tick::Constraint::by_count(3),
        // stage::tick::Constraint::by_time(Duration::from_secs(120)),
    );

    let mut default_headers = reqwest::header::HeaderMap::new();
    default_headers.insert("x-rapidapi-key", "fe37af1e07mshd1763d86e5f2a8cp1714cfjsnb6145a35e7ca".parse()?);

    let to_metric_group = |base: HashMap<String, ExchangeRate>| {
        let (_, rate) = base.into_iter().next().unwrap();
        let mut data = HashMap::new();
        data.insert(
            format!(
                "{}.to.{}",
                CurrencyCode::label(&rate.from_currency.code),
                CurrencyCode::label(&rate.to_currency.code)
            ),
            rate.rate.to_string(),
        );
        // data.insert("currency".to_string(), ex.from_currency.name);
        // data.insert("value".to_string(), ex.rate.to_string());
        TelemetryData::from_data(data)
    };

    let collect = Collect::new("collect", url, default_headers, to_metric_group).await;

    let sink = stage::LoggedSink::new("log");

    (tick.outlet(), collect.inlet()).connect().await;
    (collect.outlet(), sink.inlet()).connect().await;

    tracing::warn!("registering graph...");
    let mut g = Graph::default();
    g.push_back(Box::new(tick)).await;
    g.push_back(Box::new(collect)).await;
    g.push_back(Box::new(sink)).await;

    tracing::warn!("running graph...");
    g.run().await?;

    // tracing::warn!("sleeping...");
    // tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    // tracing::warn!("... waking");
    tracing::warn!("completing graph...");
    // g.complete().await.expect("failed to close graph.");
    Ok(())
}

#[tracing::instrument(level = "info", name = "test deser")]
fn test_exchange_rate_deser() {
    use chrono::Utc;

    let json = r#"{
"Realtime Currency Exchange Rate": {
"1. From_Currency Code": "ETH",
"2. From_Currency Name": "Ethereum",
"3. To_Currency Code": "USD",
"4. To_Currency Name": "United States Dollar",
"5. Exchange Rate": "1858.11000000",
"6. Last Refreshed": "2021-02-13 01:57:08",
"7. Time Zone": "UTC",
"8. Bid Price": "1858.10000000",
"9. Ask Price": "1858.11000000"
}
}"#;

    let doc = serde_json::from_str::<HashMap<String, ExchangeRate>>(json).unwrap();
    let actual = doc.values().next().unwrap();
    tracing::trace!(?actual, "json deser result");
    assert_eq!(
        actual,
        &ExchangeRate {
            from_currency: Currency {
                code: CurrencyCode::ETH,
                name: "Ethereum".to_string(),
            },
            to_currency: Currency {
                code: CurrencyCode::USD,
                name: "United States Dollar".to_string(),
            },
            rate: 1858.11_f32,
            timestamp: Utc.ymd(2021, 2, 13).and_hms(1, 57, 8),
        }
    )
}
