use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

use serde::de::{self, DeserializeOwned, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::PolicySource;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PolicySettings<T>
where
    T: Debug + Serialize + DeserializeOwned,
{
    pub required_subscription_fields: HashSet<String>,
    pub optional_subscription_fields: HashSet<String>,
    pub policies: Vec<PolicySource>,
    pub template_data: Option<T>,
}

impl<T> PolicySettings<T>
where
    T: Debug + Serialize + DeserializeOwned,
{
    pub fn new(required_fields: HashSet<String>, optional_fields: HashSet<String>) -> Self {
        Self {
            required_subscription_fields: required_fields,
            optional_subscription_fields: optional_fields,
            policies: vec![],
            template_data: None,
        }
    }

    pub fn with_source(mut self, source: PolicySource) -> Self {
        self.policies.push(source);
        self
    }

    pub fn with_template_data(mut self, data: T) -> Self {
        self.template_data = Some(data);
        self
    }
}

const REQ_SUBSCRIPTION_FIELDS: &'static str = "required_subscription_fields";
const OPT_SUBSCRIPTION_FIELDS: &'static str = "optional_subscription_fields";
const POLICIES: &'static str = "policies";
const TEMPLATE_DATA: &'static str = "template_data";

impl<T: Debug + Serialize + DeserializeOwned> Serialize for PolicySettings<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut length = 0;
        if !self.required_subscription_fields.is_empty() {
            length += 1;
        }
        if !self.optional_subscription_fields.is_empty() {
            length += 1;
        }
        if !self.policies.is_empty() {
            length += 1;
        }
        if self.template_data.is_some() {
            length += 1;
        }

        let mut state = serializer.serialize_struct("PolicySettings", length)?;

        if !self.required_subscription_fields.is_empty() {
            state.serialize_field(REQ_SUBSCRIPTION_FIELDS, &self.required_subscription_fields)?;
        }

        if !self.optional_subscription_fields.is_empty() {
            state.serialize_field(OPT_SUBSCRIPTION_FIELDS, &self.optional_subscription_fields)?;
        }

        if !self.policies.is_empty() {
            state.serialize_field(POLICIES, &self.policies)?;
        }

        if let Some(ref data) = self.template_data {
            state.serialize_field(TEMPLATE_DATA, &data)?;
        }

        state.end()
    }
}

impl<'de, T> Deserialize<'de> for PolicySettings<T>
where
    T: Debug + Serialize + DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            ReqSubscriptionFields,
            OptSubscriptionFields,
            Policies,
            TemplateData,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("PolicySettings fields")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            REQ_SUBSCRIPTION_FIELDS => Ok(Field::ReqSubscriptionFields),
                            OPT_SUBSCRIPTION_FIELDS => Ok(Field::OptSubscriptionFields),
                            POLICIES => Ok(Field::Policies),
                            TEMPLATE_DATA => Ok(Field::TemplateData),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct PolicySettingsVisitor<T0> {
            marker: PhantomData<T0>,
        }

        impl<'de, T0> Visitor<'de> for PolicySettingsVisitor<T0>
        where
            T0: Debug + Serialize + DeserializeOwned,
        {
            type Value = PolicySettings<T0>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let t_name = std::any::type_name::<T0>();
                f.write_str(format!("struct PolicySettings<{}>", t_name).as_str())
            }

            fn visit_map<V>(self, mut map: V) -> Result<PolicySettings<T0>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut required_subscription_fields = HashSet::default();
                let mut optional_subscription_fields = HashSet::default();
                let mut policies = vec![];
                let mut template_data = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::ReqSubscriptionFields => {
                            if !required_subscription_fields.is_empty() {
                                return Err(de::Error::duplicate_field(REQ_SUBSCRIPTION_FIELDS));
                            }
                            required_subscription_fields = map.next_value()?;
                        },
                        Field::OptSubscriptionFields => {
                            if !optional_subscription_fields.is_empty() {
                                return Err(de::Error::duplicate_field(OPT_SUBSCRIPTION_FIELDS));
                            }
                            optional_subscription_fields = map.next_value()?;
                        },
                        Field::Policies => {
                            if !policies.is_empty() {
                                return Err(de::Error::duplicate_field(POLICIES));
                            }
                            policies = map.next_value()?;
                        },
                        Field::TemplateData => {
                            if template_data.is_some() {
                                return Err(de::Error::duplicate_field(TEMPLATE_DATA));
                            }
                            template_data = Some(map.next_value()?);
                        },
                    }
                }
                Ok(PolicySettings {
                    required_subscription_fields,
                    optional_subscription_fields,
                    policies,
                    template_data,
                })
            }
        }

        const FIELDS: &'static [&'static str] = &[
            REQ_SUBSCRIPTION_FIELDS,
            OPT_SUBSCRIPTION_FIELDS,
            POLICIES,
            TEMPLATE_DATA,
        ];

        deserializer.deserialize_struct("PolicySettings", FIELDS, PolicySettingsVisitor { marker: PhantomData })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use claim::*;
    use pretty_assertions::assert_eq;
    use ron::ser::PrettyConfig;
    use serde_test::{assert_tokens, Token};
    use trim_margin::MarginTrimmable;

    use super::*;

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(default)]
    pub struct TestTemplateData {
        pub basis: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub max_healthy_lag: Option<f64>,
        pub min_healthy_lag: f64,
        #[serde(flatten, skip_serializing_if = "BTreeMap::is_empty")]
        pub custom: BTreeMap<String, String>,
    }

    #[test]
    fn test_test_data_serde() {
        let data = TestTemplateData {
            basis: "eligibility_basis".to_string(),
            max_healthy_lag: Some(133_f64),
            min_healthy_lag: 0_f64,
            custom: maplit::btreemap! {
                // "foo".to_string() => "bar".to_string(),
            },
            ..TestTemplateData::default()
        };

        assert_tokens(
            &data,
            &[
                Token::Map { len: None },
                Token::Str("basis"),
                Token::Str("eligibility_basis"),
                Token::Str("max_healthy_lag"),
                Token::Some,
                Token::F64(133.0),
                Token::Str("min_healthy_lag"),
                Token::F64(0.0),
                // Token::Str("custom"),
                // Token::Map { len: Some(1), },
                // Token::Str("foo"),
                // Token::Str("bar"),
                // Token::MapEnd,
                Token::MapEnd,
            ],
        )
    }

    #[test]
    fn test_policy_settings_serde() {
        let settings = PolicySettings {
            policies: vec![assert_ok!(PolicySource::from_template_file(
                "../resources/eligibility.polar"
            ))],
            template_data: Some(TestTemplateData {
                basis: "eligibility_basis".to_string(),
                max_healthy_lag: Some(133_f64),
                min_healthy_lag: 0_f64,
                custom: maplit::btreemap! {
                    "foo".to_string() => "bar".to_string(),
                },
                ..TestTemplateData::default()
            }),
            ..PolicySettings::default()
        };

        assert_tokens(
            &settings,
            &[
                Token::Struct { name: "PolicySettings", len: 2 },
                Token::Str("policies"),
                Token::Seq { len: Some(1) },
                Token::Struct { name: "PolicySource", len: 2 },
                Token::Str("source"),
                Token::Str("file"),
                Token::Str("policy"),
                Token::Struct { name: "file", len: 2 },
                Token::Str("path"),
                Token::Str("../resources/eligibility.polar"),
                Token::Str("is_template"),
                Token::Bool(true),
                Token::StructEnd,
                Token::StructEnd,
                Token::SeqEnd,
                Token::Str("template_data"),
                Token::Map { len: None },
                Token::Str("basis"),
                Token::Str("eligibility_basis"),
                Token::Str("max_healthy_lag"),
                Token::Some,
                Token::F64(133.0),
                Token::Str("min_healthy_lag"),
                Token::F64(0.0),
                Token::Str("foo"),
                Token::Str("bar"),
                Token::MapEnd,
                Token::StructEnd,
            ],
        )
    }

    #[test]
    fn test_policy_settings_ron_serde() {
        let expected = PolicySettings {
            policies: vec![assert_ok!(PolicySource::from_template_file(
                "../resources/eligibility.polar"
            ))],
            template_data: Some(TestTemplateData {
                basis: "eligibility_basis".to_string(),
                max_healthy_lag: Some(133_f64),
                min_healthy_lag: 0_f64,
                custom: maplit::btreemap! {
                    "foo".to_string() => "bar".to_string(),
                },
                ..TestTemplateData::default()
            }),
            ..PolicySettings::default()
        };

        let rep = assert_ok!(ron::ser::to_string_pretty(&expected, PrettyConfig::default()));
        let mut ron_deser = assert_ok!(ron::Deserializer::from_str(&rep));

        //DMR: Ron gets confused across a full serde-deser round trip (which is only used in
        // testing or tooling. So, since I really like the flatten approach to custom props, I
        // am working around the issue in test/tooling with transcoding Obj->Ron->Json->Obj.
        // longer term Dhal looks interesting.
        let mut json_rep = vec![];
        let mut json_ser = serde_json::Serializer::pretty(json_rep);
        let _ = assert_ok!(serde_transcode::transcode(&mut ron_deser, &mut json_ser));
        let json_rep = assert_ok!(String::from_utf8(json_ser.into_inner()));
        let expected_rep = r##"|{
        |  "policies": [
        |    {
        |      "source": "file",
        |      "policy": {
        |        "path": "../resources/eligibility.polar",
        |        "is_template": true
        |      }
        |    }
        |  ],
        |  "template_data": {
        |    "basis": "eligibility_basis",
        |    "max_healthy_lag": 133,
        |    "min_healthy_lag": 0,
        |    "foo": "bar"
        |  }
        |}"##
            .trim_margin_with("|")
            .unwrap();
        assert_eq!(json_rep, expected_rep);

        let actual: PolicySettings<TestTemplateData> = assert_ok!(serde_json::from_str(&json_rep));
        assert_eq!(actual, expected)
    }
}
