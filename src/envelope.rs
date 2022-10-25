use crate::elements::telemetry::TableType;
use crate::phases::sense::CorrelationGenerator;
use crate::AppData;
use crate::{Correlation, ProctorContext, ReceivedAt, Timestamp};
use async_trait::async_trait;
use frunk::{Monoid, Semigroup};
use once_cell::sync::Lazy;
use pretty_snowflake::{AlphabetCodec, Id, IdPrettifier, Label, MachineNode};
use serde::ser::SerializeStruct;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use strum_macros::AsRefStr;

// Since I expect the generator to be set once, at the application start I'm favoring `std::sync::RwLock`
// over tokio::sync::RwLock. This enables the Envelope API to be used in non async circumstances.
static ID_GENERATOR: Lazy<std::sync::RwLock<Option<CorrelationGenerator>>> = Lazy::new(|| std::sync::RwLock::new(None));

pub fn set_correlation_generator(gen: CorrelationGenerator) {
    let mut correlation_generator = ID_GENERATOR.write().unwrap();
    *correlation_generator = Some(gen);
}

fn gen_correlation_id<T: Label>() -> Id<T> {
    let guard = ID_GENERATOR.read().unwrap();
    let id = match &*guard {
        Some(g) => g.next_id(),
        None => {
            drop(guard);
            let g = CorrelationGenerator::distributed(MachineNode::default(), IdPrettifier::<AlphabetCodec>::default());
            let id = g.next_id();
            set_correlation_generator(g);
            id
        },
    };

    id.relabel()
}

pub trait IntoEnvelope {
    type Content: AppData + Label;

    fn into_envelope(self) -> Envelope<Self::Content>;
    fn metadata(&self) -> MetaData<Self::Content>;
}

/// A metadata wrapper for a data set
#[derive(Clone)]
pub struct Envelope<T>
where
    T: Label,
{
    metadata: MetaData<T>,
    content: T,
}

impl<T> fmt::Debug for Envelope<T>
where
    T: fmt::Debug + Label + Send,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format!("[{}]{{ {:?} }}", self.metadata, self.content))
    }
}

impl<T> fmt::Display for Envelope<T>
where
    T: fmt::Display + Label + Send,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]({})", self.metadata, self.content)
    }
}

impl<T> IntoEnvelope for Envelope<T>
where
    T: AppData + Label,
{
    type Content = T;

    fn into_envelope(self) -> Envelope<Self::Content> {
        self
    }

    fn metadata(&self) -> MetaData<Self::Content> {
        self.metadata.clone()
    }
}

impl<T> Envelope<T>
where
    T: Label + Send,
{
    /// Create a new enveloped data.
    pub fn new(content: T) -> Self {
        Self { metadata: MetaData::default(), content }
    }

    /// Get a reference to the sensor data metadata.
    pub const fn metadata(&self) -> &MetaData<T> {
        &self.metadata
    }

    /// Consumes self, returning the data item
    #[allow(clippy::missing_const_for_fn)]
    #[inline]
    pub fn into_inner(self) -> T {
        self.content
    }

    #[allow(clippy::missing_const_for_fn)]
    #[inline]
    pub fn into_parts(self) -> (MetaData<T>, T) {
        (self.metadata, self.content)
    }

    #[inline]
    pub const fn from_parts(metadata: MetaData<T>, content: T) -> Self {
        Self { metadata, content }
    }

    pub fn adopt_metadata<U>(&mut self, new_metadata: MetaData<U>) -> MetaData<T>
    where
        U: Label,
    {
        let old_metadata = self.metadata.clone();
        self.metadata = new_metadata.relabel();
        old_metadata
    }

    pub fn map<F, U>(self, f: F) -> Envelope<U>
    where
        U: Label + Send,
        F: FnOnce(T) -> U,
    {
        let metadata = self.metadata.clone().relabel();
        Envelope { metadata, content: f(self.content) }
    }

    pub fn flat_map<F, U>(self, f: F) -> Envelope<U>
    where
        U: Label + Send,
        F: FnOnce(Self) -> U,
    {
        let metadata = self.metadata.clone().relabel();
        Envelope { metadata, content: f(self) }
    }

    pub async fn and_then<Op, Fut, U>(self, f: Op) -> Envelope<U>
    where
        U: Label + Send,
        Fut: Future<Output = U> + Send,
        Op: FnOnce(T) -> Fut + Send,
    {
        let metadata = self.metadata.clone().relabel();
        Envelope { metadata, content: f(self.content).await }
    }
}

impl<T> std::ops::Deref for Envelope<T>
where
    T: Label,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.content
    }
}

impl<T> std::ops::DerefMut for Envelope<T>
where
    T: Label,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.content
    }
}

impl<T> AsRef<T> for Envelope<T>
where
    T: Label,
{
    fn as_ref(&self) -> &T {
        &self.content
    }
}

impl<T> AsMut<T> for Envelope<T>
where
    T: Label,
{
    fn as_mut(&mut self) -> &mut T {
        &mut self.content
    }
}

impl<T> PartialEq for Envelope<T>
where
    T: Label + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.content == other.content
    }
}

impl<T> PartialEq<T> for Envelope<T>
where
    T: Label + PartialEq,
{
    fn eq(&self, other: &T) -> bool {
        &self.content == other
    }
}

impl<T> Correlation for Envelope<T>
where
    T: Label + Sync,
{
    type Correlated = T;

    fn correlation(&self) -> &Id<Self::Correlated> {
        self.metadata.correlation()
    }
}

impl<T> ReceivedAt for Envelope<T>
where
    T: Label,
{
    fn recv_timestamp(&self) -> Timestamp {
        self.metadata.recv_timestamp()
    }
}

impl<T> Label for Envelope<T>
where
    T: Label,
{
    type Labeler = <T as Label>::Labeler;

    fn labeler() -> Self::Labeler {
        <T as Label>::labeler()
    }
}

impl<T> std::ops::Add for Envelope<T>
where
    T: std::ops::Add<Output = T> + Label + Send,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::from_parts(self.metadata + rhs.metadata, self.content + rhs.content)
    }
}

impl<T> Monoid for Envelope<T>
where
    T: Monoid + Label + Send,
{
    fn empty() -> Self {
        Self::from_parts(<MetaData<T> as Monoid>::empty(), <T as Monoid>::empty())
    }
}

impl<T> Semigroup for Envelope<T>
where
    T: Semigroup + Label + Send,
{
    fn combine(&self, other: &Self) -> Self {
        Self::from_parts(
            self.metadata().combine(other.metadata()),
            self.content.combine(&other.content),
        )
    }
}

impl<T> From<Envelope<T>> for crate::elements::Telemetry
where
    T: Label + Into<Self>,
{
    fn from(that: Envelope<T>) -> Self {
        that.content.into()
    }
}

impl<T> oso::ToPolar for Envelope<T>
where
    T: Label + oso::ToPolar,
{
    fn to_polar(self) -> oso::PolarValue {
        self.content.to_polar()
    }
}

impl<T> Envelope<Option<T>>
where
    T: Label + Send,
{
    /// Transposes an `Envelope` of an [`Option`] into an [`Option`] of `Envelope`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use proctor::Envelope;
    ///
    /// let x: Option<Envelope<i32>> = Some(Envelope::new(5));
    /// let y: Envelope<Option<i32>> = Envelope::new(Some(5));
    /// assert_eq!(x, y.transpose());
    /// ```
    #[inline]
    pub fn transpose(self) -> Option<Envelope<T>> {
        match self.content {
            Some(d) => Some(Envelope { content: d, metadata: self.metadata.relabel() }),
            None => None,
        }
    }
}

impl<T, E> Envelope<Result<T, E>>
where
    T: Label + Send,
{
    /// Transposes a `Envelope` of a [`Result`] into a [`Result`] of `Envelope`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use proctor::Envelope;
    ///
    /// #[derive(Debug, Eq, PartialEq)]
    /// struct SomeErr;
    ///
    /// let x: Result<Envelope<i32>, SomeErr> = Ok(Envelope::new(5));
    /// let y: Envelope<Result<i32, SomeErr>> = Envelope::new(Ok(5));
    /// assert_eq!(x, y.transpose());
    /// ```
    #[inline]
    pub fn transpose(self) -> Result<Envelope<T>, E> {
        match self.content {
            Ok(content) => Ok(Envelope { content, metadata: self.metadata.relabel() }),
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl<C> ProctorContext for Envelope<C>
where
    C: Label + ProctorContext + PartialEq,
{
    type ContextData = <C as ProctorContext>::ContextData;
    type Error = <C as ProctorContext>::Error;

    fn custom(&self) -> TableType {
        self.content.custom()
    }
}

impl<T> Serialize for Envelope<T>
where
    T: Serialize + Label,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("Envelope", 2)?;
        s.serialize_field("metadata", &self.metadata)?;
        s.serialize_field("content", &self.content)?;
        s.end()
    }
}

impl<'de, T> Deserialize<'de> for Envelope<T>
where
    T: de::DeserializeOwned + Label,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(AsRefStr)]
        enum Field {
            MetaData,
            Content,
        }

        impl Field {
            const META_DATA: &'static str = "metadata";
            const CONTENT: &'static str = "content";
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> de::Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("`metadata` or `content`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "metadata" => Ok(Field::MetaData),
                            "content" => Ok(Field::Content),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct EnvelopeVisitor<T0> {
            marker: PhantomData<T0>,
        }

        impl<T0> EnvelopeVisitor<T0> {
            pub const fn new() -> Self {
                Self { marker: PhantomData }
            }
        }

        impl<'de, T0> de::Visitor<'de> for EnvelopeVisitor<T0>
        where
            T0: de::DeserializeOwned + Label,
        {
            type Value = Envelope<T0>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let t_name = std::any::type_name::<T0>();
                f.write_str(format!("struct Envelope<{}>", t_name).as_str())
            }

            fn visit_map<V>(self, mut map: V) -> Result<Envelope<T0>, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut metadata = None;
                let mut content = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::MetaData => {
                            if metadata.is_some() {
                                return Err(de::Error::duplicate_field(Field::META_DATA));
                            }
                            metadata = Some(map.next_value()?);
                        },
                        Field::Content => {
                            if content.is_some() {
                                return Err(de::Error::duplicate_field(Field::CONTENT));
                            }
                            content = Some(map.next_value()?);
                        },
                    }
                }

                let metadata = metadata.ok_or_else(|| de::Error::missing_field(Field::META_DATA))?;
                let content = content.ok_or_else(|| de::Error::missing_field(Field::CONTENT))?;

                Ok(Envelope { metadata, content })
            }
        }

        const FIELDS: &[&str] = &[Field::META_DATA, Field::CONTENT];
        deserializer.deserialize_struct("Envelope", FIELDS, EnvelopeVisitor::<T>::new())
    }
}

/// A set of metdata regarding the envelope contents.
#[derive(Eq, Serialize, Deserialize)]
pub struct MetaData<T>
where
    T: Label,
{
    correlation_id: Id<T>,
    recv_timestamp: Timestamp,
}

impl<T> fmt::Debug for MetaData<T>
where
    T: Label,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaData")
            .field("correlation", &self.correlation_id)
            .field("recv_timestamp", &self.recv_timestamp.to_string())
            .finish()
    }
}

impl<T> fmt::Display for MetaData<T>
where
    T: Label + Send,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} @ {}", self.correlation_id, self.recv_timestamp)
    }
}

impl<T> Default for MetaData<T>
where
    T: Label,
{
    fn default() -> Self {
        Self::from_parts(gen_correlation_id(), Timestamp::now())
    }
}

impl<T> MetaData<T>
where
    T: Label,
{
    pub const fn from_parts(correlation_id: Id<T>, recv_timestamp: Timestamp) -> Self {
        Self { correlation_id, recv_timestamp }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_recv_timestamp(self, recv_timestamp: Timestamp) -> Self {
        Self { recv_timestamp, ..self }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn into_parts(self) -> (Id<T>, Timestamp) {
        (self.correlation_id, self.recv_timestamp)
    }

    pub fn relabel<U: Label>(self) -> MetaData<U> {
        MetaData {
            correlation_id: self.correlation_id.relabel(),
            recv_timestamp: self.recv_timestamp,
        }
    }
}

impl<T> Correlation for MetaData<T>
where
    T: Label + Sync,
{
    type Correlated = T;

    fn correlation(&self) -> &Id<Self::Correlated> {
        &self.correlation_id
    }
}

impl<T> ReceivedAt for MetaData<T>
where
    T: Label,
{
    fn recv_timestamp(&self) -> Timestamp {
        self.recv_timestamp
    }
}

impl<T> Clone for MetaData<T>
where
    T: Label,
{
    fn clone(&self) -> Self {
        Self {
            correlation_id: self.correlation_id.clone(),
            recv_timestamp: self.recv_timestamp,
        }
    }
}

impl<T> PartialEq for MetaData<T>
where
    T: Label,
{
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<T> PartialOrd for MetaData<T>
where
    T: Label,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.recv_timestamp.partial_cmp(&other.recv_timestamp)
    }
}

impl<T> Ord for MetaData<T>
where
    T: Eq + Label,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.recv_timestamp.cmp(&other.recv_timestamp)
    }
}

impl<T> Hash for MetaData<T>
where
    T: Label,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.correlation_id.hash(state)
    }
}

impl<T> std::ops::Add for MetaData<T>
where
    T: Label,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        if self < rhs {
            rhs
        } else {
            self
        }
    }
}

impl<T> Monoid for MetaData<T>
where
    T: Label,
{
    fn empty() -> Self {
        MetaData::from_parts(gen_correlation_id(), Timestamp::ZERO)
    }
}

impl<T> Semigroup for MetaData<T>
where
    T: Label,
{
    fn combine(&self, other: &Self) -> Self {
        if self < other {
            other.clone()
        } else {
            self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use pretty_snowflake::Labeling;
    use serde_test::{assert_tokens, Token};

    static META_DATA: Lazy<MetaData<TestData>> = Lazy::new(|| MetaData::default());

    #[derive(Debug, Clone, Label, PartialEq, Serialize, Deserialize)]
    struct TestData(i32);

    #[derive(Debug, Label, PartialEq)]
    struct TestContainer(TestData);

    #[derive(Debug, Label, PartialEq)]
    struct TestEnvelopeContainer(Envelope<TestData>);

    #[test]
    fn test_envelope_map() {
        let data = TestData(13);

        let metadata = MetaData::from_parts(
            Id::direct(<TestData as Label>::labeler().label(), 0, "zero"),
            Timestamp::now(),
        );
        let enveloped_data = Envelope::from_parts(metadata.clone(), data);
        let expected = TestContainer(enveloped_data.clone().into_inner());
        let actual = enveloped_data.map(TestContainer);

        assert_eq!(actual.metadata().correlation().num(), metadata.correlation().num());
        assert_eq!(
            actual.metadata().correlation().pretty(),
            metadata.correlation().pretty()
        );
        assert_eq!(actual.metadata().recv_timestamp(), metadata.recv_timestamp());
        assert_eq!(actual.as_ref(), &expected);
    }

    #[test]
    fn test_envelope_flat_map() {
        let data = TestData(13);

        let metadata = MetaData::from_parts(
            Id::direct(<TestData as Label>::labeler().label(), 0, "zero"),
            Timestamp::now(),
        );
        let enveloped_data = Envelope::from_parts(metadata.clone(), data);
        let expected = TestEnvelopeContainer(enveloped_data.clone());
        let actual = enveloped_data.flat_map(TestEnvelopeContainer);

        assert_eq!(actual.metadata().correlation().num(), metadata.correlation().num());
        assert_eq!(
            actual.metadata().correlation().pretty(),
            metadata.correlation().pretty()
        );
        assert_eq!(actual.metadata().recv_timestamp(), metadata.recv_timestamp());
        assert_eq!(actual.as_ref(), &expected);
    }

    #[test]
    fn test_envelope_serde_tokens() {
        let data = TestData(17);
        let actual = Envelope::from_parts(META_DATA.clone(), data);
        assert_tokens(
            &actual,
            &vec![
                Token::Struct { name: "Envelope", len: 2 },
                Token::Str("metadata"),
                Token::Struct { name: "MetaData", len: 2 },
                Token::Str("correlation_id"),
                Token::Struct { name: "Id", len: 2 },
                Token::Str("snowflake"),
                Token::I64(META_DATA.correlation_id.num()),
                Token::Str("pretty"),
                Token::Str(META_DATA.correlation_id.pretty()),
                Token::StructEnd,
                Token::Str("recv_timestamp"),
                Token::TupleStruct { name: "Timestamp", len: 2 },
                Token::I64(META_DATA.recv_timestamp.as_pair().0),
                Token::U32(META_DATA.recv_timestamp.as_pair().1),
                Token::TupleStructEnd,
                Token::StructEnd,
                Token::Str("content"),
                Token::NewtypeStruct { name: "TestData" },
                Token::I32(17),
                Token::StructEnd,
            ],
        )
    }
}
