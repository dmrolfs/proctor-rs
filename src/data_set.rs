use crate::elements::telemetry::TableType;
use crate::phases::sense::CorrelationGenerator;
use crate::AppData;
use crate::{Correlation, ProctorContext, ReceivedAt, Timestamp};
use async_trait::async_trait;
use frunk::{Monoid, Semigroup};
use once_cell::sync::Lazy;
use pretty_snowflake::{AlphabetCodec, Id, IdPrettifier, Label, Labeling, MachineNode};
use serde::ser::SerializeStruct;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use strum_macros::AsRefStr;
use tokio::sync::Mutex;

static ID_GENERATOR: Lazy<Mutex<Option<CorrelationGenerator>>> = Lazy::new(|| Mutex::new(None));

pub async fn set_correlation_generator(gen: CorrelationGenerator) {
    let mut correlation_generator = ID_GENERATOR.lock().await;
    *correlation_generator = Some(gen);
}

async fn gen_correlation_id<T: Label>() -> Id<T> {
    let mut guard = ID_GENERATOR.lock().await;
    let gen = match &mut *guard {
        Some(g) => g,
        None => {
            *guard = Some(CorrelationGenerator::distributed(
                MachineNode::default(),
                IdPrettifier::<AlphabetCodec>::default(),
            ));
            guard
                .as_mut()
                .expect("failed to set proctor::phases::sense::data::ID_GENERATOR")
        },
    };

    gen.next_id().relabel()
}

pub trait IntoDataSet {
    type Data: AppData + Label;

    fn into_dataset(self) -> DataSet<Self::Data>;
}

/// A Springline sensor data set.
#[derive(Debug, Clone)]
pub struct DataSet<T>
where
    T: Label,
{
    metadata: MetaData<T>,
    data: T,
}

impl<T: fmt::Display> fmt::Display for DataSet<T>
where
    T: Label + Send,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})[{}]", self.data, self.metadata)
    }
}

impl<T> IntoDataSet for DataSet<T>
where
    T: AppData + Label,
{
    type Data = T;

    fn into_dataset(self) -> DataSet<Self::Data> {
        self
    }
}

#[async_trait]
pub trait IntoPhaseData<T>
where
    T: Label,
{
    /// Wrap the data item in a PhaseData wrapper
    async fn into_phase_data(self) -> DataSet<T>;
}

impl<T> DataSet<T>
where
    T: Label + Send,
{
    /// Create a new sensor data set.
    pub async fn new(data: T) -> Self {
        Self { metadata: MetaData::new().await, data }
    }

    // pub fn clone_metadata_for<U>(&self, data: U) -> DataSet<U>
    // where
    //     U: Label + Send,
    // {
    //     DataSet { metadata: self.metadata.clone().relabel(), data }
    // }

    /// Get a reference to the sensor data metadata.
    pub const fn metadata(&self) -> &MetaData<T> {
        &self.metadata
    }

    /// Consumes self, returning the data item
    #[allow(clippy::missing_const_for_fn)]
    #[inline]
    pub fn into_inner(self) -> T {
        self.data
    }

    #[allow(clippy::missing_const_for_fn)]
    #[inline]
    pub fn into_parts(self) -> (MetaData<T>, T) {
        (self.metadata, self.data)
    }

    #[inline]
    pub const fn from_parts(metadata: MetaData<T>, data: T) -> Self {
        Self { metadata, data }
    }

    // pub fn translate_metadata<U>(metadata: &MetaData<U>, data: T) -> Self
    // where
    //     U: Label,
    // {
    //     Self { metadata: metadata.clone().relabel(), data }
    // }

    pub fn adopt_metadata<U>(&mut self, new_metadata: MetaData<U>) -> MetaData<T>
    where
        U: Label,
    {
        let old_metadata = self.metadata.clone();
        self.metadata = new_metadata.relabel();
        old_metadata
    }

    pub fn map<F, U>(self, f: F) -> DataSet<U>
    where
        U: Label + Send,
        F: FnOnce(T) -> U,
    {
        let metadata = self.metadata.clone().relabel();
        DataSet { metadata, data: f(self.data) }
    }

    //todo: maybe offer ref mapping via DataSetRef type?
    // pub fn as_mapped<F, U>(&self, f: F) -> DataSet<&U>
    // where
    //     U: Label + Send + Sync,
    //     F: FnOnce(&Self) -> &U,
    // {
    //     let metadata = self.metadata.clone().relabel();
    //     DataSet { metadata, data: f(&self.data) }
    // }

    pub fn flat_map<F, U>(self, f: F) -> DataSet<U>
    where
        U: Label + Send,
        F: FnOnce(Self) -> U,
    {
        let metadata = self.metadata.clone().relabel();
        DataSet { metadata, data: f(self) }
    }

    pub async fn and_then<Op, Fut, U>(self, f: Op) -> DataSet<U>
    where
        U: Label + Send,
        Fut: Future<Output = U> + Send,
        Op: FnOnce(T) -> Fut + Send,
    {
        let metadata = self.metadata.clone().relabel();
        DataSet { metadata, data: f(self.data).await }
    }
}

impl<T> std::ops::Deref for DataSet<T>
where
    T: Label,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> AsRef<T> for DataSet<T>
where
    T: Label,
{
    fn as_ref(&self) -> &T {
        &self.data
    }
}

impl<T> AsMut<T> for DataSet<T>
where
    T: Label,
{
    fn as_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

impl<T> PartialEq for DataSet<T>
where
    T: Label + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T> PartialEq<T> for DataSet<T>
where
    T: Label + PartialEq,
{
    fn eq(&self, other: &T) -> bool {
        &self.data == other
    }
}

impl<T> Correlation for DataSet<T>
where
    T: Label + Sync,
{
    type Correlated = T;

    fn correlation(&self) -> &Id<Self::Correlated> {
        self.metadata.correlation()
    }
}

impl<T> ReceivedAt for DataSet<T>
where
    T: Label,
{
    fn recv_timestamp(&self) -> Timestamp {
        self.metadata.recv_timestamp()
    }
}

impl<T> Label for DataSet<T>
where
    T: Label,
{
    type Labeler = <T as Label>::Labeler;

    fn labeler() -> Self::Labeler {
        <T as Label>::labeler()
    }
}

impl<T> Monoid for DataSet<T>
where
    T: Monoid + Label + Send,
{
    fn empty() -> Self {
        Self::from_parts(
            MetaData::from_parts(
                Id::<T>::direct(<Self as Label>::labeler().label(), 0, "<undefined>"),
                Timestamp::ZERO,
            ),
            <T as Monoid>::empty(),
        )
    }
}

impl<T> Semigroup for DataSet<T>
where
    T: Semigroup + Label + Send,
{
    fn combine(&self, other: &Self) -> Self {
        Self::from_parts(self.metadata().clone(), self.data.combine(&other.data))
    }
}

impl<T> From<DataSet<T>> for crate::elements::Telemetry
where
    T: Label + Into<Self>,
{
    fn from(that: DataSet<T>) -> Self {
        that.data.into()
    }
}

impl<T> oso::ToPolar for DataSet<T>
where
    T: Label + oso::ToPolar,
{
    fn to_polar(self) -> oso::PolarValue {
        self.data.to_polar()
    }
}

impl<T> DataSet<Option<T>>
where
    T: Label + Send,
{
    /// Transposes a `PhaseData` of an [`Option`] into an [`Option`] of `PhaseData`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use proctor::DataSet;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let x: Option<DataSet<i32>> = Some(DataSet::new(5).await);
    ///     let y: DataSet<Option<i32>> = DataSet::new(Some(5)).await;
    ///     assert_eq!(x, y.transpose());
    /// }
    /// ```
    #[inline]
    pub fn transpose(self) -> Option<DataSet<T>> {
        match self.data {
            Some(d) => Some(DataSet { data: d, metadata: self.metadata.relabel() }),
            None => None,
        }
    }
}

impl<T, E> DataSet<Result<T, E>>
where
    T: Label + Send,
{
    /// Transposes a `PhaseData` of a [`Result`] into a [`Result`] of `PhaseData`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use proctor::DataSet;
    ///
    /// #[derive(Debug, Eq, PartialEq)]
    /// struct SomeErr;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let x: Result<DataSet<i32>, SomeErr> = Ok(DataSet::new(5).await);
    ///     let y: DataSet<Result<i32, SomeErr>> = DataSet::new(Ok(5)).await;
    ///     assert_eq!(x, y.transpose());
    /// }
    /// ```
    #[inline]
    pub fn transpose(self) -> Result<DataSet<T>, E> {
        match self.data {
            Ok(d) => Ok(DataSet { data: d, metadata: self.metadata.relabel() }),
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl<C> ProctorContext for DataSet<C>
where
    C: Label + ProctorContext + PartialEq,
{
    type ContextData = <C as ProctorContext>::ContextData;
    type Error = <C as ProctorContext>::Error;

    fn custom(&self) -> TableType {
        self.data.custom()
    }
}

#[async_trait]
impl<T> IntoPhaseData<T> for T
where
    T: Label + Send,
{
    async fn into_phase_data(self) -> DataSet<T> {
        DataSet::new(self).await
    }
}

#[async_trait]
impl<T> IntoPhaseData<T> for DataSet<T>
where
    T: Label + Send,
{
    async fn into_phase_data(self) -> Self {
        self
    }
}

impl<T> Serialize for DataSet<T>
where
    T: Serialize + Label,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("PhaseData", 2)?;
        s.serialize_field("metadata", &self.metadata)?;
        s.serialize_field("data", &self.data)?;
        s.end()
    }
}

impl<'de, T> Deserialize<'de> for DataSet<T>
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
            Data,
        }

        impl Field {
            const META_DATA: &'static str = "metadata";
            const DATA: &'static str = "data";
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
                        formatter.write_str("`metadata` or `data`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "metadata" => Ok(Field::MetaData),
                            "data" => Ok(Field::Data),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct PhaseDataVisitor<T0> {
            marker: PhantomData<T0>,
        }

        impl<T0> PhaseDataVisitor<T0> {
            pub const fn new() -> Self {
                Self { marker: PhantomData }
            }
        }

        impl<'de, T0> de::Visitor<'de> for PhaseDataVisitor<T0>
        where
            T0: de::DeserializeOwned + Label,
        {
            type Value = DataSet<T0>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let t_name = std::any::type_name::<T0>();
                f.write_str(format!("struct PhaseData<{}>", t_name).as_str())
            }

            fn visit_map<V>(self, mut map: V) -> Result<DataSet<T0>, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut metadata = None;
                let mut data = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::MetaData => {
                            if metadata.is_some() {
                                return Err(de::Error::duplicate_field(Field::META_DATA));
                            }
                            metadata = Some(map.next_value()?);
                        },
                        Field::Data => {
                            if data.is_some() {
                                return Err(de::Error::duplicate_field(Field::DATA));
                            }
                            data = Some(map.next_value()?);
                        },
                    }
                }

                let metadata = metadata.ok_or_else(|| de::Error::missing_field(Field::META_DATA))?;
                let data = data.ok_or_else(|| de::Error::missing_field(Field::DATA))?;

                Ok(DataSet { metadata, data })
            }
        }

        const FIELDS: &[&str] = &[Field::META_DATA, Field::DATA];
        deserializer.deserialize_struct("PhaseData", FIELDS, PhaseDataVisitor::<T>::new())
    }
}

/// A set of metdata regarding the sensor data set.
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

impl<T: fmt::Display> fmt::Display for MetaData<T>
where
    T: Label + Send,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} @ {}", self.correlation_id, self.recv_timestamp)
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

impl<T> MetaData<T>
where
    T: Label,
{
    pub async fn new() -> Self {
        Self::from_parts(gen_correlation_id().await, Timestamp::now())
    }
}

impl<T> MetaData<T>
where
    T: Label,
{
    #[allow(clippy::missing_const_for_fn)]
    pub fn into_parts(self) -> (Id<T>, Timestamp) {
        (self.correlation_id, self.recv_timestamp)
    }

    pub const fn from_parts(correlation_id: Id<T>, recv_timestamp: Timestamp) -> Self {
        Self { correlation_id, recv_timestamp }
    }

    pub fn relabel<U: Label>(self) -> MetaData<U> {
        MetaData {
            correlation_id: self.correlation_id.relabel(),
            recv_timestamp: self.recv_timestamp,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[derive(Debug, Clone, Label, PartialEq)]
    struct TestData(i32);

    #[derive(Debug, Label, PartialEq)]
    struct TestContainer(TestData);

    #[derive(Debug, Label, PartialEq)]
    struct TestDataSetContainer(DataSet<TestData>);

    #[test]
    fn test_data_set_map() {
        let data = TestData(13);

        let metadata = MetaData::from_parts(
            Id::direct(<TestData as Label>::labeler().label(), 0, "zero"),
            Timestamp::now(),
        );
        let dataset = DataSet::from_parts(metadata.clone(), data);
        let expected = TestContainer(dataset.clone().into_inner());
        let actual = dataset.map(TestContainer);

        assert_eq!(actual.metadata().correlation().num(), metadata.correlation().num());
        assert_eq!(
            actual.metadata().correlation().pretty(),
            metadata.correlation().pretty()
        );
        assert_eq!(actual.metadata().recv_timestamp(), metadata.recv_timestamp());
        assert_eq!(actual.as_ref(), &expected);
    }

    #[test]
    fn test_data_set_flat_map() {
        let data = TestData(13);

        let metadata = MetaData::from_parts(
            Id::direct(<TestData as Label>::labeler().label(), 0, "zero"),
            Timestamp::now(),
        );
        let dataset = DataSet::from_parts(metadata.clone(), data);
        let expected = TestDataSetContainer(dataset.clone());
        let actual = dataset.flat_map(TestDataSetContainer);

        assert_eq!(actual.metadata().correlation().num(), metadata.correlation().num());
        assert_eq!(
            actual.metadata().correlation().pretty(),
            metadata.correlation().pretty()
        );
        assert_eq!(actual.metadata().recv_timestamp(), metadata.recv_timestamp());
        assert_eq!(actual.as_ref(), &expected);
    }
}
