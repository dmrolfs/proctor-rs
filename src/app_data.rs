use std::fmt::Debug;

pub trait AppData: Debug + Clone + Send + Sync + 'static {}

/// AppData is automatically derived for types compatible with graph stage processing. If needed,
/// the AppData trait may also be included in the #[derive] specification.
impl<T: Debug + Clone + Send + Sync + 'static> AppData for T {}
