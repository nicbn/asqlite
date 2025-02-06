use crate::Result;

#[doc(hidden)]
pub mod __private {
    use super::ParamList;
    use crate::{convert::Sql, Result};

    #[derive(Debug)]
    pub struct Param {
        pub name: Option<&'static str>,
        pub param: Sql,
    }

    #[inline]
    pub fn new_param_list(items: Result<Box<[Param]>>) -> ParamList {
        ParamList { items }
    }
}

/// Send parameters to queries.
///
/// Created by the [`params!`] macro.
///
/// [`params!`]: crate::params!
#[derive(Debug)]
pub struct ParamList {
    pub(crate) items: Result<Box<[Param]>>,
}

impl From<()> for ParamList {
    fn from(_: ()) -> Self {
        Self {
            items: Ok(Box::new([])),
        }
    }
}

pub(crate) use __private::Param;
