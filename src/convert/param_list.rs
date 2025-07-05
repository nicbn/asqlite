use crate::{
    convert::{IntoSql, Sql},
    Error, Result,
};
use std::{borrow::Cow, iter, mem};

#[derive(Debug, Default)]
pub(crate) struct Param {
    pub(crate) name: Option<Cow<'static, str>>,
    pub(crate) param: Sql,
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

impl ParamList {
    /// Create a new builder.
    ///
    /// Usually not necessary to call from user code: you can use [`params!`] instead.
    pub fn builder(len: usize) -> ParamListBuilder {
        ParamListBuilder {
            items: iter::repeat_with(Default::default).take(len).collect(),
            error: None,
            initialized: 0,
        }
    }

    /// Take the parameter list.
    pub(crate) fn take(&mut self) -> Self {
        mem::replace(self, ParamList::from(()))
    }
}

impl From<()> for ParamList {
    fn from(_: ()) -> Self {
        Self {
            items: Ok(Box::new([])),
        }
    }
}

/// A builder for the struct [`ParamList`].
pub struct ParamListBuilder {
    items: Box<[Param]>,
    error: Option<Error>,
    initialized: usize,
}

impl ParamListBuilder {
    /// Add a parameter.
    ///
    /// # Panics
    ///
    /// Panics if already full.
    pub fn param<T>(mut self, value: T) -> Self
    where
        T: IntoSql,
    {
        if self.error.is_none() {
            match value.into_sql() {
                Ok(value) => {
                    *self
                        .items
                        .get_mut(self.initialized)
                        .expect("tried to add parameter to full ParamListBuilder") = Param {
                        name: None,
                        param: value,
                    };
                    self.initialized += 1;
                }
                Err(e) => {
                    self.error = Some(e);
                }
            }
        }

        self
    }

    /// Add a parameter, named.
    ///
    /// # Panics
    ///
    /// Panics if already full.
    pub fn named_param<T>(mut self, name: impl Into<Cow<'static, str>>, value: T) -> Self
    where
        T: IntoSql,
    {
        if self.error.is_none() {
            match value.into_sql() {
                Ok(value) => {
                    *self
                        .items
                        .get_mut(self.initialized)
                        .expect("tried to add parameter to full ParamListBuilder") = Param {
                        name: Some(name.into()),
                        param: value,
                    };
                    self.initialized += 1;
                }
                Err(e) => {
                    self.error = Some(e);
                }
            }
        }

        self
    }

    /// Build.
    pub fn build(self) -> ParamList {
        ParamList {
            items: if let Some(e) = self.error {
                Err(e)
            } else {
                Ok(self.items)
            },
        }
    }
}
