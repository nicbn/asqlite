use crate::Statement;

/// Sealed trait that allows [`str`], [`String`] to be used instead of
/// statements.
pub trait IntoStatement: private::Sealed {
    #[doc(hidden)]
    fn __into_str_or_statement(this: &Self) -> __StrOrStatement<'_>;
}

#[doc(hidden)]
#[derive(Debug)]
pub enum __StrOrStatement<'a> {
    Str(&'a str),
    Statement(Statement),
}

mod private {
    use crate::Statement;

    pub trait Sealed {}

    impl Sealed for &str {}

    impl Sealed for String {}

    impl Sealed for &Statement {}

    impl Sealed for Statement {}
}

impl IntoStatement for &str {
    #[inline]
    fn __into_str_or_statement(this: &Self) -> __StrOrStatement<'_> {
        __StrOrStatement::Str(this)
    }
}

impl IntoStatement for String {
    #[inline]
    fn __into_str_or_statement(this: &Self) -> __StrOrStatement<'_> {
        __StrOrStatement::Str(this.as_str())
    }
}

impl IntoStatement for &Statement {
    #[inline]
    fn __into_str_or_statement(this: &Self) -> __StrOrStatement<'_> {
        __StrOrStatement::Statement((*this).clone())
    }
}

impl IntoStatement for Statement {
    #[inline]
    fn __into_str_or_statement(this: &Self) -> __StrOrStatement<'_> {
        __StrOrStatement::Statement(this.clone())
    }
}
