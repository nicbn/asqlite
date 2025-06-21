#[cfg(test)]
mod tests;

/// Creates a parameter list.
///
/// This can be used for either positional parameters, named parameters, or
/// mixing both.
///
/// The syntax is as follows:
///
/// ```
/// # use asqlite::params;
/// #
/// params!(
///     // Positional arguments
///     1, 2, 3, 4, 5,
///     // Named arguments
///     "name" => "Jeremy", "age" => 50,
/// )
/// # ;
/// ```
///
/// # Using parameters
///
/// Every parameter, including named parameters, has a position, starting at 1.
///
/// Inside the SQL query, you can reference parameters positionally either by
/// using the `?` token, which references the next parameter, or the `?NNN`
/// token, where `NNN` is the parameter position.
///
/// You can also reference parameters by name. This can be done using either
/// a `:` prefix (`:parameter`), a `@` prefix (`@parameter`) or a `$` prefix
/// (`$parameter`).
///
/// When using `params!` with named arguments, you should include the `@`
/// or `$` prefix in the parameter name. Including the `:` prefix is optional:
/// the library will automatically add it if the other two are not found.
///
/// See also [the SQLite documentation](https://www.sqlite.org/lang_expr.html#parameters).
///
/// # Example
///
/// ```
/// async fn insert_user_data(
///     connection: &mut asqlite::Connection,
///     name: String,
///     email: String,
/// ) -> asqlite::Result<()> {
///     connection
///         .insert(
///             "INSERT INTO users (name, email) VALUES (?, :email)",
///             asqlite::params!(name, "email" => email),
///         )
///         .await?;
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! params {
    ( $( $x1:expr $( => $x2:expr )? ),* $(,)? ) => {{
        let builder = $crate::convert::ParamList::builder($crate::__len!($($x1),*));
        $( let builder = $crate::__create_param!(builder, $x1 $( => $x2 )?); )*
        builder.build()
    }};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __create_param {
    ($b:expr, $name:expr => $param:expr) => {
        $b.named_param($name, $param)
    };

    ($b:expr, $param:expr) => {
        $b.param($param)
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __len {
    ($h:expr, $($r:expr),*) => {
        1 + $crate::__len!($($r),*)
    };

    ($h:expr) => {
        1
    };

    () => {
        0
    };
}
