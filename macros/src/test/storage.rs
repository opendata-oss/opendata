use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    Attribute, Error, Expr, Ident, ItemFn, Token,
    parse::{Parse, ParseStream},
    parse_quote, parse2,
    spanned::Spanned,
};

/// Parsed arguments for the storage test macro
struct TestMacroArgs {
    merge_operator: Option<Expr>,
}

impl Parse for TestMacroArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut merge_operator = None;

        // handle empty args
        if input.is_empty() {
            return Ok(TestMacroArgs { merge_operator });
        }

        // parse merge_operator = path syntax
        if input.peek(Ident) {
            // parse as identifier
            let key: Ident = input.parse()?;

            match key.to_string().as_str() {
                // parse as:
                // match identifier: merge_operator
                // match token '='
                // match some expression (Expr)
                "merge_operator" => {
                    input.parse::<Token![=]>()?;
                    merge_operator = Some(input.parse()?);
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        &key,
                        format!(
                            "unsupported argument '{}'. Supported arguments: merge_operator",
                            key
                        ),
                    ));
                }
            }
        }

        // check for any remaining unparsed tokens
        if !input.is_empty() {
            let remaining: TokenStream = input.parse()?;
            return Err(syn::Error::new_spanned(
                &remaining,
                "unexpected tokens. Expected end of arguments",
            ));
        }

        Ok(TestMacroArgs { merge_operator })
    }
}

/// Accept `Arc<slatedb::Db>` with or without the `slatedb::` prefix.
///
/// The tokenstream `to_string()` representation pads operators with spaces, so the
/// canonical forms we compare against are `Arc < slatedb :: Db >` and `Arc < Db >`.
fn is_accepted_db_arc(ty_str: &str) -> bool {
    matches!(ty_str, "Arc < slatedb :: Db >" | "Arc < Db >")
}

pub fn test_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    // parse arguments to the macro (see Parse impl for TestMacroArgs for implementation)
    let args_parsed = match parse2::<TestMacroArgs>(args) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error(),
    };

    // parse string below macro as free standing function
    let item_fn = match parse2::<ItemFn>(input) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error(),
    };

    let tokio_macro: Attribute = item_fn
        .attrs
        .iter()
        .find(|attr| {
            attr.path().segments.len() == 2
                && attr.path().segments[0].ident == "tokio"
                && attr.path().segments[1].ident == "test"
        })
        .cloned()
        .unwrap_or_else(|| parse_quote!(#[tokio::test]));

    // grab the name of the function from signature
    let fn_name = &item_fn.sig.ident;

    // construct inner function name
    let fn_name_inner = Ident::new(&format!("{}_inner", fn_name), item_fn.sig.ident.span());

    // validate that the function only has a single parameter of type Arc<slatedb::Db>
    if item_fn.sig.inputs.len() != 1 {
        return Error::new(
            item_fn.span(),
            "expected single parameter of type Arc<slatedb::Db>",
        )
        .to_compile_error();
    }

    // validate type of parameter
    let input_name = match &item_fn.sig.inputs[0] {
        syn::FnArg::Typed(pat_type) => {
            let ty_str = pat_type.ty.to_token_stream().to_string();
            if !is_accepted_db_arc(&ty_str) {
                return Error::new(
                    item_fn.span(),
                    format!(
                        "parameter '{}' must be of type Arc<slatedb::Db> (or Arc<Db>)",
                        pat_type.pat.to_token_stream()
                    ),
                )
                .to_compile_error();
            }
            pat_type.pat.to_token_stream()
        }
        _ => {
            return Error::new(
                item_fn.span(),
                "first parameter must be of type Arc<slatedb::Db>",
            )
            .to_compile_error();
        }
    };

    // get statements from function body
    let body = item_fn.block.stmts.clone();

    // generate storage creation based on whether merge_operator was provided
    let db_creation = if let Some(merge_op) = args_parsed.merge_operator {
        quote! {
            let __object_store = std::sync::Arc::new(
                ::slatedb::object_store::memory::InMemory::new()
            );
            let __merge_op: std::sync::Arc<
                dyn ::slatedb::MergeOperator + Send + Sync
            > = std::sync::Arc::new(#merge_op);
            let #input_name: std::sync::Arc<::slatedb::Db> = std::sync::Arc::new(
                ::slatedb::DbBuilder::new("test", __object_store)
                    .with_merge_operator(__merge_op)
                    .build()
                    .await
                    .expect("failed to build in-memory slatedb")
            );
        }
    } else {
        quote! {
            let __object_store = std::sync::Arc::new(
                ::slatedb::object_store::memory::InMemory::new()
            );
            let #input_name: std::sync::Arc<::slatedb::Db> = std::sync::Arc::new(
                ::slatedb::DbBuilder::new("test", __object_store)
                    .build()
                    .await
                    .expect("failed to build in-memory slatedb")
            );
        }
    };

    quote! {
        #tokio_macro
        #[allow(unused_must_use)]
        async fn #fn_name() {
            #db_creation
            #fn_name_inner(#input_name.clone()).await;
            let _ = #input_name.close().await;
        }

        async fn #fn_name_inner(#input_name: std::sync::Arc<::slatedb::Db>) {
            #(#body)*
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::ToTokens;
    use syn::{File, Item, ItemFn, parse2};

    /// Parse generated TokenStream into a File for structural analysis
    fn parse_output(output: &TokenStream) -> File {
        syn::parse2::<File>(output.clone()).expect("Generated code should be valid Rust")
    }

    /// Extract function items from a File, keyed by function name
    fn extract_functions(file: &File) -> std::collections::HashMap<String, ItemFn> {
        file.items
            .iter()
            .filter_map(|item| {
                if let Item::Fn(item_fn) = item {
                    Some((item_fn.sig.ident.to_string(), item_fn.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Check if a function has a specific attribute path (e.g., "tokio::test")
    fn has_attribute(func: &ItemFn, attr_path: &str) -> bool {
        func.attrs.iter().any(|attr| {
            let attr_str = attr.path().to_token_stream().to_string();
            let normalized_attr = attr_str
                .replace(" ", "")
                .trim_start_matches(':')
                .to_string();
            let normalized_path = attr_path
                .replace(" ", "")
                .trim_start_matches(':')
                .to_string();
            normalized_attr == normalized_path
        })
    }

    #[test]
    fn test_simple_function() {
        let input = quote! {
            async fn my_test(db: Arc<slatedb::Db>) {
                assert_eq!(1, 1);
            }
        };

        let parsed_original_input = parse2::<ItemFn>(input.clone()).unwrap();
        let output = test_impl(TokenStream::new(), input);
        let file = parse_output(&output);
        let functions = extract_functions(&file);

        // verify that 2 functions were generated (outer + inner function)
        assert_eq!(
            functions.len(),
            2,
            "Should generate exactly 2 functions (wrapper and inner)"
        );

        // verify wrapper function exists and has tokio::test attribute
        let wrapper = functions
            .get("my_test")
            .expect("Should have wrapper function named 'my_test'");
        assert!(
            has_attribute(wrapper, "tokio::test"),
            "Wrapper function should have #[tokio::test] attribute"
        );
        assert!(
            wrapper.sig.asyncness.is_some(),
            "Wrapper function should be async"
        );

        let wrapper_code = wrapper.block.to_token_stream().to_string();
        assert!(
            wrapper_code.contains("DbBuilder :: new"),
            "Wrapper should build the db via DbBuilder::new"
        );
        assert!(
            wrapper_code.contains("InMemory :: new"),
            "Wrapper should use an in-memory object store"
        );
        assert!(
            wrapper_code.contains("db . close ()"),
            "Wrapper should call db.close()"
        );

        // verify inner function exists with correct name
        let inner = functions
            .get("my_test_inner")
            .expect("Should have inner function named 'my_test_inner'");
        assert!(
            inner.sig.asyncness.is_some(),
            "Inner function should be async"
        );
        assert!(
            inner.sig.inputs.first().is_some(),
            "Inner function should accept a single parameter"
        );
        assert_eq!(
            inner
                .sig
                .inputs
                .first()
                .unwrap()
                .to_token_stream()
                .to_string(),
            "db : std :: sync :: Arc < :: slatedb :: Db >",
            "Inner function first parameter should be Arc<slatedb::Db>"
        );

        // verify inner function has the original body
        assert_eq!(
            parsed_original_input.block.to_token_stream().to_string(),
            inner.block.to_token_stream().to_string(),
            "Inner function should have the same body as the original input"
        );
    }

    #[test]
    fn test_simple_function_with_db_alias() {
        // Users may have imported `slatedb::Db` as `Db` — accept the short form too.
        let input = quote! {
            async fn my_test(db: Arc<Db>) {
                assert_eq!(1, 1);
            }
        };

        let output = test_impl(TokenStream::new(), input);
        let file = parse_output(&output);
        let functions = extract_functions(&file);
        assert_eq!(
            functions.len(),
            2,
            "Should generate exactly 2 functions even with the `Arc<Db>` short form"
        );
    }

    #[test]
    fn test_with_merge_operator() {
        let args = quote! { merge_operator = MyMergeOp };
        let input = quote! {
            async fn my_test(db: Arc<slatedb::Db>) {
                assert_eq!(1, 1);
            }
        };

        let output = test_impl(args, input);
        let file = parse_output(&output);
        let functions = extract_functions(&file);

        // verify we have exactly 2 functions
        assert_eq!(functions.len(), 2, "Should generate exactly 2 functions");

        // verify wrapper function
        let wrapper = functions
            .get("my_test")
            .expect("Should have wrapper function named 'my_test'");
        assert!(
            has_attribute(wrapper, "tokio::test"),
            "Should have #[tokio::test] attribute"
        );

        // verify wrapper body wires the merge operator into the DbBuilder
        let wrapper_code = wrapper.block.to_token_stream().to_string();
        assert!(
            wrapper_code.contains("with_merge_operator"),
            "Generated code should call DbBuilder::with_merge_operator when merge_operator is specified"
        );
        assert!(
            wrapper_code.contains("MyMergeOp"),
            "Generated code should reference the merge operator"
        );
        assert!(
            wrapper_code.contains("DbBuilder :: new"),
            "Generated code should build the db via DbBuilder::new"
        );
        assert!(
            wrapper_code.contains("my_test_inner"),
            "Wrapper should call the inner function"
        );
        assert!(
            wrapper_code.contains("db . close ()"),
            "Wrapper should call db.close()"
        );

        // Verify inner function exists
        functions
            .get("my_test_inner")
            .expect("Should have inner function named 'my_test_inner'");
    }

    #[test]
    fn test_tokio_macro_args() {
        let input = quote! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn my_test(db: Arc<slatedb::Db>) {
                assert_eq!(1, 1);
            }
        };

        let output = test_impl(TokenStream::new(), input);
        let file = parse_output(&output);
        let functions = extract_functions(&file);

        // verify we have exactly 2 functions
        assert_eq!(functions.len(), 2, "Should generate exactly 2 functions");

        // verify wrapper function
        let wrapper = functions
            .get("my_test")
            .expect("Should have wrapper function named 'my_test'");
        assert!(
            has_attribute(wrapper, "tokio::test"),
            "Should have #[tokio::test] attribute"
        );

        // verify the tokio::test args were preserved
        assert!(wrapper.attrs.iter().any(|attr| {
            attr.to_token_stream().to_string()
                == "# [tokio :: test (flavor = \"multi_thread\" , worker_threads = 2)]"
        }));
    }

    #[test]
    fn test_rejects_non_db_arc() {
        let input = quote! {
            async fn my_test(storage: Arc<dyn Storage>) {
                assert_eq!(1, 1);
            }
        };

        let output = test_impl(TokenStream::new(), input);
        let output_str = output.to_string();
        assert!(
            output_str.contains("compile_error"),
            "Expected compile_error! for Arc<dyn Storage>, got: {}",
            output_str
        );
        assert!(
            output_str.contains("Arc<slatedb::Db>"),
            "Expected diagnostic to mention Arc<slatedb::Db>, got: {}",
            output_str
        );
    }

    #[test]
    fn test_unsupported_argument() {
        let args = quote! { invalid_arg = value };
        let result = syn::parse2::<TestMacroArgs>(args);

        assert!(result.is_err(), "Should error on unsupported argument");
        let err_msg = result.err().unwrap().to_string();
        assert!(
            err_msg.contains("unsupported argument"),
            "Error should mention unsupported argument"
        );
        assert!(
            err_msg.contains("invalid_arg"),
            "Error should mention the invalid argument name"
        );
    }

    #[test]
    fn test_trailing_tokens() {
        let args = quote! { merge_operator = MyOp extra };
        let result = syn::parse2::<TestMacroArgs>(args);

        assert!(result.is_err(), "Should error on trailing tokens");
        let err_msg = result.err().unwrap().to_string();
        assert!(
            err_msg.contains("unexpected tokens"),
            "Error should mention unexpected tokens"
        );
    }
}
