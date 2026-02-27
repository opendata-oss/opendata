use proc_macro2::TokenStream;
use quote::quote;
use syn::{Expr, Ident, ItemFn, Stmt, Token, parse::Parse, parse::ParseStream, parse2};

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
            // check that next token is an identifier
            // parse as identifier
            let key: Ident = input.parse()?;

            match key.to_string().as_str() {
                // parse as merge_operator = expression (Expr)
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

pub fn test_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    // parse arguments to the macro (see Parse impl for TestMacroArgs for implementation)
    let args_parsed = parse2::<TestMacroArgs>(args).expect("failed to parse macro args");

    // parse string below macro as free standing function
    let item_fn = parse2::<ItemFn>(input).expect("failed to parse function");

    // grab the name of the function from signature
    let fn_name = &item_fn.sig.ident;

    // construct inner function name
    let fn_name_inner = Ident::new(&format!("{}__inner", fn_name), item_fn.sig.ident.span());

    // get statements from function body
    let body = item_fn.block.stmts.clone();

    // generate storage creation based on whether merge_operator was provided
    let storage_creation = if let Some(merge_op) = args_parsed.merge_operator {
        quote! {
            let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(
                common::storage::in_memory::InMemoryStorage::with_merge_operator(
                    std::sync::Arc::new(#merge_op)
                )
            );
        }
    } else {
        quote! {
            let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(
                common::storage::in_memory::InMemoryStorage::default()
            );
        }
    };

    quote! {
         #[::tokio::test]
        #[allow(unused_must_use)]
        async fn #fn_name() {
            #storage_creation
            #fn_name_inner(storage.clone()).await;
            let _ = storage.close().await;
        }

        async fn #fn_name_inner(storage: std::sync::Arc<dyn Storage>) {
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
            async fn my_test() {
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
        assert!(
            wrapper
                .block
                .to_token_stream()
                .to_string()
                .contains("storage . close ()"),
            "Wrapper should call storage.close()"
        );

        // verify inner function exists with correct name
        let inner = functions
            .get("my_test__inner")
            .expect("Should have inner function named 'my_test__inner'");
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
            "storage : std :: sync :: Arc < dyn Storage >",
            "Inner function first parameter should be dynamic storage Arc"
        );

        // verify inner function has the original body
        assert_eq!(
            parsed_original_input.block.to_token_stream().to_string(),
            inner.block.to_token_stream().to_string(),
            "Inner function should have the same body as the original input"
        );
    }

    #[test]
    fn test_with_merge_operator() {
        let args = quote! { merge_operator = MyMergeOp };
        let input = quote! {
            async fn my_test() {
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

        // verify wrapper body contains storage creation with merge operator
        let wrapper_code = wrapper.block.to_token_stream().to_string();
        assert!(
            wrapper_code.contains("InMemoryStorage :: with_merge_operator"),
            "Storage creation should use with_merge_operator when merge_operator is specified"
        );
        assert!(
            wrapper_code.contains("MyMergeOp"),
            "Generated code should reference the merge operator"
        );
        assert!(
            wrapper_code.contains("my_test__inner"),
            "Wrapper should call the inner function"
        );
        assert!(
            wrapper_code.contains("storage . close ()"),
            "Wrapper should call storage.close()"
        );

        // Verify inner function exists
        functions
            .get("my_test__inner")
            .expect("Should have inner function named 'my_test__inner'");
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
