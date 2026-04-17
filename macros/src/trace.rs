//! Implementation of the `#[trace_instrument]` attribute macro.
//!
//! Expands to `#[tracing::instrument(...)]` with a well-known marker field
//! (`_otrace = tracing::field::Empty`) injected into the `fields(...)` list.
//! The Chrome trace filter in `common::tracing` uses the presence of that
//! field to discriminate trace spans from ordinary `info!`/`info_span!`
//! callsites, so ordinary logs don't pollute the Chrome trace file.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{ItemFn, Meta, MetaList, Token, parse_quote, parse2};

/// Name of the marker field that identifies a span or event as participating
/// in OpenData query tracing. Kept in sync with
/// `common::tracing::TRACE_MARKER_FIELD`.
pub const TRACE_MARKER_FIELD: &str = "_otrace";

pub fn trace_instrument_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    let item_fn: ItemFn = match parse2(input) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    let new_args = match inject_marker(args) {
        Ok(ts) => ts,
        Err(e) => return e.to_compile_error(),
    };

    quote! {
        #[::tracing::instrument(#new_args)]
        #item_fn
    }
}

/// Rewrite the attribute args so that
/// 1. a `fields(...)` list exists and includes the trace marker field, and
/// 2. `level = "debug"` is set unless the caller specified their own level.
///
/// Trace spans default to `debug` rather than `info` so they don't pollute
/// ordinary `RUST_LOG=info` console output — they appear only when the fmt
/// layer is at debug+, or when the Chrome trace filter accepts them.
fn inject_marker(args: TokenStream) -> syn::Result<TokenStream> {
    let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
    let metas = parser.parse2(args)?;
    let marker_ident = proc_macro2::Ident::new(TRACE_MARKER_FIELD, proc_macro2::Span::call_site());
    let trace_id_ident = proc_macro2::Ident::new("trace_id", proc_macro2::Span::call_site());
    // Injected trace fields (marker + trace_id, both declared as Empty so
    // a layer can record real values at span creation time).
    let auto_fields: TokenStream = quote! {
        #marker_ident = ::tracing::field::Empty,
        #trace_id_ident = ::tracing::field::Empty
    };

    let mut new_metas: Punctuated<Meta, Token![,]> = Punctuated::new();
    let mut had_fields = false;
    let mut had_level = false;

    for meta in metas {
        if let Meta::NameValue(ref nv) = meta
            && nv.path.is_ident("level")
        {
            had_level = true;
        }
        match meta {
            Meta::List(list) if list.path.is_ident("fields") => {
                had_fields = true;
                let inner = list.tokens;
                let new_inner = if inner.is_empty() {
                    auto_fields.clone()
                } else {
                    quote! { #auto_fields, #inner }
                };
                new_metas.push(Meta::List(MetaList {
                    path: list.path,
                    delimiter: list.delimiter,
                    tokens: new_inner,
                }));
            }
            other => new_metas.push(other),
        }
    }

    if !had_fields {
        new_metas.push(parse_quote! { fields(#auto_fields) });
    }
    if !had_level {
        new_metas.push(parse_quote! { level = "debug" });
    }

    Ok(quote! { #new_metas })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_inject_marker_when_fields_absent() {
        // given
        let args = quote! { skip_all };

        // when
        let out = inject_marker(args).unwrap().to_string();

        // then
        assert!(out.contains("skip_all"), "preserves original args: {}", out);
        assert!(out.contains("fields"), "adds fields: {}", out);
        assert!(out.contains(TRACE_MARKER_FIELD), "injects marker: {}", out);
    }

    #[test]
    fn should_inject_marker_into_existing_fields() {
        // given
        let args = quote! { skip_all, fields(limit = 10, user = "x") };

        // when
        let out = inject_marker(args).unwrap().to_string();

        // then
        assert!(out.contains(TRACE_MARKER_FIELD), "marker injected: {}", out);
        assert!(out.contains("limit"), "original field preserved: {}", out);
        assert!(out.contains("user"), "original field preserved: {}", out);
    }

    #[test]
    fn should_inject_marker_into_empty_fields() {
        // given
        let args = quote! { fields() };

        // when
        let out = inject_marker(args).unwrap().to_string();

        // then
        assert!(out.contains(TRACE_MARKER_FIELD), "marker injected: {}", out);
    }

    #[test]
    fn should_produce_fn_attribute_with_instrument() {
        // given
        let args = quote! { skip_all };
        let body = quote! {
            async fn do_thing(x: u32) -> u32 { x + 1 }
        };

        // when
        let out = trace_instrument_impl(args, body).to_string();

        // then
        assert!(
            out.contains("tracing :: instrument"),
            "emits tracing::instrument: {}",
            out
        );
        assert!(out.contains("_otrace"), "emits marker field name: {}", out);
        assert!(out.contains("do_thing"), "preserves original fn: {}", out);
    }

    #[test]
    fn should_default_level_to_debug_when_caller_omits_it() {
        // given
        let args = quote! { skip_all };

        // when
        let out = inject_marker(args).unwrap().to_string();

        // then
        assert!(
            out.contains("level = \"debug\""),
            "expected level = \"debug\" injected by default, got: {}",
            out
        );
    }

    #[test]
    fn should_not_override_caller_specified_level() {
        // given - caller pins the level explicitly
        let args = quote! { skip_all, level = "info" };

        // when
        let out = inject_marker(args).unwrap().to_string();

        // then - only the caller-supplied level is present
        assert!(
            out.contains("level = \"info\""),
            "caller's level preserved: {}",
            out
        );
        assert!(
            !out.contains("level = \"debug\""),
            "default level must not be injected when one was supplied: {}",
            out
        );
    }

    #[test]
    fn should_surface_parse_errors_as_compile_error() {
        // given - garbage in the attribute position
        let args = quote! { , , fields(x = 1) };
        let body = quote! { async fn f() {} };

        // when
        let out = trace_instrument_impl(args, body).to_string();

        // then - a compile_error! macro invocation is emitted
        assert!(
            out.contains("compile_error"),
            "emits compile_error: {}",
            out
        );
    }
}
