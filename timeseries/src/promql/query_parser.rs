use serde::de::DeserializeOwned;

/// This helper enables reuse of existing `*Params` structs across endpoints
/// without duplicating parsing logic for repeated query keys.
pub fn parse_query_with_repeated<T>(
    raw_query: &str,
    repeated_key: &str,
) -> Result<(Vec<String>, T), serde_urlencoded::de::Error>
where
    T: DeserializeOwned,
{
    // Parse raw query into key-value pairs
    let params: Vec<(String, String)> = serde_urlencoded::from_str(raw_query).unwrap_or_default();

    // Collect repeated params (e.g. match[])
    let repeated: Vec<String> = params
        .iter()
        .filter(|(k, _)| k == repeated_key)
        .map(|(_, v)| v.clone())
        .collect();

    // Filter out repeated params
    let remaining: Vec<(String, String)> = params
        .into_iter()
        .filter(|(k, _)| k != repeated_key)
        .collect();

    // Deserialize remaining params into T
    let remaining_query = serde_urlencoded::to_string(remaining).unwrap_or_default();
    let other_params: T = serde_urlencoded::from_str(&remaining_query)?;

    Ok((repeated, other_params))
}

// TODO!
// Temporary Tests for the SeriesParams to test the helper function 'parse_query_with_repeated'
// is working as expected, will move this text back to 'SeriesParams' handler later.
#[cfg(test)]
mod series_tests {
    use crate::promql::query_parser::parse_query_with_repeated;
    use crate::promql::request::SeriesParams;

    #[test]
    fn parses_series_params_with_match_and_options() {
        let raw = concat!(
            "match%5B%5D=%7Bjob%3D%22api%22%7D&",
            "match%5B%5D=%7Binstance%3D%22host-1%22%7D&",
            "start=100&end=200&limit=10"
        );

        let (matches, mut params): (Vec<String>, SeriesParams) =
            parse_query_with_repeated(raw, "match[]").unwrap();

        params.matches = matches;

        assert_eq!(
            params.matches,
            vec![
                r#"{job="api"}"#.to_string(),
                r#"{instance="host-1"}"#.to_string(),
            ]
        );
        assert_eq!(params.start.as_deref(), Some("100"));
        assert_eq!(params.end.as_deref(), Some("200"));
        assert_eq!(params.limit, Some(10));
    }

    #[test]
    fn parses_series_params_without_match() {
        let raw = "start=50&limit=5";

        let (matches, mut params): (Vec<String>, SeriesParams) =
            parse_query_with_repeated(raw, "match[]").unwrap();

        params.matches = matches;

        assert!(params.matches.is_empty());
        assert_eq!(params.start.as_deref(), Some("50"));
        assert!(params.end.is_none());
        assert_eq!(params.limit, Some(5));
    }
}
