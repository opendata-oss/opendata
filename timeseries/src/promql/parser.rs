use promql_parser::parser::EvalStmt;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

use crate::promql::request::QueryRangeRequest;
use crate::promql::request::QueryRequest;
use common::Clock;

pub const DEFAULT_LOOKBACK_DURATION: Duration = Duration::from_secs(5 * 60);

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Could not parse query: {0}")]
    Query(String),
}

/// Trait for types that can be parsed into a PromQL EvalStmt
pub trait Parseable: Send + Sync {
    fn parse(&self, clock: Arc<dyn Clock>) -> Result<EvalStmt, ParseError>;
}

impl Parseable for QueryRequest {
    fn parse(&self, clock: Arc<dyn Clock>) -> Result<EvalStmt, ParseError> {
        let expr = promql_parser::parser::parse(&self.query).map_err(ParseError::Query)?;
        let time = self.time.unwrap_or_else(|| clock.now());

        let eval_stmt = EvalStmt {
            expr,
            start: time,
            end: time,
            interval: Duration::from_secs(0),
            lookback_delta: DEFAULT_LOOKBACK_DURATION,
        };

        Ok(eval_stmt)
    }
}

impl Parseable for QueryRangeRequest {
    fn parse(&self, _clock: Arc<dyn Clock>) -> Result<EvalStmt, ParseError> {
        let expr = promql_parser::parser::parse(&self.query).map_err(ParseError::Query)?;

        let eval_stmt = EvalStmt {
            expr,
            start: self.start,
            end: self.end,
            interval: self.step,
            lookback_delta: DEFAULT_LOOKBACK_DURATION,
        };

        Ok(eval_stmt)
    }
}

#[cfg(test)]
mod tests {
    use crate::promql::parser::{DEFAULT_LOOKBACK_DURATION, Parseable};
    use crate::promql::request::{QueryRangeRequest, QueryRequest};
    use chrono::DateTime;
    use common::clock::{Clock, MockClock, SystemClock};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    #[test]
    fn should_succeed_instant_query_with_datetime_format() {
        // Given
        let timestamp_str = "2025-10-10T12:39:19.781Z";
        let timestamp = SystemTime::from(DateTime::parse_from_rfc3339(timestamp_str).unwrap());
        let request = QueryRequest {
            query: "http_requests_total{}".to_string(),
            time: Some(timestamp),
            timeout: None,
        };

        // When
        let clock = Arc::new(SystemClock {});
        let result_parsed_query = request.parse(clock);

        // Then
        assert!(result_parsed_query.is_ok());
        let parsed_query = result_parsed_query.unwrap();
        assert_eq!(parsed_query.start, timestamp);
        assert_eq!(parsed_query.lookback_delta, DEFAULT_LOOKBACK_DURATION);
    }

    #[test]
    fn should_succeed_instant_query_with_unix_timestamp_str() {
        // Given
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(1234567.56);
        let request = QueryRequest {
            query: "http_requests_total{}".to_string(),
            time: Some(timestamp),
            timeout: None,
        };

        // When
        let clock = Arc::new(SystemClock {});
        let result_parsed_query = request.parse(clock);

        // Then
        assert!(result_parsed_query.is_ok());
        let parsed_query = result_parsed_query.unwrap();
        assert_eq!(parsed_query.start, timestamp);
        assert_eq!(parsed_query.lookback_delta, DEFAULT_LOOKBACK_DURATION);
    }

    #[test]
    fn should_succeed_instant_query_with_no_timestamp_str() {
        // Given
        let mock_system_time = Arc::new(MockClock::new());
        let request = QueryRequest {
            query: "http_requests_total{}".to_string(),
            time: None,
            timeout: None,
        };

        // When
        let result_parsed_query = request.parse(mock_system_time.clone());

        // Then
        assert!(result_parsed_query.is_ok());
        let parsed_query = result_parsed_query.unwrap();
        assert_eq!(parsed_query.start, mock_system_time.now());
        assert_eq!(parsed_query.lookback_delta, DEFAULT_LOOKBACK_DURATION);
    }

    #[test]
    fn should_fail_instant_query_with_invalid_query() {
        // Given
        let timestamp_str = "2025-10-10T12:39:19.781Z";
        let timestamp = SystemTime::from(DateTime::parse_from_rfc3339(timestamp_str).unwrap());
        let request = QueryRequest {
            query: "http_requests_total(}".to_string(),
            time: Some(timestamp),
            timeout: None,
        };

        // When
        let clock = Arc::new(SystemClock {});
        let result_parsed_query = request.parse(clock);

        // Then
        assert!(result_parsed_query.is_err());
        assert!(
            result_parsed_query
                .unwrap_err()
                .to_string()
                .starts_with("Could not parse query")
        );
    }

    #[test]
    fn should_succeed_range_query_with_datetime_format() {
        // Given
        let start_str = "2025-10-10T12:39:19.781Z";
        let end_str = "2025-10-10T13:39:19.781Z";
        let start = SystemTime::from(DateTime::parse_from_rfc3339(start_str).unwrap());
        let end = SystemTime::from(DateTime::parse_from_rfc3339(end_str).unwrap());
        let step = Duration::from_secs(60);
        let request = QueryRangeRequest {
            query: "http_requests_total{}".to_string(),
            start,
            end,
            step,
            timeout: None,
        };

        // When
        let clock = Arc::new(SystemClock {});
        let result_parsed_query = request.parse(clock);

        // Then
        assert!(result_parsed_query.is_ok());
        let parsed_query = result_parsed_query.unwrap();
        assert_eq!(parsed_query.start, start);
        assert_eq!(parsed_query.end, end);
        assert_eq!(parsed_query.interval, step);
        assert_eq!(parsed_query.lookback_delta, DEFAULT_LOOKBACK_DURATION);
    }

    #[test]
    fn should_succeed_range_query_with_float_step() {
        // Given
        let start_str = "2025-10-10T12:39:19.781Z";
        let end_str = "2025-10-10T13:39:19.781Z";
        let start = SystemTime::from(DateTime::parse_from_rfc3339(start_str).unwrap());
        let end = SystemTime::from(DateTime::parse_from_rfc3339(end_str).unwrap());
        let step = Duration::from_secs_f64(60.2);
        let request = QueryRangeRequest {
            query: "http_requests_total{}".to_string(),
            start,
            end,
            step,
            timeout: None,
        };

        // When
        let clock = Arc::new(SystemClock {});
        let result_parsed_query = request.parse(clock);

        // Then
        assert!(result_parsed_query.is_ok());
        let parsed_query = result_parsed_query.unwrap();
        assert_eq!(parsed_query.start, start);
        assert_eq!(parsed_query.end, end);
        assert_eq!(parsed_query.interval, step);
        assert_eq!(parsed_query.lookback_delta, DEFAULT_LOOKBACK_DURATION);
    }

    #[test]
    fn should_succeed_range_query_with_unix_timestamp() {
        // Given
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(1660140179.123);
        let end = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(1760140179.123);
        let step = Duration::from_secs(60);
        let request = QueryRangeRequest {
            query: "http_requests_total{}".to_string(),
            start,
            end,
            step,
            timeout: None,
        };

        // When
        let clock = Arc::new(SystemClock {});
        let result_parsed_query = request.parse(clock);

        // Then
        assert!(result_parsed_query.is_ok());
        let parsed_query = result_parsed_query.unwrap();
        assert_eq!(parsed_query.start, start);
        assert_eq!(parsed_query.end, end);
        assert_eq!(parsed_query.interval, step);
        assert_eq!(parsed_query.lookback_delta, DEFAULT_LOOKBACK_DURATION);
    }

    #[test]
    fn should_fail_range_query_with_invalid_query() {
        // Given
        let start_str = "2025-10-10T12:39:19.781Z";
        let end_str = "2025-10-10T13:39:19.781Z";
        let start = SystemTime::from(DateTime::parse_from_rfc3339(start_str).unwrap());
        let end = SystemTime::from(DateTime::parse_from_rfc3339(end_str).unwrap());
        let step = Duration::from_secs(60);
        let request = QueryRangeRequest {
            query: "http_requests_total{)".to_string(),
            start,
            end,
            step,
            timeout: None,
        };

        // When
        let clock = Arc::new(SystemClock {});
        let result_parsed_query = request.parse(clock);

        // Then
        assert!(result_parsed_query.is_err());
        assert!(
            result_parsed_query
                .unwrap_err()
                .to_string()
                .starts_with("Could not parse query")
        );
    }

    // This test verifies that the parser fails when the metric name is specified multiple times in
    // the selector. If the parser checks it, we do not have to check it again. This test ensures
    // changes in the external parser do not break the assumption that the parser takes care of
    // this
    #[test]
    fn should_fail_if_metric_name_is_set_twice() {
        // Given
        let timestamp_str = "2025-10-10T12:39:19.781Z";
        let timestamp = SystemTime::from(DateTime::parse_from_rfc3339(timestamp_str).unwrap());
        let request = QueryRequest {
            query: r#"http_requests_total{__name__="http_requests_total"}"#.to_string(),
            time: Some(timestamp),
            timeout: None,
        };

        // When
        let clock = Arc::new(SystemClock {});
        let result_parsed_query = request.parse(clock);

        // Then
        assert!(result_parsed_query.is_err());
        assert!(
            result_parsed_query
                .unwrap_err()
                .to_string()
                .contains("metric name must not be set twice:")
        );
    }
}
