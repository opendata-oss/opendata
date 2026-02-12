use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ============================================================================
// Command Types
// ============================================================================

#[derive(Debug, Clone)]
pub struct LoadCmd {
    pub interval: Duration,
    pub series: Vec<SeriesLoad>,
}

#[derive(Debug, Clone)]
pub struct EvalInstantCmd {
    pub time: SystemTime,
    pub query: String,
    pub expected: Vec<ExpectedSample>,
}

#[derive(Debug, Clone)]
pub struct ClearCmd;

#[derive(Debug, Clone)]
pub struct IgnoreCmd;

#[derive(Debug, Clone)]
pub struct ResumeCmd;

#[derive(Debug, Clone)]
pub enum Command {
    Load(LoadCmd),
    EvalInstant(EvalInstantCmd),
    Clear(ClearCmd),
    Ignore(IgnoreCmd),
    Resume(ResumeCmd),
}

// ============================================================================
// Data Structures
// ============================================================================

#[derive(Debug, Clone)]
pub struct SeriesLoad {
    pub labels: HashMap<String, String>, // includes __name__
    pub values: Vec<(i64, f64)>,         // (step_index, value)
}

#[derive(Debug, Clone)]
pub struct ExpectedSample {
    pub labels: HashMap<String, String>,
    pub value: f64,
    // Future:
    // #[cfg(feature = "histograms")]
    // pub histogram: Option<Histogram>,
    // #[cfg(feature = "range-vectors")]
    // pub range_values: Option<Vec<(i64, f64)>>,
}

#[derive(Debug, Clone)]
pub struct EvalResult {
    pub labels: HashMap<String, String>,
    pub value: f64,
}

// ============================================================================
// Parser
// ============================================================================

struct Parser;

impl Parser {
    /// Parse entire test file into commands
    pub fn parse_file(input: &str) -> Result<Vec<Command>, String> {
        let mut commands = Vec::new();
        let lines: Vec<&str> = input.lines().collect();
        let mut i = 0;

        while i < lines.len() {
            let line = lines[i].trim();
            i += 1;

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Try each parser dispatcher in order
            if let Some(cmd) = Self::try_parse_clear(line) {
                commands.push(cmd);
            } else if let Some(cmd) = Self::try_parse_ignore(line) {
                commands.push(cmd);
            } else if let Some(cmd) = Self::try_parse_resume(line) {
                commands.push(cmd);
            } else if let Some(cmd) = Self::try_parse_load(line, &lines, &mut i)? {
                commands.push(cmd);
            } else if let Some(cmd) = Self::try_parse_eval_instant(line, &lines, &mut i)? {
                commands.push(cmd);
            } else {
                return Err(format!("Unknown directive at line {}: {}", i, line));
            }
        }

        Ok(commands)
    }

    /// Try to parse "clear" command
    fn try_parse_clear(line: &str) -> Option<Command> {
        if line == "clear" {
            Some(Command::Clear(ClearCmd))
        } else {
            None
        }
    }

    /// Try to parse "ignore" command
    fn try_parse_ignore(line: &str) -> Option<Command> {
        if line == "ignore" {
            Some(Command::Ignore(IgnoreCmd))
        } else {
            None
        }
    }

    /// Try to parse "resume" command
    fn try_parse_resume(line: &str) -> Option<Command> {
        if line == "resume" {
            Some(Command::Resume(ResumeCmd))
        } else {
            None
        }
    }

    /// Try to parse "load" command (with indented series lines)
    fn try_parse_load(
        line: &str,
        lines: &[&str],
        i: &mut usize,
    ) -> Result<Option<Command>, String> {
        let rest = match line.strip_prefix("load ") {
            Some(r) => r,
            None => return Ok(None),
        };

        let interval = parse_duration(rest)?;
        let mut series = Vec::new();

        // Collect indented series lines
        while *i < lines.len() {
            let next_line = lines[*i];
            if !next_line.starts_with(' ') && !next_line.starts_with('\t') {
                break;
            }

            let trimmed = next_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                *i += 1;
                continue;
            }

            series.push(parse_series(trimmed)?);
            *i += 1;
        }

        Ok(Some(Command::Load(LoadCmd { interval, series })))
    }

    /// Try to parse "eval instant at" command (with indented expected results)
    fn try_parse_eval_instant(
        line: &str,
        lines: &[&str],
        i: &mut usize,
    ) -> Result<Option<Command>, String> {
        let rest = match line.strip_prefix("eval instant at ") {
            Some(r) => r,
            None => return Ok(None),
        };

        // Parse time and query from same line or next line
        let (time_str, query) = Self::parse_time_and_query(rest, lines, i)?;
        let time = parse_time(&time_str)?;

        let mut expected = Vec::new();

        // Collect indented expected result lines
        while *i < lines.len() {
            let next_line = lines[*i];
            if !next_line.starts_with(' ') && !next_line.starts_with('\t') {
                break;
            }

            let trimmed = next_line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                *i += 1;
                continue;
            }

            expected.push(parse_expected(trimmed)?);
            *i += 1;
        }

        Ok(Some(Command::EvalInstant(EvalInstantCmd {
            time,
            query,
            expected,
        })))
    }

    /// Parse time and query from "eval instant at" line
    /// Format: "eval instant at <time> <query>"
    /// Query can span to next line if on same line it's empty
    fn parse_time_and_query(
        rest: &str,
        lines: &[&str],
        i: &mut usize,
    ) -> Result<(String, String), String> {
        // Split on any whitespace (space, tab, etc.)
        // Note: This means "eval instant at 10s\t\tmetric" is valid, which could be
        // confusing if test files have inconsistent whitespace formatting
        let parts: Vec<&str> = rest.splitn(2, char::is_whitespace).collect();
        let time_str = parts[0];

        let query = if parts.len() > 1 && !parts[1].trim().is_empty() {
            parts[1].trim().to_string()
        } else {
            // Query on next line
            if *i < lines.len() {
                let query_line = lines[*i];
                *i += 1;
                query_line.trim().to_string()
            } else {
                return Err("Missing query after 'eval instant at'".to_string());
            }
        };

        Ok((time_str.to_string(), query))
    }
}

// ============================================================================
// Parsing Helpers (Series, Metrics, Values, Durations)
// ============================================================================

fn parse_series(line: &str) -> Result<SeriesLoad, String> {
    let mut chars = line.chars().peekable();
    let mut metric_part = String::new();

    // Read metric name (until { or whitespace)
    while let Some(&c) = chars.peek() {
        if c == '{' || c.is_whitespace() {
            break;
        }
        metric_part.push(chars.next().unwrap());
    }

    // Read label set if present
    if chars.peek() == Some(&'{') {
        let mut brace_depth = 0;
        for c in chars.by_ref() {
            metric_part.push(c);
            if c == '{' {
                brace_depth += 1;
            } else if c == '}' {
                brace_depth -= 1;
                if brace_depth == 0 {
                    break;
                }
            }
        }

        if brace_depth != 0 {
            return Err(format!("Unbalanced {{ }} in series: {}", line));
        }
    }

    let value_parts: String = chars.collect::<String>().trim().to_string();
    if value_parts.is_empty() {
        return Err(format!("Series missing values: {}", line));
    }

    let (metric, labels) = parse_metric(metric_part.trim())?;
    let values = parse_multiple_value_exprs(&value_parts)?;

    let mut all_labels = labels;
    if !metric.is_empty() {
        all_labels.insert("__name__".to_string(), metric);
    }

    Ok(SeriesLoad {
        labels: all_labels,
        values,
    })
}

fn parse_metric(s: &str) -> Result<(String, HashMap<String, String>), String> {
    if let Some((m, rest)) = s.split_once('{') {
        let rest = rest
            .strip_suffix('}')
            .ok_or_else(|| format!("Missing closing }} in metric: '{}'", s))?;
        let labels = parse_labels(rest, s)?;
        Ok((m.to_string(), labels))
    } else if s.starts_with('{') {
        // Label-only (no metric name, used in expected results)
        let rest = s
            .strip_prefix('{')
            .unwrap()
            .strip_suffix('}')
            .ok_or_else(|| format!("Missing closing }} in labels: '{}'", s))?;
        let labels = parse_labels(rest, s)?;
        Ok((String::new(), labels))
    } else {
        // Metric name only (no labels)
        Ok((s.to_string(), HashMap::new()))
    }
}

fn parse_labels(labels_str: &str, context: &str) -> Result<HashMap<String, String>, String> {
    let mut labels = HashMap::new();
    for kv in labels_str.split(',') {
        let kv = kv.trim();
        if kv.is_empty() {
            continue;
        }
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("Invalid label '{}' (missing =) in: '{}'", kv, context))?;
        labels.insert(k.to_string(), v.trim_matches('"').to_string());
    }
    Ok(labels)
}

fn parse_multiple_value_exprs(s: &str) -> Result<Vec<(i64, f64)>, String> {
    let mut all = Vec::new();
    let mut base_step = 0i64;

    // IMPORTANT (test-driver design decision):
    //
    // We treat multiple value expressions as sequential blocks to guarantee
    // monotonically increasing step indices at parse time.
    //
    // Example: "0+10x3 100+20x2" produces steps [0,1,2,3,4] not [0,1,2,0,1]
    //
    // Why this matters:
    // 1. Predictable behavior: Test authors see sequential timestamps
    // 2. Avoids confusion: Overlapping steps would be non-obvious
    // 3. Safety: Prevents accidental backwards timestamps
    //
    // Our runner also sorts/deduplicates before ingestion, so overlaps would
    // work. But we enforce sequential blocks for clarity and predictability.
    //
    // This is a test-driver constraint, not a PromQL semantic requirement.
    for part in s.split_whitespace() {
        let values = parse_values(part)?;
        for (step, value) in values {
            all.push((step + base_step, value));
        }
        base_step = all.len() as i64;
    }

    Ok(all)
}

fn parse_values(s: &str) -> Result<Vec<(i64, f64)>, String> {
    let s = s.trim();

    // Check for expansion syntax: "start+step x count"
    // Example: "0+10x100" → [0, 10, 20, ..., 990]
    if s.contains('+') && s.contains('x') {
        let (lhs, count_str) = s
            .split_once('x')
            .ok_or_else(|| format!("Invalid expansion syntax: {}", s))?;
        let (start_str, step_str) = lhs
            .split_once('+')
            .ok_or_else(|| format!("Invalid expansion syntax: {}", s))?;

        let start: f64 = start_str
            .parse()
            .map_err(|_| format!("Invalid start value: {}", start_str))?;
        let step: f64 = step_str
            .parse()
            .map_err(|_| format!("Invalid step value: {}", step_str))?;
        let count: usize = count_str
            .parse()
            .map_err(|_| format!("Invalid count: {}", count_str))?;

        Ok((0..count)
            .map(|i| (i as i64, start + step * i as f64))
            .collect())
    } else {
        // Space-separated individual values
        s.split_whitespace()
            .enumerate()
            .filter_map(|(i, v)| if v == "_" { None } else { Some((i as i64, v)) })
            .map(|(i, v)| {
                v.parse::<f64>()
                    .map(|f| (i, f))
                    .map_err(|_| format!("Invalid value '{}'", v))
            })
            .collect()
    }
}

fn parse_expected(line: &str) -> Result<ExpectedSample, String> {
    let mut chars = line.chars().peekable();
    let mut metric_part = String::new();

    // Read metric name if present (until { or whitespace)
    while let Some(&c) = chars.peek() {
        if c == '{' || c.is_whitespace() {
            break;
        }
        metric_part.push(chars.next().unwrap());
    }

    // Read full label set if present
    if chars.peek() == Some(&'{') {
        let mut brace_depth = 0;
        for c in chars.by_ref() {
            metric_part.push(c);
            if c == '{' {
                brace_depth += 1;
            } else if c == '}' {
                brace_depth -= 1;
                if brace_depth == 0 {
                    break;
                }
            }
        }

        if brace_depth != 0 {
            return Err(format!("Unbalanced {{ }} in expected: {}", line));
        }
    }

    let value_str: String = chars.collect::<String>().trim().to_string();
    if value_str.is_empty() {
        return Err(format!("Missing value in expected: {}", line));
    }

    let (_, labels) = parse_metric(metric_part.trim())?;
    let value = value_str
        .parse::<f64>()
        .map_err(|_| format!("Invalid value '{}' in expected: {}", value_str, line))?;

    Ok(ExpectedSample { labels, value })
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();

    // Try suffix-based parsing (strict: requires units)
    if let Some(ms) = s.strip_suffix("ms") {
        ms.parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| format!("Invalid duration {}: {}", s, e))
    } else if let Some(h) = s.strip_suffix('h') {
        h.parse::<u64>()
            .map(|v| Duration::from_secs(v * 3600))
            .map_err(|e| format!("Invalid duration {}: {}", s, e))
    } else if let Some(m) = s.strip_suffix('m') {
        m.parse::<u64>()
            .map(|v| Duration::from_secs(v * 60))
            .map_err(|e| format!("Invalid duration {}: {}", s, e))
    } else if let Some(sec) = s.strip_suffix('s') {
        sec.parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| format!("Invalid duration {}: {}", s, e))
    } else {
        Err(format!(
            "Invalid duration {}: missing unit (ms, s, m, h)",
            s
        ))
    }
}

fn parse_time(s: &str) -> Result<SystemTime, String> {
    // Matches durations with or without units
    // "10s", "5m", "100", "100.5" all valid

    // Try parsing as duration first (e.g., "10s", "5m")
    if let Ok(duration) = parse_duration(s) {
        return Ok(UNIX_EPOCH + duration);
    }

    // Fall back to unitless seconds (e.g., "100", "100.5")
    // This matches @ 100, @ 100s, and @ 1m40s are all equivalent
    match s.parse::<f64>() {
        Ok(secs) => Ok(UNIX_EPOCH + Duration::from_secs_f64(secs)),
        Err(_) => Err(format!(
            "Invalid time '{}': expected duration (10s, 5m) or seconds (100, 100.5)",
            s
        )),
    }
}

// ============================================================================
// Public API
// ============================================================================

pub fn parse_test_file(input: &str) -> Result<Vec<Command>, String> {
    Parser::parse_file(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_clear_command() {
        // given
        let input = "clear";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Clear(_)));
    }

    #[test]
    fn should_parse_ignore_command() {
        // given
        let input = "ignore";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Ignore(_)));
    }

    #[test]
    fn should_parse_resume_command() {
        // given
        let input = "resume";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], Command::Resume(_)));
    }

    #[test]
    fn should_parse_load_command() {
        // given
        let input = "load 10s\n  metric{job=\"1\"} 1 2 3";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Command::Load(cmd) => {
                assert_eq!(cmd.interval, Duration::from_secs(10));
                assert_eq!(cmd.series.len(), 1);
            }
            _ => panic!("Expected Load command"),
        }
    }

    #[test]
    fn should_parse_eval_instant_command() {
        // given
        let input = "eval instant at 10s metric\n  {job=\"1\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric");
                assert_eq!(cmd.expected.len(), 1);
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_parse_expansion_syntax() {
        // given
        let input = "0+10x5";

        // when
        let vals = parse_values(input).unwrap();

        // then
        assert_eq!(
            vals,
            vec![(0, 0.0), (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)]
        );
    }

    #[test]
    fn should_use_absolute_step_indices_for_multiple_expressions() {
        // given
        let input = "0+10x3 100+20x2";

        // when
        let vals = parse_multiple_value_exprs(input).unwrap();

        // then
        assert_eq!(
            vals,
            vec![(0, 0.0), (1, 10.0), (2, 20.0), (3, 100.0), (4, 120.0),]
        );
    }

    #[test]
    fn should_reject_invalid_values() {
        // given
        let input = "1 2 invalid 4";

        // when
        let result = parse_values(input);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid value 'invalid'"));
    }

    #[test]
    fn should_use_absolute_step_offsets_for_multiple_expressions() {
        // given
        let input = "0+10x3 100+5x3";

        // when
        let vals = parse_multiple_value_exprs(input).unwrap();

        // then
        assert_eq!(
            vals,
            vec![
                (0, 0.0),
                (1, 10.0),
                (2, 20.0),
                (3, 100.0),
                (4, 105.0),
                (5, 110.0),
            ]
        );
    }

    #[test]
    fn should_produce_sequential_steps_to_preserve_monotonic_timestamps() {
        // given - this demonstrates why we use sequential blocks
        let input = "0+10x3 0+20x2"; // Second expression also starts at step 0

        // when
        let vals = parse_multiple_value_exprs(input).unwrap();

        // then - we produce sequential steps [0,1,2,3,4] not overlapping [0,1,2,0,1]
        // This guarantees strictly increasing timestamps when steps are converted
        // to wall-clock time, preventing Gorilla/tsz encoder panics.
        assert_eq!(
            vals,
            vec![
                (0, 0.0),  // First expression
                (1, 10.0), // First expression
                (2, 20.0), // First expression
                (3, 0.0),  // Second expression (step 3, not 0)
                (4, 20.0), // Second expression (step 4, not 1)
            ]
        );

        // Without sequential blocks, this would produce:
        // [(0, 0.0), (1, 10.0), (2, 20.0), (0, 0.0), (1, 20.0)]
        // which has backwards timestamps after sorting
    }

    #[test]
    fn should_parse_duration_with_units() {
        // given / when / then
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(
            parse_duration("1000ms").unwrap(),
            Duration::from_millis(1000)
        );
    }

    #[test]
    fn should_parse_time_with_and_without_units() {
        // given
        let with_unit = "10s";
        let without_unit = "10";

        // when
        let t1 = parse_time(with_unit).unwrap();
        let t2 = parse_time(without_unit).unwrap();

        // then
        assert_eq!(t1, t2);
    }

    #[test]
    fn should_parse_metric_with_labels() {
        // given
        let input = "load 5m\n  metric{job=\"test\",instance=\"localhost\"} 1 2 3";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::Load(cmd) => {
                assert_eq!(
                    cmd.series[0].labels.get("__name__"),
                    Some(&"metric".to_string())
                );
                assert_eq!(cmd.series[0].labels.get("job"), Some(&"test".to_string()));
                assert_eq!(
                    cmd.series[0].labels.get("instance"),
                    Some(&"localhost".to_string())
                );
            }
            _ => panic!("Expected Load command"),
        }
    }

    #[test]
    fn should_parse_label_only_selector() {
        // given
        let input = "eval instant at 10s {job=\"test\"}\n  {job=\"test\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "{job=\"test\"}");
                assert_eq!(cmd.expected[0].labels.get("job"), Some(&"test".to_string()));
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_reject_unbalanced_braces() {
        // given
        let input = "load 5m\n  metric{job=\"test\" 1 2 3";

        // when
        let result = Parser::parse_file(input);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unbalanced"));
    }

    #[test]
    fn should_handle_multiple_tabs_between_time_and_query() {
        // given
        let input = "eval instant at 10s\t\tmetric\n  {job=\"test\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric");
                assert_eq!(cmd.time, UNIX_EPOCH + Duration::from_secs(10));
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }

    #[test]
    fn should_trim_leading_whitespace_from_query() {
        // given
        let input = "eval instant at 10s \t  metric{job=\"test\"}\n  {job=\"test\"} 5";

        // when
        let cmds = Parser::parse_file(input).unwrap();

        // then
        match &cmds[0] {
            Command::EvalInstant(cmd) => {
                assert_eq!(cmd.query, "metric{job=\"test\"}");
            }
            _ => panic!("Expected EvalInstant command"),
        }
    }
}
