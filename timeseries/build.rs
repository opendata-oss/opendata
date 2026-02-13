use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let testdata_dir = Path::new("src/promql/promqltest/testdata");
    println!("cargo:rerun-if-changed={}", testdata_dir.display());
    println!("cargo:rerun-if-changed=ui");

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest = Path::new(&out_dir).join("promql_tests_generated.rs");

    let mut code = String::new();

    if testdata_dir.exists() {
        let mut entries: Vec<_> = fs::read_dir(testdata_dir)
            .expect("failed to read testdata directory")
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s == "test")
                    .unwrap_or(false)
            })
            .collect();

        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let path = entry.path();
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("invalid test filename");

            let fn_name = stem.replace('-', "_");

            code.push_str(&format!(
                r#"
#[tokio::test]
async fn should_pass_{fn_name}() {{
    run_test("{stem}", include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/promql/promqltest/testdata/{stem}.test")))
        .await
        .unwrap();
}}
"#,
            ));
        }
    }

    fs::write(&dest, code).expect("failed to write generated test file");
}
