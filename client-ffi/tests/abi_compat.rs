#[test]
fn expected_exports_remain_declared() {
    let source = include_str!("../src/lib.rs");
    for symbol in include_str!("expected_symbols.txt")
        .lines()
        .filter(|v| !v.is_empty())
    {
        assert!(
            source.contains(&format!("fn {symbol}")),
            "missing ABI export {symbol}"
        );
    }
}
