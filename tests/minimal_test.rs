//! Minimal test to isolate initialization issue

#[test]
fn test_minimal_initialization() {
    println!("Test starting");
    let x = 42;
    assert_eq!(x, 42);
    println!("Test completed");
}