//! Test to isolate initialization issue

#[test]
fn test_static_initialization() {
    println!("Test starting");
    // This should help us identify if there's a static initialization issue
    assert!(true);
}