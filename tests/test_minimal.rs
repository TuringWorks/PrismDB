// Test with NO dependencies - just basic Rust
fn test_no_deps() {
    println!("Test starting");
    assert_eq!(1 + 1, 2);
    println!("Test passed");
}

fn main() {
    println!("Main starting");
    test_no_deps();
    println!("Main finished");
}