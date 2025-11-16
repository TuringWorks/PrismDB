// Test with individual dependencies to isolate the issue

// Test 1: Just tokio
#[cfg(test)]
mod test_tokio {
    use tokio::runtime::Runtime;

    #[test]
    fn test_tokio_basic() {
        println!("Tokio test starting");
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Tokio async block");
        });
        println!("Tokio test passed");
    }
}

// Test 2: Just rayon
#[cfg(test)]
mod test_rayon {
    use rayon::prelude::*;

    #[test]
    fn test_rayon_basic() {
        println!("Rayon test starting");
        let data = vec![1, 2, 3, 4, 5];
        let sum: i32 = data.par_iter().sum();
        assert_eq!(sum, 15);
        println!("Rayon test passed");
    }
}

// Test 3: Just parking_lot
#[cfg(test)]
mod test_parking_lot {
    use parking_lot::Mutex;

    #[test]
    fn test_parking_lot_basic() {
        println!("Parking lot test starting");
        let mutex = Mutex::new(42);
        let guard = mutex.lock();
        assert_eq!(*guard, 42);
        println!("Parking lot test passed");
    }
}

// Test 4: All three together
#[cfg(test)]
mod test_all {
    use rayon::prelude::*;
    use parking_lot::Mutex;
    use tokio::runtime::Runtime;

    #[test]
    fn test_all_deps() {
        println!("All deps test starting");
        
        // Test parking_lot
        let mutex = Mutex::new(42);
        let guard = mutex.lock();
        assert_eq!(*guard, 42);
        drop(guard);
        
        // Test rayon
        let data = vec![1, 2, 3, 4, 5];
        let sum: i32 = data.par_iter().sum();
        assert_eq!(sum, 15);
        
        // Test tokio
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            println!("Async block in all deps test");
        });
        
        println!("All deps test passed");
    }
}