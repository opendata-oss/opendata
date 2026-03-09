use std::sync::{Arc, Mutex};

pub struct AtomicCell<T: Clone> {
    value: Arc<Mutex<T>>
}

impl <T: Clone> AtomicCell<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: Arc::new(Mutex::new(value))
        }
    }

    pub fn set(&self, value: T) {
        let mut guard = self.value.lock().expect("lock poisoned");
        *guard = value;
    }

    pub fn get(&self) -> T {
        let guard = self.value.lock().unwrap();
        guard.clone()
    }

    pub fn reader(&self) -> AtomicCellReader<T> {
        AtomicCellReader{ value: Arc::clone(&self.value) }
    }
}

pub struct AtomicCellReader<T: Clone> {
    value: Arc<Mutex<T>>
}

impl <T: Clone> AtomicCellReader<T> {
    pub fn get(&self) -> T {
        let guard = self.value.lock().unwrap();
        guard.clone()
    }
}