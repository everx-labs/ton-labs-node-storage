use ton_types::types::UInt256;

/// Trait for database key
pub trait DbKey {
    fn key(&self) -> &[u8];
}

impl DbKey for &[u8] {
    fn key(&self) -> &[u8] {
        self
    }
}

impl DbKey for &str {
    fn key(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl DbKey for UInt256 {
    fn key(&self) -> &[u8] {
        self.as_slice()
    }
}