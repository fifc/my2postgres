fn main() {
    let hash1 = blake3::hash(b"hello,world!");
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"hello,");
    hasher.update(b"world!");
    let hash2 = hasher.finalize();
    assert_eq!(hash1, hash2);
    let hex = hash1.to_hex();
    println!("hash: {}, bits {}", hex, hex.len()*4);
}
