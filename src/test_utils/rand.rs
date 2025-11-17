use base64::{Engine as _, engine::general_purpose::STANDARD};
use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::Result,
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::env;

/// Create a seeded ChaCha8 RNG from TEST_SEED environment variable or generate a new random seed
///
/// Checks for TEST_SEED environment variable (base64-encoded string). If not set, generates a random seed.
pub fn rng() -> Result<(ChaCha8Rng, String)> {
    rng_helper(env::var("TEST_SEED").ok())
}

pub fn rng_helper(seed: Option<String>) -> Result<(ChaCha8Rng, String)> {
    match seed {
        Some(seed_str) => {
            let seed_bytes = STANDARD
                .decode(&seed_str)
                .map_err(|e| internal_datafusion_err!("Invalid base64 seed in TEST_SEED: {}", e))?;

            if seed_bytes.len() != 32 {
                return internal_err!("TEST_SEED must be 32 bytes (256 bits) when base64-decoded");
            }

            let mut seed_array = [0u8; 32];
            seed_array.copy_from_slice(&seed_bytes);
            let rng = ChaCha8Rng::from_seed(seed_array);

            Ok((rng, seed_str))
        }
        None => {
            let mut seed = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut seed);
            let seed_b64 = STANDARD.encode(seed);
            let rng = ChaCha8Rng::from_seed(seed);
            Ok((rng, seed_b64))
        }
    }
}

#[cfg(all(test, feature = "integration"))]
mod tests {
    use super::*;

    #[test]
    fn test_rng_with_seed() {
        let seed_bytes = [42u8; 32];
        let seed_b64 = STANDARD.encode(seed_bytes);

        let (mut rng1, _) = rng_helper(Some(seed_b64.clone())).unwrap();
        let (mut rng2, _) = rng_helper(Some(seed_b64)).unwrap();

        // Both RNGs should generate the same sequence
        assert_eq!(rng1.next_u64(), rng2.next_u64());
        assert_eq!(rng1.next_u64(), rng2.next_u64());
    }
}
