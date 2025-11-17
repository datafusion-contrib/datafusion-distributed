use base64::{Engine as _, engine::general_purpose::STANDARD};
use datafusion::{
    common::{internal_datafusion_err, internal_err},
    error::Result,
};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Create a seeded ChaCha8 RNG from a base64 string or generate a new random seed
pub fn rng(seed: Option<String>) -> Result<(ChaCha8Rng, String)> {
    let (rng, seed_b64) = if let Some(seed_str) = seed {
        // Use provided seed
        let seed_bytes = STANDARD
            .decode(&seed_str)
            .map_err(|e| internal_datafusion_err!("Invalid base64 seed: {}", e))?;

        if seed_bytes.len() != 32 {
            return internal_err!(
                "Seed must be 32 bytes (256 bits) when base64-decoded"
            );
        }

        let mut seed_array = [0u8; 32];
        seed_array.copy_from_slice(&seed_bytes);
        let rng = ChaCha8Rng::from_seed(seed_array);

        (rng, seed_str)
    } else {
        // Generate random seed
        let mut seed = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut seed);
        let seed_b64 = STANDARD.encode(seed);
        let rng = ChaCha8Rng::from_seed(seed);

        (rng, seed_b64)
    };

    Ok((rng, seed_b64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rng_with_seed() {
        // Create a known seed
        let seed_bytes = [42u8; 32];
        let seed_b64 = STANDARD.encode(seed_bytes);

        let (mut rng1, _) = rng(Some(seed_b64.clone())).unwrap();
        let (mut rng2, _) = rng(Some(seed_b64)).unwrap();

        // Both RNGs should generate the same sequence
        assert_eq!(rng1.next_u64(), rng2.next_u64());
        assert_eq!(rng1.next_u64(), rng2.next_u64());
    }
}
