//! Content-addressed digest types.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::DomainError;

macro_rules! digest_type {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("SHA-256 digest identifying a ", $kind, ".")]
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name([u8; 32]);

        impl $name {
            #[doc = concat!("Hash bytes into a ", $kind, " digest.")]
            #[must_use]
            pub fn hash(bytes: &[u8]) -> Self {
                Self(Sha256::digest(bytes).into())
            }

            #[doc = concat!("Parse a hexadecimal ", $kind, " digest.")]
            ///
            /// # Errors
            ///
            /// Returns an error when the value is not exactly 32 hexadecimal
            /// bytes.
            pub fn parse_hex(value: &str) -> Result<Self, DomainError> {
                let mut bytes = [0_u8; 32];
                hex::decode_to_slice(value, &mut bytes)
                    .map_err(|_| DomainError::InvalidDigest { kind: $kind })?;
                Ok(Self(bytes))
            }

            /// Return the lowercase hexadecimal representation.
            #[must_use]
            pub fn to_hex(self) -> String {
                hex::encode(self.0)
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.to_hex())
                    .finish()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.to_hex())
            }
        }
    };
}

digest_type!(SourceDigest, "source");
digest_type!(PayloadDigest, "payload");
digest_type!(ContextDigest, "context");
digest_type!(InvocationDigest, "tool invocation");
digest_type!(ActivityId, "semantic activity");
digest_type!(RiskId, "risk exposure");
digest_type!(PathSignature, "execution path");

#[cfg(test)]
mod tests {
    use super::SourceDigest;

    #[test]
    fn digest_round_trips_through_hex() -> Result<(), Box<dyn std::error::Error>> {
        let digest = SourceDigest::hash(b"verified source");
        let parsed = SourceDigest::parse_hex(&digest.to_hex())?;
        assert_eq!(digest, parsed);
        Ok(())
    }
}
