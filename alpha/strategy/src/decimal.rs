//! Exact fixed-point decimal parser for the host wire contract.
//!
//! No binary floating point is used. Downstream portfolio/ledger own Decimal
//! arithmetic; this parser enforces representability before handoff without
//! changing the workspace Cargo.lock.

use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Decimal {
    mantissa: i128,
    scale: u32,
}

impl Decimal {
    pub(crate) const ZERO: Self = Self {
        mantissa: 0,
        scale: 0,
    };

    pub(crate) fn scale(self) -> u32 {
        self.scale
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Only comparison to ZERO is needed at this trust boundary.
        if other.mantissa == 0 {
            Some(self.mantissa.cmp(&0))
        } else if self.scale == other.scale {
            Some(self.mantissa.cmp(&other.mantissa))
        } else {
            None
        }
    }
}

impl FromStr for Decimal {
    type Err = ();
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.is_empty() || input.len() > 64 {
            return Err(());
        }
        let (sign, unsigned) = match input.as_bytes()[0] {
            b'-' => (-1_i128, &input[1..]),
            b'+' => (1, &input[1..]),
            _ => (1, input),
        };
        if unsigned.is_empty() {
            return Err(());
        }
        let mut point = false;
        let mut digits = 0;
        let mut scale = 0_u32;
        let mut mantissa = 0_i128;
        for byte in unsigned.bytes() {
            if byte == b'.' && !point {
                point = true;
                continue;
            }
            if !byte.is_ascii_digit() {
                return Err(());
            }
            digits += 1;
            if digits > 28 {
                return Err(());
            }
            mantissa = mantissa
                .checked_mul(10)
                .and_then(|n| n.checked_add(i128::from(byte - b'0')))
                .ok_or(())?;
            if point {
                scale += 1;
            }
        }
        if digits == 0 || (point && unsigned.ends_with('.')) {
            return Err(());
        }
        Ok(Self {
            mantissa: mantissa * sign,
            scale,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finite_exact_fixed_point_only() {
        assert_eq!(Decimal::from_str("0.00000001").unwrap().scale(), 8);
        assert!(Decimal::from_str("0.00000001").unwrap() > Decimal::ZERO);
        assert!(Decimal::from_str("-0.1").unwrap() < Decimal::ZERO);
        assert!(Decimal::from_str("-0").unwrap() == Decimal::ZERO);
        for value in [
            "NaN",
            "inf",
            "1e-8",
            "1.2.3",
            ".",
            "1.",
            "",
            "１２",
            "100000000000000000000000000000",
        ] {
            assert!(Decimal::from_str(value).is_err(), "{value}");
        }
    }
}
