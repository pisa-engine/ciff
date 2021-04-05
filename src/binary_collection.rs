use std::convert::TryFrom;
use std::convert::TryInto;
use std::error::Error;
use std::fmt;

/// Error raised when the bytes cannot be properly parsed into the collection format.
#[derive(Debug)]
pub struct InvalidFormat(Option<String>);

impl InvalidFormat {
    /// Constructs an error with a message.
    pub fn new<S: Into<String>>(msg: S) -> Self {
        Self(Some(msg.into()))
    }
}

impl Default for InvalidFormat {
    fn default() -> Self {
        Self(None)
    }
}

impl Error for InvalidFormat {}

impl fmt::Display for InvalidFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid binary collection format")?;
        if let Some(msg) = &self.0 {
            write!(f, ": {}", msg)?;
        }
        Ok(())
    }
}

/// Represents a single binary collection.
///
/// A binary collection is a series of sequences, each starting with a 4-byte length, followed by
/// this many 4-byte values.
///
/// # Examples
///
/// ```
/// # use ciff::{encode_u32_sequence, BinaryCollection, InvalidFormat};
/// # use std::convert::TryFrom;
/// # fn main() -> Result<(), anyhow::Error> {
/// let mut buffer: Vec<u8> = Vec::new();
/// encode_u32_sequence(&mut buffer, 3, &[1, 2, 3])?;
/// encode_u32_sequence(&mut buffer, 1, &[4])?;
/// encode_u32_sequence(&mut buffer, 3, &[5, 6, 7])?;
///
/// // Binary collection is actually an iterator
/// let mut collection = BinaryCollection::try_from(&buffer[..])?;
/// assert_eq!(
///     collection.next().unwrap().map(|seq| seq.iter().collect::<Vec<_>>()).ok(),
///     Some(vec![1_u32, 2, 3])
/// );
/// assert_eq!(
///     collection.next().unwrap().map(|seq| seq.iter().collect::<Vec<_>>()).ok(),
///     Some(vec![4_u32])
/// );
/// assert_eq!(
///     collection.next().unwrap().map(|seq| seq.iter().collect::<Vec<_>>()).ok(),
///     Some(vec![5_u32, 6, 7])
/// );
///
/// // Must create a new collection to iterate again.
/// let collection = BinaryCollection::try_from(&buffer[..])?;
/// let elements: Result<Vec<_>, InvalidFormat> = collection
///     .map(|sequence| Ok(sequence?.iter().collect::<Vec<_>>()))
///     .collect();
/// assert_eq!(elements?, vec![vec![1_u32, 2, 3], vec![4], vec![5, 6, 7]]);
/// # Ok(())
/// # }
/// ```
pub struct BinaryCollection<'a> {
    bytes: &'a [u8],
}

impl<'a> TryFrom<&'a [u8]> for BinaryCollection<'a> {
    type Error = InvalidFormat;
    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() % std::mem::size_of::<u32>() == 0 {
            Ok(Self { bytes })
        } else {
            Err(InvalidFormat::new(
                "The byte-length of the collection is not divisible by the element size (4)",
            ))
        }
    }
}

fn get_next<'a>(
    collection: &mut BinaryCollection<'a>,
) -> Result<BinarySequence<'a>, InvalidFormat> {
    const ELEMENT_SIZE: usize = std::mem::size_of::<u32>();
    let length_bytes = collection
        .bytes
        .get(..ELEMENT_SIZE)
        .ok_or_else(InvalidFormat::default)?;
    let length = u32::from_le_bytes(length_bytes.try_into().unwrap()) as usize;
    let bytes = collection
        .bytes
        .get(ELEMENT_SIZE..(ELEMENT_SIZE * (length + 1)))
        .ok_or_else(InvalidFormat::default)?;
    collection.bytes = &collection.bytes[length_bytes.len() + bytes.len()..];
    Ok(BinarySequence { length, bytes })
}

impl<'a> Iterator for BinaryCollection<'a> {
    type Item = Result<BinarySequence<'a>, InvalidFormat>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.is_empty() {
            None
        } else {
            Some(get_next(self))
        }
    }
}

/// A single binary sequence.
///
/// # Examples
///
/// ```
/// # use ciff::BinarySequence;
/// # use std::convert::TryFrom;
/// # fn main() -> Result<(), ()> {
/// let bytes: [u8; 16] = [1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0];
/// let sequence = BinarySequence::try_from(&bytes[..])?;
/// assert_eq!(sequence.len(), 4);
/// assert_eq!(sequence.get(0), Some(1));
/// assert_eq!(sequence.get(1), Some(2));
/// assert_eq!(sequence.get(2), Some(3));
/// assert_eq!(sequence.get(3), Some(4));
/// let elements: Vec<_> = sequence.iter().collect();
/// assert_eq!(elements, vec![1_u32, 2, 3, 4]);
/// # Ok(())
/// # }
/// ```
pub struct BinarySequence<'a> {
    /// All bytes, **excluding** the length bytes.
    bytes: &'a [u8],
    /// Length extracted from the first 4 bytes of the sequence.
    length: usize,
}

impl<'a> TryFrom<&'a [u8]> for BinarySequence<'a> {
    type Error = ();
    /// Tries to construct a binary sequence from a slice of bytes.
    ///
    /// # Errors
    ///
    /// It will fail if the length of the slice is not divisible by 4.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ciff::BinarySequence;
    /// # use std::convert::TryFrom;
    /// # fn main() -> Result<(), ()> {
    /// let bytes: [u8; 8] = [1, 0, 0, 0, 2, 0, 0, 0];
    /// let sequence = BinarySequence::try_from(&bytes[..])?;
    /// assert_eq!(sequence.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() % std::mem::size_of::<u32>() == 0 {
            let length = bytes.len() / std::mem::size_of::<u32>();
            Ok(Self { bytes, length })
        } else {
            Err(())
        }
    }
}

/// # Safety
///
/// The length of `bytes` must be 4.
unsafe fn bytes_to_u32(bytes: &[u8]) -> u32 {
    let mut value: std::mem::MaybeUninit<[u8; 4]> = std::mem::MaybeUninit::uninit();
    value
        .as_mut_ptr()
        .copy_from_nonoverlapping(bytes.as_ptr().cast(), 1);
    u32::from_le_bytes(value.assume_init())
}

impl<'a> BinarySequence<'a> {
    /// Returns the number of elements in the sequence.
    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Checks if the sequence is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns `index`-th element of the sequence or `None` if `index` is out of bounds.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<u32> {
        if index < self.len() {
            let offset = index * std::mem::size_of::<u32>();
            self.bytes.get(offset..offset + 4).map(|bytes| {
                // SAFETY: it is safe because if `get` returns `Some`, the slice must be of length 4.
                unsafe { bytes_to_u32(bytes) }
            })
        } else {
            None
        }
    }

    /// An iterator over all sequence elements.
    #[must_use]
    pub fn iter(&'a self) -> BinarySequenceIterator<'a> {
        BinarySequenceIterator {
            sequence: self,
            index: 0,
        }
    }

    /// Returns the byte slice of the sequence. This **does not** include the length.
    #[must_use]
    pub fn bytes(&'a self) -> &'a [u8] {
        self.bytes
    }
}

pub struct BinarySequenceIterator<'a> {
    sequence: &'a BinarySequence<'a>,
    index: usize,
}

impl<'a> Iterator for BinarySequenceIterator<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;
        self.sequence.get(index)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[test]
    fn test_binary_sequence() {
        let bytes: Vec<u8> = (0_u32..10).flat_map(|i| i.to_le_bytes().to_vec()).collect();
        let sequence = BinarySequence {
            bytes: &bytes,
            length: 10,
        };
        for n in 0..10 {
            assert_eq!(sequence.get(n).unwrap(), n as u32);
        }
    }

    #[quickcheck]
    fn biniary_sequence_get_never_crashes(bytes: Vec<u8>, indices: Vec<usize>) {
        let sequence = BinarySequence {
            bytes: &bytes,
            length: bytes.len() / 4,
        };
        for idx in indices {
            let _ = sequence.get(idx);
        }
    }
}
