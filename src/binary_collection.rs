use std::convert::TryFrom;
use std::convert::TryInto;
use std::error::Error;
use std::fmt;

const ELEMENT_SIZE: usize = std::mem::size_of::<u32>();

/// Error raised when the bytes cannot be properly parsed into the collection format.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct InvalidFormat(Option<String>);

impl InvalidFormat {
    /// Constructs an error with a message.
    pub fn new<S: Into<String>>(msg: S) -> Self {
        Self(Some(msg.into()))
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
#[derive(Debug, Clone, Copy)]
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

fn get_from(bytes: &[u8]) -> Result<BinarySequence<'_>, InvalidFormat> {
    let length_bytes = bytes
        .get(..ELEMENT_SIZE)
        .ok_or_else(InvalidFormat::default)?;
    let length = u32::from_le_bytes(length_bytes.try_into().unwrap()) as usize;
    let bytes = bytes
        .get(ELEMENT_SIZE..(ELEMENT_SIZE * (length + 1)))
        .ok_or_else(InvalidFormat::default)?;
    Ok(BinarySequence { bytes, length })
}

fn get_next<'a>(
    collection: &mut BinaryCollection<'a>,
) -> Result<BinarySequence<'a>, InvalidFormat> {
    let sequence = get_from(collection.bytes)?;
    collection.bytes = &collection.bytes[ELEMENT_SIZE * (sequence.len() + 1)..];
    Ok(sequence)
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

/// A version of [`BinaryCollection`] with random access to sequences.
///
/// Because the binary format underlying [`BinaryCollection`] does not
/// support random access, implementing it requires precomputing memory
/// offsets for the sequences, and storing them in the struct.
/// This means [`RandomAccessBinaryCollection::try_from`] will have to
/// perform one full pass through the entire collection to collect the
/// offsets. Thus, use this class only if you need the random access
/// funcionality.
///
/// Note that the because offsets are stored within the struct, it is
/// not `Copy` as opposed to [`BinaryCollection`], which is simply a view
/// over a memory buffer.
///
/// # Examples
///
/// ```
/// # use ciff::{encode_u32_sequence, RandomAccessBinaryCollection, InvalidFormat};
/// # use std::convert::TryFrom;
/// # fn main() -> Result<(), anyhow::Error> {
/// let mut buffer: Vec<u8> = Vec::new();
/// encode_u32_sequence(&mut buffer, 3, &[1, 2, 3])?;
/// encode_u32_sequence(&mut buffer, 1, &[4])?;
/// encode_u32_sequence(&mut buffer, 3, &[5, 6, 7])?;
///
/// let mut collection = RandomAccessBinaryCollection::try_from(&buffer[..])?;
/// assert_eq!(
///     collection.get(0).map(|seq| seq.iter().collect::<Vec<_>>()),
///     Some(vec![1_u32, 2, 3]),
/// );
/// assert_eq!(
///     collection.at(2).iter().collect::<Vec<_>>(),
///     vec![5_u32, 6, 7],
/// );
/// assert_eq!(collection.get(3), None);
/// # Ok(())
/// # }
/// ```
///
/// ```should_panic
/// # use ciff::{encode_u32_sequence, RandomAccessBinaryCollection, InvalidFormat};
/// # use std::convert::TryFrom;
/// # fn main() -> Result<(), anyhow::Error> {
/// # let mut buffer: Vec<u8> = Vec::new();
/// # encode_u32_sequence(&mut buffer, 3, &[1, 2, 3])?;
/// # encode_u32_sequence(&mut buffer, 1, &[4])?;
/// # encode_u32_sequence(&mut buffer, 3, &[5, 6, 7])?;
/// # let mut collection = RandomAccessBinaryCollection::try_from(&buffer[..])?;
/// collection.at(3); // out of bounds
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RandomAccessBinaryCollection<'a> {
    inner: BinaryCollection<'a>,
    offsets: Vec<usize>,
}

impl<'a> TryFrom<&'a [u8]> for RandomAccessBinaryCollection<'a> {
    type Error = InvalidFormat;
    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let collection = BinaryCollection::try_from(bytes)?;
        let offsets = collection
            .map(|sequence| sequence.map(|s| s.len()))
            .scan(0, |offset, len| {
                Some(len.map(|len| {
                    let result = *offset;
                    *offset += ELEMENT_SIZE * (len + 1);
                    result
                }))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            inner: collection,
            offsets,
        })
    }
}

impl<'a> RandomAccessBinaryCollection<'a> {
    /// Returns an iterator over sequences.
    pub fn iter(&self) -> impl Iterator<Item = Result<BinarySequence<'a>, InvalidFormat>> {
        self.inner
    }

    /// Returns the sequence at the given index.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds.
    #[must_use]
    pub fn at(&self, index: usize) -> BinarySequence<'a> {
        if let Some(sequence) = self.get(index) {
            sequence
        } else {
            panic!(
                "out of bounds: requested {} out of {} elements",
                index,
                self.len()
            );
        }
    }

    /// Returns the sequence at the given index or `None` if out of bounds.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<BinarySequence<'a>> {
        let byte_offset = *self.offsets.get(index)?;
        if let Ok(sequence) = get_from(self.inner.bytes.get(byte_offset..)?) {
            Some(sequence)
        } else {
            // The following case should be unreachable, because when constructing
            // the collection, we iterate through all sequences. Though there still
            // can be an error when iterating the sequence elements, the sequence
            // itself must be Ok.
            unreachable!()
        }
    }

    /// Returns the number of sequences in the collection.
    #[must_use]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Checks if the collection is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.offsets.len() == 0
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    const COLLECTION_BYTES: [u8; 100] = [
        1, 0, 0, 0, 3, 0, 0, 0, // Number of documents
        1, 0, 0, 0, 0, 0, 0, 0, // t0
        1, 0, 0, 0, 0, 0, 0, 0, // t1
        1, 0, 0, 0, 0, 0, 0, 0, // t2
        1, 0, 0, 0, 0, 0, 0, 0, // t3
        1, 0, 0, 0, 2, 0, 0, 0, // t4
        3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, // t5
        2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, // t6
        3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, // t7
        1, 0, 0, 0, 1, 0, 0, 0, // t8
    ];

    #[test]
    fn test_binary_sequence() {
        let bytes: Vec<u8> = (0_u32..10).flat_map(|i| i.to_le_bytes().to_vec()).collect();
        let sequence = BinarySequence::try_from(bytes.as_ref()).unwrap();
        assert!(!sequence.is_empty());
        for n in 0..10 {
            assert_eq!(sequence.get(n).unwrap(), n as u32);
        }
    }

    #[allow(clippy::needless_pass_by_value)]
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

    #[test]
    fn test_binary_collection() {
        let coll = BinaryCollection::try_from(COLLECTION_BYTES.as_ref()).unwrap();
        let sequences = coll
            .map(|sequence| {
                sequence.map(|sequence| (sequence.len(), sequence.iter().collect::<Vec<_>>()))
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            sequences,
            vec![
                (1, vec![3]),
                (1, vec![0]),
                (1, vec![0]),
                (1, vec![0]),
                (1, vec![0]),
                (1, vec![2]),
                (3, vec![0, 1, 2]),
                (2, vec![1, 2]),
                (3, vec![0, 1, 2]),
                (1, vec![1]),
            ]
        );
    }

    #[test]
    fn test_binary_collection_invalid_format() {
        let input: Vec<u8> = vec![1, 0, 0, 0, 3, 0, 0, 0, 1];
        let coll = BinaryCollection::try_from(input.as_ref());
        assert_eq!(
            coll.err(),
            Some(InvalidFormat::new(
                "The byte-length of the collection is not divisible by the element size (4)"
            ))
        );
    }

    #[test]
    fn test_random_access_binary_collection() {
        let coll = RandomAccessBinaryCollection::try_from(COLLECTION_BYTES.as_ref()).unwrap();
        assert!(!coll.is_empty());
        let sequences = coll
            .iter()
            .map(|sequence| {
                sequence.map(|sequence| (sequence.len(), sequence.iter().collect::<Vec<_>>()))
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            sequences,
            vec![
                (1, vec![3]),
                (1, vec![0]),
                (1, vec![0]),
                (1, vec![0]),
                (1, vec![0]),
                (1, vec![2]),
                (3, vec![0, 1, 2]),
                (2, vec![1, 2]),
                (3, vec![0, 1, 2]),
                (1, vec![1]),
            ]
        );
        assert_eq!(coll.offsets, vec![0, 8, 16, 24, 32, 40, 48, 64, 76, 92]);
        assert_eq!(coll.len(), 10);
        assert_eq!(
            (0..coll.len())
                .map(|idx| coll.at(idx).iter().collect())
                .collect::<Vec<Vec<u32>>>(),
            vec![
                vec![3],
                vec![0],
                vec![0],
                vec![0],
                vec![0],
                vec![2],
                vec![0, 1, 2],
                vec![1, 2],
                vec![0, 1, 2],
                vec![1],
            ]
        );
    }

    #[test]
    #[should_panic]
    fn test_random_access_binary_collection_out_of_bounds() {
        let coll = RandomAccessBinaryCollection::try_from(COLLECTION_BYTES.as_ref()).unwrap();
        let _ = coll.at(10);
    }
}
