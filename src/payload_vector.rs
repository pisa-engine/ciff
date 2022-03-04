use std::convert::TryInto;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::ops::{Deref, Index};
use std::path::Path;

/// Owning variant of [`PayloadSlice`], in which the underlying bytes are fully
/// in memory within the struct. This is useful mainly for building the structure
/// before writing it to a file, but also if one decides to fully load the bytes
/// to memory and use it to assess elements without parsing the whole vector
/// to a `Vec`.
///
/// `PayloadVector` implements `Deref<Target = PayloadSlice>`. See [`PayloadSlice`]
/// for all the methods supported through dereferencing.
#[derive(Debug, Clone)]
pub struct PayloadVector {
    data: Vec<u8>,
}

impl AsRef<[u8]> for PayloadVector {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsRef<PayloadSlice> for PayloadVector {
    fn as_ref(&self) -> &PayloadSlice {
        &*self
    }
}

impl<Item> std::iter::FromIterator<Item> for PayloadVector
where
    Item: AsRef<[u8]>,
{
    fn from_iter<T: IntoIterator<Item = Item>>(iter: T) -> Self {
        let mut data = Vec::<u8>::new();
        let mut length: u64 = 0;

        // Write empty bytes that will be modified once length is known.
        data.extend(length.to_le_bytes());

        // Must collect separately, to later append to `data`.
        let mut payloads = Vec::<u8>::new();
        let mut offset: u64 = 0;

        data.extend(offset.to_le_bytes());
        for item in iter {
            let bytes: &[u8] = item.as_ref();
            payloads.extend(bytes);
            offset += bytes.len() as u64;
            length += 1;
            data.extend(offset.to_le_bytes());
        }

        data.extend(payloads);
        data[..std::mem::size_of::<u64>()].copy_from_slice(&length.to_le_bytes());

        Self { data }
    }
}

impl<'a> Deref for PayloadVector {
    type Target = PayloadSlice;
    fn deref(&self) -> &Self::Target {
        let data: *const [u8] = &*self.data;
        let data = data as *const PayloadSlice;
        // SAFETY: PayloadSlice just wraps [u8],
        // and &*self.data is &[u8], therefore
        // transmuting &[u8] to &PayloadSlice is safe.
        unsafe { &*data }
    }
}

/// Payload slice is a slice of variable-sized elements (payloads) encoded in
/// a single block of memory. This way, sequences of, say, strings, can be indexed
/// into without loading all the elements in memory, but rather using a memory
/// mapped buffer.
///
/// # Use case
///
/// The primary use case of this struct is not necessarily to limit the bytes
/// that are loaded in memory, but rather to limit the time to initialize it.
/// For example, one can use it in a command line program to quickly look up
/// one or several values from a file-encoded vector, without the overhead of
/// parsing the entire file and loading it in memory.
///
/// # Examples
///
/// ```
/// # use std::fs::File;
/// # use ciff::{PayloadSlice, PayloadVector};
/// # use tempfile::TempDir;
/// use memmap::Mmap;
/// # fn main() -> anyhow::Result<()> {
///
/// // We will store out vector to a temporary directory
/// let temp = TempDir::new()?;
/// let file_path = temp.path().join("words.bin");
///
/// // We can use any elements that implement `AsRef<[u8]>`.
/// let words = vec!["dog", "cat", "gnu"];
///
/// // One way of encoding is to collect elements to a PayloadVector in memory
/// let payloads: PayloadVector = words.into_iter().collect();
///
/// // Write to file
/// let mut output = File::create(&file_path)?;
/// payloads.write(&mut output);
/// drop(output);
///
/// // Load as payload slice
/// let input = File::open(&file_path)?;
/// let bytes = unsafe { Mmap::map(&input)? };
/// let payloads = PayloadSlice::new(&bytes);
///
/// // Note that it returns byte slices.
/// assert_eq!(&payloads[0], b"dog");
/// assert_eq!(&payloads[1], b"cat");
/// assert_eq!(&payloads[2], b"gnu");
///
/// // Non-panicing access.
/// assert_eq!(payloads.get(3), None);
///
/// // Collect to a vector of strings
/// let items: Vec<_> = payloads
///     .iter()
///     .map(|b| String::from_utf8(b.to_vec()).unwrap())
///     .collect();
/// assert_eq!(items, vec![
///     "dog".to_string(),
///     "cat".to_string(),
///     "gnu".to_string()
/// ]);
///
/// # Ok(())
/// # }
/// ```
#[repr(transparent)]
pub struct PayloadSlice {
    data: [u8],
}

impl AsRef<PayloadSlice> for PayloadSlice {
    fn as_ref(&self) -> &PayloadSlice {
        self
    }
}

impl Index<usize> for &'_ PayloadSlice {
    type Output = [u8];
    fn index(&self, index: usize) -> &Self::Output {
        if let Some(payload) = self.get(index as u64) {
            payload
        } else {
            panic!("index out of bounds: {}", index)
        }
    }
}

impl Index<usize> for PayloadVector {
    type Output = [u8];
    fn index(&self, index: usize) -> &Self::Output {
        if let Some(payload) = self.get(index as u64) {
            payload
        } else {
            panic!("index out of bounds: {}", index)
        }
    }
}

impl PayloadSlice {
    /// Conctructs a new slice using the given underlying data.
    #[must_use]
    pub fn new(data: &[u8]) -> &Self {
        let data: *const [u8] = &*data;
        let data = data as *const PayloadSlice;
        // SAFETY: PayloadSlice just wraps [u8],
        // and &*data is &[u8], therefore
        // transmuting &[u8] to &PayloadSlice is safe.
        unsafe { &*data }
    }

    /// Writes the underlying memory to the output.
    ///
    /// # Errors
    ///
    /// Will return an error if an error occurs while writing to the output.
    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.data)?;
        writer.flush()
    }

    /// Returns the element at position `index` or `None` if `index` is out of bounds.
    #[must_use]
    pub fn get(&self, index: u64) -> Option<&[u8]> {
        if index >= self.len() {
            None
        } else {
            let payloads_offset = (self.len() as usize + 2) * 8;
            let offset_pos = (index as usize + 1) * 8;
            let offset = payloads_offset + self.int_at(offset_pos) as usize;
            let next_offset = payloads_offset + self.int_at(offset_pos + 8) as usize;
            self.data.get(offset..next_offset)
        }
    }

    /// Returns the length of the slice.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.int_at(0)
    }

    /// Checks if the slice is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the iterator over all items.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        PayloadIter {
            slice: self,
            index: 0,
            length: self.len(),
        }
    }

    fn int_at(&self, offset: usize) -> u64 {
        u64::from_le_bytes(self.data[offset..offset + 8].try_into().unwrap())
    }
}

/// Iterator over [`PayloadSlice`].
pub struct PayloadIter<'a> {
    slice: &'a PayloadSlice,
    index: u64,
    length: u64,
}

impl<'a> Iterator for PayloadIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.length {
            None
        } else {
            let item = self.slice.get(self.index);
            self.index += 1;
            item
        }
    }
}

pub fn build_lexicon(input: &Path, output: &Path) -> io::Result<()> {
    let lex = BufReader::new(File::open(input)?)
        .lines()
        .collect::<Result<PayloadVector, _>>()?;
    let mut lex_path = BufWriter::new(File::create(output)?);
    lex.write(&mut lex_path)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io;

    #[test]
    #[cfg(not(miri))]
    fn test_write() -> io::Result<()> {
        let test_data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test_data");
        let lex: PayloadVector = std::fs::read_to_string(test_data_dir.join("terms.txt"))?
            .trim()
            .split_whitespace()
            .map(str::to_string)
            .collect();
        let mut output = Vec::<u8>::new();
        let expected_lex_bytes = std::fs::read(test_data_dir.join("terms.lex"))?;
        lex.write(&mut output)?;
        assert_eq!(output, expected_lex_bytes);
        Ok(())
    }

    #[test]
    #[cfg(not(miri))]
    fn test_elements() -> io::Result<()> {
        let test_data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test_data");
        let lex: PayloadVector = std::fs::read_to_string(test_data_dir.join("terms.txt"))?
            .trim()
            .split_whitespace()
            .map(str::to_string)
            .collect();

        assert_eq!(lex.get(0), Some(b"aardvark".as_ref()));
        assert_eq!(lex.get(1), Some(b"cat".as_ref()));
        assert_eq!(lex.get(2), Some(b"dog".as_ref()));
        assert_eq!(lex.get(3), Some(b"gnu".as_ref()));
        assert_eq!(lex.get(4), Some(b"mouse".as_ref()));
        assert_eq!(lex.get(5), Some(b"zebra".as_ref()));

        assert_eq!(&lex[0], b"aardvark".as_ref());
        assert_eq!(&lex[1], b"cat".as_ref());
        assert_eq!(&lex[2], b"dog".as_ref());
        assert_eq!(&lex[3], b"gnu".as_ref());
        assert_eq!(&lex[4], b"mouse".as_ref());
        assert_eq!(&lex[5], b"zebra".as_ref());

        let expected = vec![
            b"aardvark".as_ref(),
            b"cat".as_ref(),
            b"dog".as_ref(),
            b"gnu".as_ref(),
            b"mouse".as_ref(),
            b"zebra".as_ref(),
        ];
        assert_eq!(lex.iter().collect::<Vec<_>>(), expected);

        Ok(())
    }

    fn assert_payloads<L: AsRef<PayloadSlice>>(lex: L, payloads: &[&[u8]]) {
        let lex = lex.as_ref();
        assert!(!lex.is_empty());
        for (idx, payload) in payloads.iter().enumerate() {
            assert_eq!(lex.get(idx as u64), Some(*payload));
            assert_eq!(&lex[idx], *payload);
        }
        assert!(lex.get(6).is_none());
        assert_eq!(lex.iter().collect::<Vec<_>>(), payloads);
    }

    #[test]
    fn test_element_access() {
        let payloads = vec![
            b"aardvark".as_ref(),
            b"cat".as_ref(),
            b"dog".as_ref(),
            b"gnu".as_ref(),
            b"mouse".as_ref(),
            b"zebra".as_ref(),
        ];
        let lex: PayloadVector = payloads
            .iter()
            .map(|&b| String::from_utf8(Vec::from(b)).unwrap())
            .collect();
        assert_payloads(&lex, &payloads);
        assert_payloads(PayloadSlice::new(lex.as_ref()), &payloads);
    }
}
