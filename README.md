# Common Index File Format (CIFF)

[![Rust](https://github.com/pisa-engine/ciff/workflows/Rust/badge.svg)](https://github.com/pisa-engine/ciff/actions?query=workflow%3ARust)
[![License](https://img.shields.io/github/license/pisa-engine/ciff)](https://github.com/pisa-engine/ciff/blob/master/LICENSE)
[![crates.io](https://img.shields.io/crates/v/ciff)](https://crates.io/crates/ciff)
[![API](https://docs.rs/ciff/badge.svg)](https://docs.rs/ciff)

## What is CIFF?

Common Index File Format [CIFF](https://github.com/osirrc/ciff/) is an inverted index exchange format as defined as part of the *Open-Source IR Replicability Challenge (OSIRRC)* initiative. The primary idea is to allow indexes to be dumped from Lucene via [Anserini](https://github.com/castorini/anserini) which can then be ingested by other search engines. This repository contains the necessary code to read the CIFF into a format which PISA can use for building (and then searching) indexes.


## Versions
We currently provide a Rust binary for converting CIFF data to a [PISA canonical index](https://pisa.readthedocs.io/en/latest/inverting.html#inverted-index-format), and for converting a PISA canonical index back to CIFF. This means PISA can generate indexes that can then be consumed by other systems that support CIFF (and vice versa).


## Build

Just run `cargo build --release` to build the binaries. 

To convert a CIFF blob to a PISA canonical:
`./target/release/ciff2pisa`

To convert a PISA canonical to a CIFF blob:
`./target/release/pisa2ciff`

