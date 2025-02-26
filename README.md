# Common Index File Format (CIFF)

[![Rust](https://github.com/pisa-engine/ciff/workflows/Rust/badge.svg)](https://github.com/pisa-engine/ciff/actions?query=workflow%3ARust)
[![License](https://img.shields.io/github/license/pisa-engine/ciff)](https://github.com/pisa-engine/ciff/blob/master/LICENSE)
[![crates.io](https://img.shields.io/crates/v/ciff)](https://crates.io/crates/ciff)
[![API](https://docs.rs/ciff/badge.svg)](https://docs.rs/ciff)

## What is CIFF?

Common Index File Format [CIFF](https://github.com/osirrc/ciff/) is an inverted index exchange format as defined as part of the *Open-Source IR Replicability Challenge (OSIRRC)* initiative. The primary idea is to allow indexes to be dumped from Lucene via [Anserini](https://github.com/castorini/anserini) which can then be ingested by other search engines. This repository contains the necessary code to read the CIFF into a format which PISA can use for building (and then searching) indexes.

## Versions
We currently provide a Rust binary for converting CIFF data to a [PISA canonical index](https://pisa.readthedocs.io/en/latest/inverting.html#inverted-index-format), and for converting a PISA canonical index back to CIFF. This means PISA can generate indexes that can then be consumed by other systems that support CIFF (and vice versa).

## Install from AUR

The package is available in [Arch User Repository](https://aur.archlinux.org/packages/ciff-pisa/).
If you are on an Arch-based system, you can install it by running the following:

```bash
# Replace yay with the helper of your choice.
yay -S ciff-pisa
```

## Install from crates.io

> Note that the installation methods described below **are not** system-wide.
> For example, on Linux the tools usually end up in `$HOME/.cargo/bin` directory.
> To use tools from command line, make sure to use the absolute path or update
> your `PATH` variable to include the `$HOME/.cargo/bin` directory.

The library and the tools are also available in crates.io, so you can install the binaries in your local repository by running:

```
cargo install ciff
```

## Install from source

### Build locally

Just run `cargo build --release` to build the binaries. 

To convert a CIFF blob to a PISA canonical:
`./target/release/ciff2pisa`

To convert a PISA canonical to a CIFF blob:
`./target/release/pisa2ciff`

To convert a Jsonl file to a CIFF blob:
`./target/release/jsonl2ciff`

Documents should have the following format. Each line should be a JSON-formatted string with the following fields:

`id`: must represent the ID of the document.
`content`: the original content of the document, as a string. This field is optional.
`vector`: a dictionary where each key represents a token, and its corresponding value is the quantized score, e.g., {"ciff": 5}.

### Install

You can also install the binaries to your local `cargo` repository:

```
cargo install --path .
```

or if you are installing the same version again:

```
cargo install --path . --force
```

## Use as Cargo dependency

If you are insterested in using the library components in your own Rust library, you can simply defeine it as a dependency in your `Cargo.toml` file:

```toml
[dependencies]
ciff = "0.1"
```

## Library API documentation

The API documentation is available on [docs.rs](https://docs.rs/ciff).
