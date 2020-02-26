# Common Index File Format (CIFF)

## What is CIFF?

Common Index File Format [CIFF](https://github.com/osirrc/ciff/) is an inverted index exchange format as defined as part of the *Open-Source IR Replicability Challenge (OSIRRC)* initiative. The primary idea is to allow indexes to be dumped from Lucene via [Anserini](https://github.com/castorini/anserini) which can then be ingested by other search engines. This repository contains the necessary code to read the CIFF into a format which PISA can use for building (and then searching) indexes.


## Versions
We currently provide both a Rust and a Python binary for converting CIFF data to a [PISA canonical index](https://pisa.readthedocs.io/en/latest/inverting.html#inverted-index-format).

## Dependencies 

- protocol buffers C++ library
- compiler for protocol buffer definition files




