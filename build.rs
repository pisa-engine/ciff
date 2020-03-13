//! Here, we generate Rust code from a proto file before project compilation.
use std::fs::{read_to_string, File};
use std::io::{BufWriter, Write};

fn main() {
    protobuf_codegen_pure::run(protobuf_codegen_pure::Args {
        out_dir: "src/proto",
        input: &["proto/common-index-format-v1.proto"],
        includes: &["proto"],
        customize: protobuf_codegen_pure::Customize {
            ..Default::default()
        },
    })
    .expect("protoc");
    let path = "src/proto/common_index_format_v1.rs";
    let code = read_to_string(path).expect("Failed to read generated file");
    let mut writer = BufWriter::new(File::create(path).unwrap());
    writer
        .write_fmt(format_args!("#![allow(clippy::pedantic)]\n"))
        .expect("Failed to write to generated file");
    writer
        .write(code.as_bytes())
        .expect("Failed to write to generated file");
}
