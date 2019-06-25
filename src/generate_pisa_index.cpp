#include <fstream>


#include "google/protobuf/io/coded_stream.h"
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "common-index-format.pb.h"
#include "CLI/CLI.hpp"
#include "gsl/span"




template <typename T>
std::ostream &write_sequence(std::ostream &os, gsl::span<T> sequence)
{
    auto length = static_cast<uint32_t>(sequence.size());
    os.write(reinterpret_cast<const char *>(&length), sizeof(length));
    os.write(reinterpret_cast<const char *>(sequence.data()), length * sizeof(T));
    return os;
}


int main(int argc, char const *argv[])
{
    std::string postings_filename;
    std::string output_basename;
    
    CLI::App app{"generate_pisa_index - a tool for generating a PISA index from a common index format."};
    app.add_option("-p,--postings", postings_filename, "Postings filename")->required();
    app.add_option("-o,--output", output_basename, "Output basename")->required();
    CLI11_PARSE(app, argc, argv);

    std::ofstream dstream(output_basename + ".docs");
    std::ofstream fstream(output_basename + ".freqs");
    std::ofstream sstream(output_basename + ".sizes");


}
