#include <fstream>


#include "google/protobuf/io/coded_stream.h"
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/util/delimited_message_util.h>
#include "common-index-format.pb.h"
#include "CLI/CLI.hpp"
#include "gsl/span"

using namespace cif;

void dump_postings_list(const PostingsList& postings_list) {
    std::cerr << "Term = [" << postings_list.term() << "]" << std::endl;
    std::cerr << "Document Frequency/Collection Term Frequency = [" 
              << postings_list.df() << "," << postings_list.cf() << "]" 
              << std::endl;
   
    int32_t prev_id = 0; 
    for (int64_t i = 0; i < postings_list.posting_size(); ++i) {
        const Posting& posting = postings_list.posting(i);
        int32_t doc_id = prev_id + posting.docid();
        std::cerr << "[" << doc_id << "," << posting.tf() << "] ";
        prev_id = doc_id;
    }
    std::cerr << std::endl; 
}




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

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    std::string postings_filename;
    std::string output_basename;
    
    CLI::App app{"generate_pisa_index - a tool for generating a PISA index from a common index format."};
    app.add_option("-p,--postings", postings_filename, "Postings filename")->required();
    app.add_option("-o,--output", output_basename, "Output basename")->required();
    CLI11_PARSE(app, argc, argv);

    std::ofstream dstream(output_basename + ".docs");
    std::ofstream fstream(output_basename + ".freqs");
    std::ofstream sstream(output_basename + ".sizes");

    std::ifstream postings_data(postings_filename, std::ios::binary);
    google::protobuf::io::ZeroCopyInputStream* postings_stream = new google::protobuf::io::IstreamInputStream(&postings_data);
    google::protobuf::io::CodedInputStream coded_stream(postings_stream);

    while (true) {
        uint32_t message_size;
        if (!coded_stream.ReadVarint32(&message_size)) {
          break; // Assuming we're done now...
        }
        google::protobuf::io::CodedInputStream::Limit size_limit = coded_stream.PushLimit(message_size);
        PostingsList postings_list;
        if(!postings_list.ParseFromCodedStream(&coded_stream)) {
            std::cerr << "Couldn't read postings list... Exiting" << std::endl;
            exit(EXIT_FAILURE);
        }
        coded_stream.PopLimit(size_limit);
        dump_postings_list(postings_list);
    }

    delete postings_stream;
}
