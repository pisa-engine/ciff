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

void dump_term(const PostingsList& postings_list, uint32_t term_id) {
  std::cerr << postings_list.term() << " " << term_id << " " << postings_list.df() << "\n";
}

template <typename T>
std::ostream &write_sequence(std::ostream &os, gsl::span<T> sequence)
{
    auto length = static_cast<uint32_t>(sequence.size());
    os.write(reinterpret_cast<const char *>(&length), sizeof(length));
    os.write(reinterpret_cast<const char *>(sequence.data()), length * sizeof(T));
    return os;
}


struct inverted_index {

  std::unordered_map<uint32_t, std::vector<uint32_t>> m_documents {};
  std::unordered_map<uint32_t, std::vector<uint32_t>> m_frequencies {};
  std::map<uint32_t, uint32_t> m_document_sizes {};
  std::vector<std::string> m_plain_terms {};
 
  void add_document_lengths(const std::string doclen_file) {
      std::ifstream input_lengths(doclen_file);
      uint32_t doc_id;
      uint32_t size;
      while (input_lengths >> doc_id >> size) {
          m_document_sizes[doc_id] = size;
      }  
      std::cerr << "Read " << m_document_sizes.size() << " document lengths."
                << std::endl;
  } 

  void add_postings_list(const PostingsList& postings_list, uint32_t term_id) {

      if (term_id % 10000 == 0) {
          std::cerr << "Processing list " << term_id << "..." << std::endl;
      }

      std::string term = postings_list.term();
      m_plain_terms.emplace_back(term);

      uint32_t doc_freq = postings_list.df();
      // uint32_t collection_freq = postings_list.cf();

      std::vector<uint32_t> documents;
      std::vector<uint32_t> frequencies;
      m_documents[term_id].reserve(doc_freq);  
      m_frequencies[term_id].reserve(doc_freq);

      uint32_t pl_size = postings_list.posting_size();

      if (doc_freq != pl_size) {
          std::cerr << "Error: Posting size is not equal to document freq."
                    << std::endl;
          exit(EXIT_FAILURE);
      }

      uint32_t prev_id = 0; 
      for (int64_t i = 0; i < postings_list.posting_size(); ++i) {
          const Posting& posting = postings_list.posting(i);
          uint32_t doc_id = prev_id + posting.docid();
          uint32_t term_freq = posting.tf();
          m_documents[term_id].push_back(doc_id);
          m_frequencies[term_id].push_back(term_freq);
          prev_id = doc_id;
      }
   }
};


void write(std::string const &output_basename,
           inverted_index const &index,
           uint32_t term_count) {

    std::ofstream dstream(output_basename + ".docs");
    std::ofstream fstream(output_basename + ".freqs");
    std::ofstream sstream(output_basename + ".sizes");
    std::ofstream lexstream(output_basename + ".lexicon.plain");

    uint32_t doc_count = index.m_document_sizes.size();
    write_sequence(dstream, gsl::make_span<uint32_t const>(&doc_count, 1));
    for (size_t term_id = 0; term_id < term_count; ++term_id) {
        auto pos = index.m_documents.find(term_id);
        if (pos != index.m_documents.end()) {
            auto const &docs = pos->second;
            auto const &freqs = index.m_frequencies.at(term_id);
            write_sequence(dstream, gsl::span<uint32_t const>(docs));
            write_sequence(fstream, gsl::span<uint32_t const>(freqs));
        } else {
            write_sequence(dstream, gsl::span<uint32_t const>());
            write_sequence(fstream, gsl::span<uint32_t const>());
        }
        lexstream << index.m_plain_terms[term_id] << std::endl;
    }

    std::vector<uint32_t> doc_sizes;
    std::transform(index.m_document_sizes.begin(), index.m_document_sizes.end(), 
                   std::back_inserter(doc_sizes), 
                   [](const auto &pair) {
        return pair.second;
    });
    write_sequence(sstream, gsl::span<uint32_t const>(doc_sizes));
}

int main(int argc, char const *argv[])
{

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    std::string postings_filename;
    std::string output_basename;
    std::string doclen_filename;
    
    CLI::App app{"generate_pisa_index - a tool for generating a PISA index from a common index format."};
    app.add_option("-p,--postings", postings_filename, "Postings filename")->required();
    app.add_option("-d,--doclen", doclen_filename, "Document lengths filename")->required();
    app.add_option("-o,--output", output_basename, "Output basename")->required();
    CLI11_PARSE(app, argc, argv);

    inverted_index invidx;
    
    // Read document length tsv
    invidx.add_document_lengths(doclen_filename);

    std::ifstream postings_data(postings_filename, std::ios::binary);
    google::protobuf::io::ZeroCopyInputStream* postings_stream = new google::protobuf::io::IstreamInputStream(&postings_data);
    google::protobuf::io::CodedInputStream coded_stream(postings_stream);

    uint32_t term_id = 0;
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
        invidx.add_postings_list(postings_list, term_id);
        ++term_id;
    }
  
    std::cerr << "Writing canonical index..." << std::endl;
    write(output_basename, invidx, term_id);
    delete postings_stream;
}
