#include <iostream>
#include <fstream>
#include "google/protobuf/io/coded_stream.h"
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "common-index-format.pb.h"


using namespace std;
int main(int argc, char const *argv[])
{
  // Verify that the version of the library that we linked against is
  // compatible with the version of the headers we compiled against.
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  io::anserini::cidxf::PostingsList pl;	

  // Read the existing address book.
  fstream in(argv[1], ios::in | ios::binary);
  google::protobuf::io::IstreamInputStream instream(&in);
  google::protobuf::io::CodedInputStream input(&instream);

  size_t max = strtol(argv[2], NULL, 10);
  for(size_t i = 0; i < max; ++i){
  uint32_t size;
  if (!input.ReadVarint32(&size)) {
    return false;
  }
    // Tell the stream not to read beyond that size.
    const auto limit = input.PushLimit(size);

    // Parse the message.
    if (!pl.MergeFromCodedStream(&input)) {
        return false;
    }
    if (!input.ConsumedEntireMessage()) {
        return false;
    }

    // Release the limit.
    input.PopLimit(limit);

    std::cout << pl.term() << std::endl;
  }
	/*
  if (!input) {
    cout << argv[1] << ": File not found.  Creating a new file." << endl;	
  } else if (!pl.ParseFromIstream(&input)) {
    cerr << "Failed to parse posting lists." << endl;
    return -1;
  }
*/
  return 0;
}

