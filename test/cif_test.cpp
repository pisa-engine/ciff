#include <iostream>
#include <fstream>

#include "common-index-format.pb.h"


using namespace std;
int main(int argc, char const *argv[])
{
  // Verify that the version of the library that we linked against is
  // compatible with the version of the headers we compiled against.
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  io::anserini::cidxf::PostingsList pl;	

  // Read the existing address book.
  fstream input(argv[1], ios::in | ios::binary);
  if (!input) {
    cout << argv[1] << ": File not found.  Creating a new file." << endl;	
  } else if (!pl.ParseFromIstream(&input)) {
    cerr << "Failed to parse posting lists." << endl;
    return -1;
  }

  return 0;
}

