#!/usr/bin/env python
from tqdm import tqdm
import baker
import numpy as np
import common_index_format_pb2
from google.protobuf.internal.decoder import _DecodeVarint32

@baker.command(default=True)
def convert(postings_filename, doclen_filename, output):
    document_sizes = []
    num_docs = 0
    with open(doclen_filename, "r") as doclen_file:
        expected = 0;
        for line in doclen_file:
            docid, length = map(int, line.strip().split())
            if (docid != expected):
                raise Exception('Document Length file needs to be sorted. Exiting.')
            document_sizes.append(length)
            expected += 1
        num_docs = len(document_sizes)
        document_sizes.insert(0, num_docs)
        np.array(document_sizes, dtype=np.uint32).astype('uint32').tofile(output+".sizes")
 
    docs = []
    docs.append(1)
    docs.append(num_docs)
    freqs = []
    with open(postings_filename, "rb") as postings_file, \
        open(output+".terms", "w") as terms_file:
        buf = postings_file.read()
        n = 0
        term_id = 0
        with tqdm(total=len(buf)) as pbar:

            while n < len(buf):
                msg_len, new_pos = _DecodeVarint32(buf, n)
                n = new_pos
                msg_buf = buf[n:n+msg_len]
                n += msg_len
                pbar.update(msg_len)
                posting_list = common_index_format_pb2.PostingsList()
                posting_list.ParseFromString(msg_buf)
                term = posting_list.term
                terms_file.write(term+"\n")
                size = len(posting_list.posting)
                docs.append(size)
                freqs.append(size)
                current = 0
                for p in posting_list.posting:
                    current += p.docid
                    docs.append(current)
                    freqs.append(p.tf)
                term_id += 1
            np.array(docs, dtype=np.uint32).astype('uint32').tofile(output+".docs")
            np.array(freqs, dtype=np.uint32).astype('uint32').tofile(output+".freqs")


if __name__ == '__main__':
    baker.run()

