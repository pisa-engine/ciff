from .pyciff import *


def ciff_to_pisa(input_file, output):
    ciff_to_pisa_internal(str(input_file), str(output))


def pisa_to_ciff(collection_input, terms_input, titles_input, output, description):
    pisa_to_ciff_internal(
        str(collection_input),
        str(terms_input),
        str(titles_input),
        str(output),
        str(description),
    )
