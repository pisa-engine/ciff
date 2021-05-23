from pyciff import ciff_to_pisa, pisa_to_ciff


def test_ciff_to_pisa_and_back(tmpdir):
    import os

    pisa_path = tmpdir.join("pisa")
    ciff_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "ciff",
        "tests",
        "test_data",
        "toy-complete-20200309.ciff",
    )
    ciff_to_pisa(ciff_path, pisa_path)
    ciff2_path = tmpdir.join("ciff2")
    pisa_to_ciff(
        pisa_path,
        str(pisa_path) + ".terms",
        str(pisa_path) + ".documents",
        ciff2_path,
        "Description",
    )
    pisa2_path = tmpdir.join("pisa2")
    ciff_to_pisa(ciff2_path, pisa2_path)

    def assert_equal(lhs, rhs, suffix):
        with open(lhs + suffix, mode="rb") as file:
            lhs = file.read()
        with open(rhs + suffix, mode="rb") as file:
            rhs = file.read()
        assert lhs == rhs

    assert_equal(pisa_path, pisa2_path, ".terms")
    assert_equal(pisa_path, pisa2_path, ".documents")
    assert_equal(pisa_path, pisa2_path, ".docs")
    assert_equal(pisa_path, pisa2_path, ".freqs")
    assert_equal(pisa_path, pisa2_path, ".sizes")
