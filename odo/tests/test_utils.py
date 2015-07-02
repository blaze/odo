from odo.utils import ext, iter_except, keywords, gentemp, records_to_tuples


def test_ext():
    assert ext('a.py') == 'py'
    assert ext('apy') == ''
    assert ext('myfile.csv.gz') == 'gz'


def test_iter_except():
    s = set([1, 2, 3])

    def iterate():
        yield 1
        yield 2
        yield 4

    elems = iterate()
    inset = iter_except(lambda: next(elems) in s, StopIteration)
    assert list(inset) == [True, True, False]


def test_keywords():
    assert keywords(keywords) == ["func"]


def test_gentemp():
    i, _, data = next(gentemp([[1, 2], [3, 4]], start=1))
    assert i == 1 and data == [1, 2]


def test_records_to_tuples():
    seq = [{'i': 1, 'f': 1.0}, {'i': 2, 'f': 2.0}]
    lst = list(records_to_tuples('var * {i: int, f: float64}', seq))
    assert lst == [(1, 1.0), (2, 2.0)]


def test_records_to_tuples_mismatch_passthrough():
    assert records_to_tuples('var * int', 'dummy') == 'dummy'
