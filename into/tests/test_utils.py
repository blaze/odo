from odo.utils import ext


def test_ext():
    assert ext('a.py') == 'py'
    assert ext('apy') == ''
    assert ext('myfile.csv.gz') == 'gz'
