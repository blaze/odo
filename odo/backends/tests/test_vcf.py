from __future__ import absolute_import, division, print_function
from __future__ import unicode_literals

import pytest

import sys
import os
import shutil
import pandas as pd
import gzip
import tempfile
from collections import Iterator

from odo.backends.vcf import (VCF, append, convert, resource,
                              vcf_to_dataframe)
from odo.utils import filetext, filetexts
from odo import (into, append, convert, resource, odo)
from odo.chunks import chunks

def test_vcf_to_dataframe():
    fp = os.path.join(os.path.dirname(__file__), 'dummydata.vcf')
    df = odo(fp, pd.DataFrame)
    assert df.shape == (5, 474)
    assert df.columns[0] == '#CHROM'
    assert df.columns[-1] == 'NA20828'
    assert df.loc[4, 'NA20802'] == '0|0:-0.01,-1.51,-5.00:0.000:.:.'

@pytest.fixture
def header():
    return ("#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO")

@pytest.fixture
def data0():
    t = []
    t.append((3, 16050, "snp_22_16050408", "T", "C", 10.0, 'bla1', 'bla2'))
    t.append((3, 16050, "snp_22_16050612", "C", "G", 10.0, 'bla1', 'bla2'))
    return t

@pytest.fixture
def data1():
    t = []
    t.append((22, 16050, "snp_00408", "T", "C", 3.0, 'bla5', 'bla2'))
    t.append((22, 16050, "snp_2_16050612", "C", "G", 11.0, 'bla1', 'bla2'))
    return t

def test_vcf_append(header, data0):
    with filetext("\t".join(header) + '\n',
                  extension='.vcf', mode='w') as fn:
        vcf = VCF(fn)

        append(vcf, list(data0))

        assert list(convert(Iterator, vcf)) == list(data0)

        with open(fn) as f:
            s = f.read()

        assert 'snp_22_16050408' in s
        assert '10.0' in s


@pytest.mark.xfail(sys.platform == 'win32' and sys.version_info[0] < 3,
                   reason="Doesn't work on Windows")
def test_vcf_read_supports_gzip(header, data0):
    data = [header] + data0
    with filetext("\n".join(["\t".join([str(i) for i in d]) for d in data]),
                  open=gzip.open,
                  mode='wt', extension='.vcf.gz') as fn:
        vcf = VCF(fn)
        df = vcf_to_dataframe(vcf)
        assert isinstance(df, pd.DataFrame)
        l = convert(list, df)
        assert len(l) == 2
        assert l[1][5] == 10.0
        assert list(df.columns) == list(header)

def test_pandas_write(header, data0):
    with filetext("\t".join(header) + '\n',
                  extension='.vcf', mode='w') as fn:

        vcf = VCF(fn)
        append(vcf, data0)

        with open(fn) as f:
            text = f.read()
            assert 'snp_22_16050612' in text
            assert '#CHROM' in text

        # Doesn't write header twice
        append(vcf, data0)
        with open(fn) as f:
            s = f.read()
            assert s.count('#CHROM') == 1

def test_pandas_write_gzip(header, data0):
    tmpdir = tempfile.mkdtemp()
    try:
        f = os.path.join(tmpdir, '.vcf')

        with gzip.open(f, 'wt') as g:
            # txt = unicode("\t".join(header) + '\n').encode()
            txt = "\t".join(header) + '\n'
            g.write(txt)
            vcf = VCF(buffer=g)
            append(vcf, data0)

        with gzip.open(f, mode='r') as g:
            s = g.read()
            assert b'snp_22_16050612' in s
            assert b'#CHROM' in s
    finally:
        shutil.rmtree(tmpdir)

def test_vcf_into_list(header, data0):
    with filetext("\t".join(header) + '\n', extension='vcf') as fn:
        vcf = VCF(fn)
        append(vcf, data0)
        assert into(list, fn) == list(data0)

def test_discover_vcf_files_without_header(header, data0):
    with filetext("\t".join(header) + '\n') as fn:
        vcf = VCF(fn)
        append(vcf, data0)
        df = convert(pd.DataFrame, vcf)
        assert len(df) == 2
        assert '16050' not in list(df.columns)

def test_glob(header, data0, data1):
    hdata0 = [header] + data0
    hdata1 = [header] + data1
    txt0 = "\n".join(["\t".join([str(i) for i in d]) for d in hdata0])
    txt1 = "\n".join(["\t".join([str(i) for i in d]) for d in hdata1])

    d = {'file1.vcf': txt0,
         'file2.vcf': txt1}
    with filetexts(d):
        r = resource('file*.vcf')
        assert convert(list, r) == data0 + data1
        assert isinstance(r, chunks(VCF))
