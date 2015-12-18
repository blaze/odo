from __future__ import absolute_import, division, print_function

from avro import datafile, io, schema
from collections import Iterator
import pandas as pd
from pandas.util.testing import assert_frame_equal
from odo.backends.avro import discover, avro_to_DataFrame, avro_to_iterator, resource, AVRO

import unittest
import tempfile

from odo.utils import tmpfile, into_path
from odo import append, convert, resource, dshape

test_schema_str = """
{
    "type"        : "record",
    "namespace"   : "dataset",
    "name"        : "test_dataset",
    "fields": [
        {"type": "int"   , "name": "field_1"},
        {"type": "string", "name": "field_2"},
        {"default": null, "name": "field_3", "type": ["null", "long"]},
        { "name": "features", "type": { "type": "map", "values": "double"}},
        { "name": "words", "type": {"type": "array", "items": "string"}}
    ]
}
"""

test_data = [
    {"field_1":2072373602,"field_2":"mxllbfxk","field_3":-3887990995227229804,"features":{"bhettcdl":0.8581552641969377,"vdqvnqgqbrjtkug":0.4938648291874551,"sgmlbagyfb":0.5796466618955293,"ka":0.9873135485253831},"words":["ciplc","htvixoujptehr","rbeiimkevsn"]},
    {"field_1":517434305,"field_2":"frgcnqrocddimu","field_3":None,"features":{"atqqsuttysdrursxlynwcrmfrwcrdxaegfnidvwjxamoj":0.2697279678696263,"kjb":0.8279248178446112,"wqlecjb":0.8241169129373344,"inihhrtnawyopu":0.08511455977126114,"dpjw":0.760489536392584},"words":["ignsrafxpgu","ckg"]},
    {"field_1":1925434607,"field_2":"aurlydvgfygmu","field_3":None,"features":{"crslipya":0.1596449079423896,"":0.4304848508533662,"imbfgwnaphh":0.19323554138270294},"words":["rqdpanbbcemg","auurshsxxkp","rdngxdthekt"]},
    {"field_1":636669589,"field_2":"","field_3":-1858103537322807465,"features":{"dv":0.9635053430456509,"lhljgywersxjp":0.5289026834129389,"nmtns":0.7645922724023969},"words":["vviuffehxh","jpquemsx","xnoj",""]},
    {"field_1":-1311284713,"field_2":"infejerere","field_3":5673921375069484569,"features":{"iaen":0.7412670573684966,"ekqfnn":0.6685382939302145,"innfcqqbdrpcdn":0.39528359165136695,"fd":0.8572519278668735,"fbryid":0.7244784428105817},"words":["ciqu","emfruneloqh"]},
    {"field_1":1716247766,"field_2":"gmmfghijngo","field_3":None,"features":{"ourul":0.1849234265503661,"vhvwhech":0.41140968300430625,"m":0.9576395352199625,"fgh":0.9547116485401502,"gqpdtvncno":0.027038814818686197},"words":["ugwcfecipffmkwi","kttgclwjlk","siejdtrpjkqennx","ixwrpmywtbgiygaoxpwnvuckdygttsssqfrplbyyv","mfsrhne"]},
    {"field_1":101453273,"field_2":"frjaqnrbfspsuw","field_3":None,"features":{"ffps":0.02989888991738765,"fxkhyomw":0.2963204572188527},"words":["jwi","rfxlxngyethg"]},
    {"field_1":-1792425886,"field_2":"pqkawoyw","field_3":None,"features":{"vsovnbsdhbkydf":0.09777409545072746,"eovoiix":0.10890846076556715},"words":["xntmmvpbrq","uof"]},
    {"field_1":-1828393530,"field_2":"nkflrmkxiry","field_3":None,"features":{"qewmpdviapfyjma":0.8727493942139006},"words":["lgtrtjhpf"]},
    {"field_1":1048099453,"field_2":"jsle","field_3":None,"features":{"qbndce":0.5459572647413652},"words":["d"]},
]

ds = dshape("""var * {
  field_1: int32,
  field_2: string,
  field_3: ?int64,
  features: map[string, float64],
  words: var * string
  }""")

test_path = into_path('backends', 'tests', 'test_file.avro')

class TestAvro(unittest.TestCase):

    def setUp(self):
        self.avrofile = resource(test_path)
        self.temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".avro")

    def tearDown(self):
        self.temp_output.unlink(self.temp_output.name)

    def test_resource_datafile(self):
        self.assertIsInstance(resource(test_path), AVRO)

    def test_discover(self):
        self.assertEquals(discover(self.avrofile), ds)

    def test_convert_avro_to_dataframe(self):
        df = convert(pd.DataFrame, self.avrofile)
        self.assertIsInstance(df, pd.DataFrame)

        names = ["field_1", "field_2", "field_3", "features", "words"]
        expected_output = pd.DataFrame(test_data, columns=names)
        assert_frame_equal(df, expected_output)

    def test_convert_avro_to_iterator(self):
        itr = convert(Iterator, self.avrofile)
        self.assertIsInstance(itr, Iterator)
        self.assertEqual(list(itr), test_data)

    def test_require_schema_for_new_file(self):
        self.assertRaises(schema.AvroException, AVRO, "doesntexist.avro")

    def test_append_and_convert_round_trip(self):
        x = AVRO(self.temp_output.name, schema=schema.parse(test_schema_str))
        append(x, test_data)
        append(x, test_data)
        assert convert(list, x) == test_data * 2


if __name__=="__main__":
    unittest.main()