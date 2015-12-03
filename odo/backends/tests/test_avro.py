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
        { "name": "features", "type": { "type": "map", "values": "double"}}
    ]
}
"""

test_data = [
    {"field_1":1512357953,"field_2":"dgqbrudjnvvhdsa","field_3":-2529599232589628512,"features":{"glbumsyvgbmv":0.5262249719797424,"ivlmyhtbhpr":0.34864726886026076}},
    {"field_1":-422002496,"field_2":"ixjcpxaoqklbyp","field_3":2549258632438482521,"features":{"iqbxiagrparn":0.7367035457367471,"afjwfns":0.025058777723185655,"ndlrehie":0.4967780945917538,"di":0.810456042568542,"jgxyg":0.9625263303627106}},
    {"field_1":-811876665,"field_2":"efixmnaidxlva","field_3":None,"features":{"hdrvmxafp":0.872563073642209,"gair":0.518073477541009,"humgtonyoii":0.8214816239303898}},
    {"field_1":-761746797,"field_2":"toomqrltrbhechq","field_3":-3875987146454676496,"features":{"ksgwqlybs":0.3694600154809954,"ubtxnuhuqcqkvirkmvnbjsggvmbasnghdiwvfpwsihcfmgdcefadrqerqjhudteucjvjwhekgpjsytfrfjubqulsxmj":0.13827426007790045,"pxrjc":0.20131148613588346,"bmkalqykovaaqbc":0.8034756735507932,"xkkiigjtfgbnj":0.6750268550341367}},
    {"field_1":955048956,"field_2":"stogtdy","field_3":-3437921209509339504,"features":{"lndm":0.25025327732757696,"qeecpdxq":0.44506724761212746,"csosogkyuckanv":0.8739675025249061,"oyvnsshedr":0.8225465933655276}},
    {"field_1":-161240515,"field_2":"qcisooo","field_3":1311605767031982299,"features":{"gxias":0.11162484249218518,"pgdyatbp":0.5487679009288856,"itvxelmfnbrq":0.906496888778101,"kkififqgvs":0.863635294016924}},
    {"field_1":1956293589,"field_2":"rsidajtxnum","field_3":3246732014409650694,"features":{"":0.14456022508672162,"fknbdjfcnag":0.2742703631839102,"rtttccsdf":0.8117164405618132}},
    {"field_1":1554383058,"field_2":"xyahxnxc","field_3":None,"features":{"lvvw":0.8727205291174954,"pytquvfii":0.43930224145682706}},
    {"field_1":1955780624,"field_2":"wvbte","field_3":7012092663456604164,"features":{"uow":0.9091043815461999,"vbrqiknjrfli":0.43792647841064225,"djf":0.22671511625162166}},
    {"field_1":-1215933043,"field_2":"nsibpyqsuf","field_3":None,"features":{"":0.1543825213081953,"mndjtjl":0.5742890151730541,"gbhiam":0.3137956141490078,"cojkna":0.9846856885267534}},
]

ds = dshape("""var * {
  field_1: int32,
  field_2: string,
  field_3: ?int64,
  features: map[string, float64]
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

        names = ["field_1", "field_2", "field_3", "features"]
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