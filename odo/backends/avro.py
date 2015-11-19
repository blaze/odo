from __future__ import absolute_import, division, print_function

from avro import schema, datafile, io
import pandas as pd
from datashape import discover, dshape, var, Record, Map, date_, datetime_, \
    Option, null, string, int32, int64, float64, float32, boolean
from collections import Iterator
from ..append import append
from ..convert import convert
from ..resource import resource

AVRO_TYPE_MAP = {
    'string': string,
    'int': int32,
    'long': int64,
    'null': null,
    'double': float64,
    'float': float32,
    'bool': boolean,
}

TD_AVRO_TYPES = {
    "BIGINT": "long",
    "BYTEINT": "int",
    "DECIMAL_SHORT": "int",
    "DECIMAL_LONG": "long",
    "DECIMAL": "double",
    "FLOAT": "double",
    "INT": "int",
    "INTEGER": "int",
    "SMALLINT": "int",
    "CHAR": "string",
    "DATE": "string",
    "TIME": "string",
    "TIMESTAMP": "string"
}


@resource.register('.+\.(avro)')
def resource_avro(uri, **kwargs):

    rec_reader = io.DatumReader()
    df_reader = datafile.DataFileReader(
        open(uri, 'r'),
        rec_reader
    )
    return df_reader

def _get_schema(f):
    return schema.parse(f.meta['avro.schema'])

def discover_schema(sch):
    
    if isinstance(sch, schema.RecordSchema):
        return var * Record([(f.name, discover_schema(f.type)) for f in sch.fields])
    elif isinstance(sch, schema.UnionSchema):
        try:
            types = [s.type for s in sch.schemas]
            assert "null" in types
            types.remove("null")
            assert len(types) == 1
            return Option(AVRO_TYPE_MAP[types[0]])
        except AssertionError:
            import pdb; pdb.set_trace()
            return null
    elif isinstance(sch, schema.PrimitiveSchema):
        return AVRO_TYPE_MAP[sch.type]
    elif isinstance(sch, schema.MapSchema):
        return Map(string, discover_schema(sch.values))
    elif isinstance(sch, schema.ArraySchema):
        raise Exception("ArraySchema TODO")
    else:
        raise Exception(str(type(sch)))

@discover.register(datafile.DataFileReader)
def discover_avro(f, **kwargs):
    return discover_schema(_get_schema(f))

@convert.register(pd.DataFrame, datafile.DataFileReader, cost=4.0)
def avro_to_DataFrame(avro, dshape=None, **kwargs):
    df = pd.DataFrame([r for r in avro])
    names = [f.name.decode('utf-8') for f in _get_schema(avro).fields]
    df = df[names] #Reorder names to match avro schema
    #names = [col.decode('utf-8') for col in avro
    #df = df[names]  # Reorder names to match sasfile
    return df

@convert.register(Iterator, datafile.DataFileReader, cost=1.0)
def avro_to_iterator(s, **kwargs):
    return s