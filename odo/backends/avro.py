from __future__ import absolute_import, division, print_function

import errno
import os
import uuid
import json
import fastavro
import six
from avro import schema, datafile, io
from avro.schema import AvroException
from multipledispatch import dispatch
import pandas as pd
from datashape import dshape, discover, var, Record, Map, Var, \
    Option, null, string, int8, int32, int64, float64, float32, boolean, bytes_
import datashape.coretypes as ct
from collections import Iterator
from ..append import append
from ..convert import convert
from ..resource import resource
from ..temp import Temp

try:
    from avro.schema import make_avsc_object as schema_from_dict #Python 2.x
except ImportError:
    from avro.schema import SchemaFromJSONData as schema_from_dict  #Python 3.x

PRIMITIVE_TYPES_MAP = {
    'string': string,
    'int': int32,
    'long': int64,
    'null': null,
    'double': float64,
    'float': float32,
    'boolean': boolean,
    'record': Record,
}

NAMED_TYPES_MAP = {
    'fixed': bytes_, #TODO
    'enum': int8, #TODO
    'record': Record,
    'error': Record, #TODO
}

COMPOUND_TYPES_MAP = {
    'array': Var,
    'map': Map,
}


AVRO_TYPE_MAP = {}
AVRO_TYPE_MAP.update(PRIMITIVE_TYPES_MAP)
AVRO_TYPE_MAP.update(NAMED_TYPES_MAP)
AVRO_TYPE_MAP.update(COMPOUND_TYPES_MAP)

dshape_to_avro_primitive_types = {
    ct.int8: 'bytes',
    ct.int16: 'int',
    ct.int32: 'int',
    ct.int64: 'long',
    ct.float32: 'float',
    ct.float64: 'double',
    ct.date_: 'long',
    ct.datetime_: 'long',
    ct.string: 'string',
    ct.bool_: 'boolean'
}


class AVRO(object):
    """Wrapper object for reading and writing an Avro container file

    Parameters
    ----------

    uri : str
        uri of avro data

    schema : avro.schema.Schema
        User specified Avro schema object.  Used to decode file or serialize new records to file.
         schema is required to create a new Avro file.
         If reading or appending to an existing Avro file, the writers_schema embedded in that file
         will be used.

    codec : str
        compression codec.  Valid values: 'null', 'deflate', 'snappy'

    """
    def __init__(self, uri, schema=None, codec='null', **kwargs):
        self._uri = uri
        self._schema = schema
        self._codec = codec

        if not schema:
            sch = self._get_writers_schema()
            if sch is None:
                raise AvroException("Couldn't extract writers schema from '{0}'.  User must provide a valid schema".format(uri))
            self._schema = sch

    def __iter__(self):
        return self.reader.__iter__()

    def next(self):
        return self.reader.next()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # Perform a close if there's no exception
        if type is None:
            self.reader.close()
            self.writer.close()

    def _get_writers_schema(self):
        """
        Extract writers schema embedded in an existing Avro file.
        """
        reader = self.reader
        return schema_from_dict(self.reader.schema) if reader else None

    uri = property(lambda self: self._uri)
    codec = property(lambda self: self._codec)
    schema = property(lambda self: self._schema)

    @property
    def reader(self):
        if hasattr(self, '_reader'):
            if hasattr(self, '_writer'):
                self.flush()
            return self._reader
        else:
            try:
                reader_schema = self.schema.to_json() if self.schema else None
                df_reader = fastavro.reader(
                    open(self.uri, 'rb'),
                    reader_schema=reader_schema
                )

                return df_reader
            except IOError as exc:
                #If file doesn't exist, don't set _reader now.
                #Allow for reevaluation later after file has been created.
                #Otherwise, reraise exception
                if exc.errno != errno.ENOENT:
                    raise exc
                return None

    @staticmethod
    def _get_append_writer(uri, writers_schema=None):
        """
        Returns an isntance of avro.datafile.DataFileWriter for appending
        to an existing avro file at `uri`.  Does not take a writers schema,
        because avro library requires that writers_schema embedded in existing
        file be used for appending.

        Parameters
        ----------

        uri : str
            uri of avro existing, non-empty avro file

        writers_schema : avro.schema.Schema object
            If not None, checks that writers_schema in existing file is the same as supplied schema.
            Avro does not allow writing records to a container file with multiple writers_schema.

        Returns
        -------
        avro.datafile.DataFileWriter
        """
        rec_writer = io.DatumWriter()
        df_writer = datafile.DataFileWriter(
            open(uri, 'a+b'),
            rec_writer
        )
        #Check for embedded schema to ensure existing file is an avro file.
        try: #Python 2.x API
            schema_str = df_writer.get_meta('avro.schema')
        except AttributeError:  #Python 3.x API
            schema_str = df_writer.GetMeta('avro.schema').decode("utf-8")

        embedded_schema = schema_from_dict(json.loads(schema_str))

        #If writers_schema supplied, check for equality with embedded schema.
        if writers_schema:
            try:
                assert embedded_schema == writers_schema
            except AssertionError:
                raise ValueError("writers_schema embedded in {uri} differs from user supplied schema for appending.")

        return df_writer

    @staticmethod
    def _get_new_writer(uri, sch):
        """
        Returns an isntance of avro.datafile.DataFileWriter for writing
        to a new avro file at `uri`.

        Parameters
        ----------

        uri : str
            uri of avro existing, non-empty avro file

        sch : avro.schema.Schema object

        Returns
        -------
        avro.datafile.DataFileWriter
        """
        rec_writer = io.DatumWriter()
        try: #Python 2.x API
            df_writer = datafile.DataFileWriter(
                open(uri, 'wb'),
                rec_writer,
                writers_schema = sch
            )
        except TypeError: #Python 3.x API
            df_writer = datafile.DataFileWriter(
                open(uri, 'wb'),
                rec_writer,
                writer_schema = sch
            )

        return df_writer

    @property
    def writer(self):
        if hasattr(self, '_writer'):
            return self._writer
        else:
            if os.path.exists(self.uri) and os.path.getsize(self.uri) > 0:
                df_writer = self._get_append_writer(self.uri, self.schema)
            else:
                df_writer = self._get_new_writer(self.uri, self.schema)
            self._writer = df_writer
        return df_writer

    def flush(self):
        if hasattr(self, '_writer'):
            self._writer.close()
            del(self._writer)


@resource.register('.+\.(avro)')
def resource_avro(uri, schema=None, **kwargs):
    return AVRO(uri, schema=schema, **kwargs)

@dispatch(schema.RecordSchema)
def discover_schema(sch):
    return var * Record([(f.name, discover_schema(f.type)) for f in sch.fields])

@dispatch(schema.UnionSchema)
def discover_schema(sch):
    try:
        types = [s.type for s in sch.schemas]
        assert "null" in types
        types.remove("null")
        assert len(types) == 1
        return Option(AVRO_TYPE_MAP[types[0]])
    except AssertionError:
        raise TypeError("odo supports avro UnionSchema only for nullable fields." + \
                        "Received {0}".format(str([s.type for s in sch.schemas])))

@dispatch(schema.PrimitiveSchema)
def discover_schema(sch):
    return AVRO_TYPE_MAP[sch.type]

@dispatch(schema.MapSchema)
def discover_schema(sch):
    # Avro map types always have string keys, see https://avro.apache.org/docs/1.7.7/spec.html#Maps
    return Map(string, discover_schema(sch.values))

@dispatch(schema.ArraySchema)
def discover_schema(sch):
    return var * discover_schema(sch.items)

@dispatch(object)
def discover_schema(sch):
    raise TypeError('Unable to discover avro type %r' % type(sch).__name__)

@discover.register(AVRO)
def discover_avro(f, **kwargs):
    return discover_schema(f.schema)

def make_avsc_object(ds, name="name", namespace="default", depth=0):
    """
    Build Avro Schema from datashape definition

    Parameters
    ----------
    ds : str, unicode, DataShape, or datashapes.coretypes.*
    <name>: string -- applied to named schema elements (i.e. record, error, fixed, enum)
    <namespace>: string -- applied to named schema elements
    depth=0:  Tracking parameter for recursion depth.  Should not be set by user.

    Examples
    --------
    >>> test_dshape = "var * {letter: string, value: ?int32}"
    >>> x = make_avsc_object(test_dshape, name="my_record", namespace="com.blaze.odo")
    >>> x == {'fields': [{'name': 'letter', 'type': 'string'},
    ...                {'name': 'value', 'type': ['null', 'int']}],
    ...                 'name': 'my_record',
    ...                 'namespace': 'com.blaze.odo',
    ...                  'type': 'record'}
    True
    >>> test_dshape = '''
    ...    var * {
    ...      field_1: int32,
    ...      field_2: string,
    ...      field_3: ?int64,
    ...      features: map[string, float64],
    ...      words: var * string,
    ...      nested_record: var * {field_1: int64, field_2: float32}
    ...    }
    ... '''
    >>> x = make_avsc_object(test_dshape, name="my_record", namespace="com.blaze.odo")
    >>> x == {'fields': [{'name': 'field_1', 'type': 'int'},
    ...            {'name': 'field_2', 'type': 'string'},
    ...            {'name': 'field_3', 'type': ['null', 'long']},
    ...            {'name': 'features', 'type': {'type': 'map', 'values': 'double'}},
    ...            {'name': 'words', 'type': {'items': 'string', 'type': 'array'}},
    ...            {'name': 'nested_record',
    ...             'type': {'items': {'fields': [{'name': 'field_1',
    ...                                            'type': 'long'},
    ...                                           {'name': 'field_2',
    ...                                            'type': 'float'}],
    ...                                'name': 'my_recordd0d1',
    ...                                'namespace': 'com.blaze.odo',
    ...                                'type': 'record'},
    ...                      'type': 'array'}}],
    ... 'name': 'my_record',
    ... 'namespace': 'com.blaze.odo',
    ... 'type': 'record'}
    True
    """

    try:
        assert depth >= 0
    except AssertionError:
        raise ValueError("depth argument must be >= 0")

    #parse string to datashape object if necessary
    if isinstance(ds, six.string_types) or isinstance(ds, six.text_type):
        ds = dshape(ds)
    if isinstance(ds, ct.DataShape):
        if depth>0:
            assert isinstance(ds.parameters[0], ct.Var), "Cannot support fixed length substructure in Avro schemas"
            return {"type": "array", "items": make_avsc_object(ds.measure, name=name+"d%d" % depth, namespace=namespace, depth=depth+1)}
        elif depth==0:
            ds = ds.measure

    if isinstance(ds, ct.Record):
        return {
            "type": "record",
            "namespace": namespace,
            "name": name,
            "fields": [{"type": make_avsc_object(typ, name=name+"d%d" % depth, namespace=namespace, depth=depth+1),
                        "name": n} for (typ, n) in zip(ds.measure.types, ds.measure.names)]

        }
    if isinstance(ds, ct.Map):
        assert ds.key == ct.string, "Avro map types only support string keys.  Cannot form map with key type %s" % ds.key
        return {
            "type": "map",
            "values": make_avsc_object(ds.value, name=name+"d%d" % depth, namespace=namespace, depth=depth+1)
        }

    if isinstance(ds, ct.Option):
        return ["null", make_avsc_object(ds.ty, name=name+"d%d" % depth, namespace=namespace, depth=depth+1)]
    if ds in dshape_to_avro_primitive_types:
        return dshape_to_avro_primitive_types[ds]

    raise NotImplementedError("No avro type known for %s" % ds)


@convert.register(pd.DataFrame, AVRO, cost=4.0)
def avro_to_DataFrame(avro, dshape=None, **kwargs):
    #XXX:AEH:todo - correct for pandas automated type conversions.  e.g. strings containing numbers get cast to numeric.
    #XXX:AEH:todo - column with nulls just becomes an "object" column.
    df = pd.DataFrame([r for r in avro])
    names = [f.name for f in avro.schema.fields]
    df = df[names]
    return df

@convert.register(Temp(AVRO), Iterator, cost=1.0)
def convert_iterator_to_temporary_avro(data, schema=None, **kwargs):
    fn = '.%s.avro' % uuid.uuid1()
    avro = Temp(AVRO)(fn, schema, **kwargs)
    return append(avro, data, **kwargs)

@convert.register(Iterator, AVRO, cost=1.0)
def avro_to_iterator(s, **kwargs):
    return iter(s)

@append.register(AVRO, Iterator)
def append_iterator_to_avro(tgt_avro, src_itr, **kwargs):
    for datum in src_itr:
        tgt_avro.writer.append(datum)
    tgt_avro.flush()

@append.register(AVRO, object)  # anything else
def append_anything_to_iterator(tgt, src, **kwargs):
    source_as_iter = convert(Iterator, src, **kwargs)
    return append(tgt, source_as_iter, **kwargs)