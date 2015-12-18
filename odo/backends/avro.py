from __future__ import absolute_import, division, print_function

import errno
import os
import uuid

from avro import schema, datafile, io
from avro.schema import AvroException
import pandas as pd
from datashape import discover, var, Record, Map, Var, \
    Option, null, string, int32, int64, float64, float32, boolean
from collections import Iterator
from ..append import append
from ..convert import convert
from ..resource import resource
from ..temp import Temp

AVRO_TYPE_MAP = {
    'string': string,
    'int': int32,
    'long': int64,
    'null': null,
    'double': float64,
    'float': float32,
    'bool': boolean,
    'map': Map,
    'record': Record,
    'array': Var,
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
        self._kwargs = kwargs #CURRENTLY UNUSED

        if not schema:
            sch = self._get_writers_schema()
            if sch is None:
                raise AvroException("Couldn't extract writers schema from '{0}'.  User must provide a valid schema".format(uri))
            self._schema = sch

    def __iter__(self):
        return self.reader

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
        return schema.parse(self.reader.meta['avro.schema']) if reader else None

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
                rec_reader = io.DatumReader(readers_schema=self.schema)

                df_reader = datafile.DataFileReader(
                    open(self.uri, 'rb'),
                    rec_reader
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
            open(uri, 'ab+'),
            rec_writer
        )
        #Check for embedded schema to ensure existing file is an avro file.
        embedded_schema = schema.parse(df_writer.get_meta('avro.schema'))

        #If writers_schema supplied, check for equality with embedded schema.
        if writers_schema:
            assert embedded_schema == writers_schema, \
                "writers_schema embedded in {uri} differs from user supplied schema for appending."

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
        df_writer = datafile.DataFileWriter(
            open(uri, 'wb'),
            rec_writer,
            writers_schema = sch
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
            raise TypeError("odo supports avro UnionSchema only for nullabel fields.  "
                            "Received {0}".format(str([s.type for s in sch.schemas])))
    elif isinstance(sch, schema.PrimitiveSchema):
        return AVRO_TYPE_MAP[sch.type]
    elif isinstance(sch, schema.MapSchema):
        return Map(string, discover_schema(sch.values))
    elif isinstance(sch, schema.ArraySchema):
        return var * discover_schema(sch.items)
    else:
        raise Exception(str(type(sch)))

@discover.register(AVRO)
def discover_avro(f, **kwargs):
    return discover_schema(f.schema)

@convert.register(pd.DataFrame, AVRO, cost=4.0)
def avro_to_DataFrame(avro, dshape=None, **kwargs):
    #XXX:AEH:todo - correct for pandas automated type conversions.  e.g. strings containing numbers get cast to numeric.
    #XXX:AEH:todo - column with nulls just becomes an "object" column.
    df = pd.DataFrame([r for r in avro])
    names = [f.name.decode('utf-8') for f in avro.schema.fields]
    df = df[names]
    return df

@convert.register(Temp(AVRO), Iterator, cost=1.0)
def convert_iterator_to_temporary_avro(data, schema=None, **kwargs):
    fn = '.%s.avro' % uuid.uuid1()
    avro = Temp(AVRO)(fn, schema, **kwargs)
    return append(avro, data, **kwargs)


@convert.register(Iterator, AVRO, cost=1.0)
def avro_to_iterator(s, **kwargs):
    return s

@append.register(AVRO, Iterator)
def append_iterator_to_avro(tgt_avro, src_itr, **kwargs):
    for datum in src_itr:
        tgt_avro.writer.append(datum)
    tgt_avro.flush()

@append.register(AVRO, object)  # anything else
def append_anything_to_iterator(tgt, src, **kwargs):
    source_as_iter = convert(Iterator, src, **kwargs)
    return append(tgt, source_as_iter, **kwargs)