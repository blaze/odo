from __future__ import absolute_import, division, print_function

import os
import re
import subprocess
import uuid
import mmap

from contextlib import closing
from functools import partial
from distutils.spawn import find_executable

import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import Executable, ClauseElement

from toolz import merge
from multipledispatch import MDNotImplementedError

from ..append import append
from ..convert import convert
from .csv import CSV, infer_header
from ..temp import Temp
from .aws import S3
from .sql import fullname


class CopyFromCSV(Executable, ClauseElement):
    def __init__(self, element, csv, delimiter=',', header=None, na_value='',
                 lineterminator='\n', quotechar='"', escapechar='\\',
                 encoding='utf8', skiprows=0, **kwargs):
        if not isinstance(element, sa.Table):
            raise TypeError('element must be a sqlalchemy.Table instance')
        self.element = element
        self.csv = csv
        self.delimiter = delimiter
        self.header = (header if header is not None else
                       (csv.has_header
                        if csv.has_header is not None else infer_header(csv.path)))
        self.na_value = na_value
        self.lineterminator = lineterminator
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.encoding = encoding
        self.skiprows = int(skiprows or self.header)

        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def bind(self):
        return self.element.bind


@compiles(CopyFromCSV, 'sqlite')
def compile_from_csv_sqlite(element, compiler, **kwargs):
    if not find_executable('sqlite3'):
        raise MDNotImplementedError("Could not find sqlite executable")

    t = element.element
    if not element.header:
        csv = element.csv
    else:
        csv = Temp(CSV)('.%s' % uuid.uuid1())
        assert csv.has_header, \
            'SQLAlchemy element.header is True but CSV inferred no header'

        # write to a temporary file after skipping the first line
        chunksize = 1 << 24  # 16 MiB
        lineterminator = element.lineterminator.encode(element.encoding)
        with open(element.csv.path, 'rb') as f:
            with closing(mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)) as mf:
                index = mf.find(lineterminator)
                if index == -1:
                    raise ValueError("'%s' not found" % lineterminator)
                mf.seek(index + len(lineterminator))  # len because \r\n
                with open(csv.path, 'wb') as g:
                    for chunk in iter(partial(mf.read, chunksize), b''):
                        g.write(chunk)

    fullpath = os.path.abspath(csv.path).encode('unicode-escape').decode()
    cmd = ['sqlite3',
           '-nullvalue', repr(element.na_value),
           '-separator', element.delimiter,
           '-cmd', '.import "%s" %s' % (fullpath, t.name),
           element.bind.url.database]
    stdout, stderr = subprocess.Popen(cmd,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT,
                                      stdin=subprocess.PIPE).communicate()
    assert not stdout, \
        'error: %s from command: %s' % (stdout, ' '.join(cmd))
    return ''


@compiles(CopyFromCSV, 'mysql')
def compile_from_csv_mysql(element, compiler, **kwargs):
    if element.na_value:
        raise ValueError('MySQL does not support custom NULL values')
    encoding = {'utf-8': 'utf8'}.get(element.encoding.lower(),
                                     element.encoding or 'utf8')
    escapechar = element.escapechar.encode('unicode-escape').decode()
    lineterminator = element.lineterminator.encode('unicode-escape').decode()
    result = r"""
        LOAD DATA {local} INFILE '{path}'
        INTO TABLE {0.element.name}
        CHARACTER SET {encoding}
        FIELDS
            TERMINATED BY '{0.delimiter}'
            ENCLOSED BY '{0.quotechar}'
            ESCAPED BY '{escapechar}'
        LINES TERMINATED BY '{0.lineterminator}'
        IGNORE {0.skiprows} LINES;
    """.format(element,
               path=os.path.abspath(element.csv.path),
               local=getattr(element, 'local', ''),
               encoding=encoding,
               lineterminator=lineterminator,
               escapechar=escapechar).strip()
    return result


@compiles(CopyFromCSV, 'postgresql')
def compile_from_csv_postgres(element, compiler, **kwargs):
    encoding = {'utf8': 'utf-8'}.get(element.encoding.lower(),
                                     element.encoding or 'utf8')
    if len(element.escapechar) != 1:
        raise ValueError('postgres does not allow escapechar longer than 1 '
                         'byte')
    statement = """
    COPY {fullname} FROM '{path}'
        (FORMAT CSV,
         DELIMITER E'{0.delimiter}',
         NULL '{0.na_value}',
         QUOTE '{0.quotechar}',
         ESCAPE '{0.escapechar}',
         HEADER {header},
         ENCODING '{encoding}')"""
    return statement.format(element,
                            fullname=fullname(element.element, compiler),
                            path=os.path.abspath(element.csv.path),
                            header=str(element.header).upper(),
                            encoding=encoding).strip()


try:
    import boto
    from odo.backends.aws import S3
    from redshift_sqlalchemy.dialect import CopyCommand
except ImportError:
    pass
else:
    @compiles(CopyFromCSV, 'redshift')
    def compile_from_csv_redshift(element, compiler, **kwargs):
        assert isinstance(element.csv, S3(CSV))
        assert element.csv.path.startswith('s3://')

        cfg = boto.Config()

        aws_access_key_id = cfg.get('Credentials', 'aws_access_key_id')
        aws_secret_access_key = cfg.get('Credentials', 'aws_secret_access_key')

        compression = getattr(element, 'compression', '').upper() or None
        cmd = CopyCommand(table=element.element,
                          data_location=element.csv.path,
                          access_key_id=aws_access_key_id,
                          secret_access_key=aws_secret_access_key,
                          format='CSV',
                          delimiter=element.delimiter,
                          ignore_header=int(element.header),
                          empty_as_null=True,
                          blanks_as_null=False,
                          compression=compression)
        return compiler.process(cmd)


@append.register(sa.Table, CSV)
def append_csv_to_sql_table(tbl, csv, **kwargs):
    dialect = tbl.bind.dialect.name

    # move things to a temporary S3 bucket if we're using redshift and we
    # aren't already in S3
    if dialect == 'redshift' and not isinstance(csv, S3(CSV)):
        csv = convert(Temp(S3(CSV)), csv, **kwargs)
    elif dialect != 'redshift' and isinstance(csv, S3(CSV)):
        csv = convert(Temp(CSV), csv, has_header=csv.has_header, **kwargs)
    elif dialect == 'hive':
        from .ssh import SSH
        return append(tbl, convert(Temp(SSH(CSV)), csv, **kwargs), **kwargs)

    kwargs = merge(csv.dialect, kwargs)
    stmt = CopyFromCSV(tbl, csv, **kwargs)
    with tbl.bind.begin() as conn:
        conn.execute(stmt)
    return tbl
