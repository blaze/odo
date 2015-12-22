from __future__ import absolute_import, division, print_function

import os
import sys
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
from .sql import getbind


class CopyFromCSV(Executable, ClauseElement):
    def __init__(self, element, csv, delimiter=',', header=None, na_value='',
                 lineterminator='\n', quotechar='"', escapechar='\\',
                 encoding='utf8', skiprows=0, bind=None, **kwargs):
        if not isinstance(element, sa.Table):
            raise TypeError('element must be a sqlalchemy.Table instance')
        self.element = element
        self.csv = csv
        self.delimiter = delimiter
        self.header = (
            header if header is not None else
            (csv.has_header
             if csv.has_header is not None else infer_header(csv.path))
        )
        self.na_value = na_value
        self.lineterminator = lineterminator
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.encoding = encoding
        self.skiprows = int(skiprows or self.header)
        self._bind = getbind(element, bind)

        for k, v in kwargs.items():
            setattr(self, k, v)

        @property
        def bind(self):
            return self._bind


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
           '-cmd', '.import "%s" %s' % (
               # FIXME: format_table(t) is correct, but sqlite will complain
               fullpath, compiler.preparer.format_table(t)
           ),
           element.bind.url.database]
    stderr = subprocess.check_output(
        cmd,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE
    ).decode(sys.getfilesystemencoding())
    if stderr:
        # TODO: this seems like a lot of rigamarole
        try:
            raise OSError(stderr)
        except OSError as e:
            raise sa.exc.DatabaseError(' '.join(cmd), [], e)
    return ''


@compiles(CopyFromCSV, 'mysql')
def compile_from_csv_mysql(element, compiler, **kwargs):
    if element.na_value:
        raise ValueError(
            'MySQL does not support custom NULL values for CSV input'
        )
    return compiler.process(
        sa.text(
            """LOAD DATA {local} INFILE :path
            INTO TABLE {table}
            CHARACTER SET :encoding
            FIELDS
                TERMINATED BY :delimiter
                ENCLOSED BY :quotechar
                ESCAPED BY :escapechar
            LINES TERMINATED BY :lineterminator
            IGNORE :skiprows LINES
            """.format(
                local=getattr(element, 'local', ''),
                table=compiler.preparer.format_table(element.element)
            )
        ).bindparams(
            path=os.path.abspath(element.csv.path),
            encoding=element.encoding or element.bind.execute(
                'select @@character_set_client'
            ).scalar(),
            delimiter=element.delimiter,
            quotechar=element.quotechar,
            escapechar=element.escapechar,
            lineterminator=element.lineterminator,
            skiprows=int(element.header)
        ),
        **kwargs
    )


@compiles(CopyFromCSV, 'postgresql')
def compile_from_csv_postgres(element, compiler, **kwargs):
    if len(element.escapechar) != 1:
        raise ValueError(
            'postgres does not allow escape characters longer than 1 byte when '
            'bulk loading a CSV file'
        )
    if element.lineterminator != '\n':
        raise ValueError(
            r'PostgreSQL does not support line terminators other than \n'
        )
    return compiler.process(
        sa.text(
            """
            COPY {0} FROM :path (
                FORMAT CSV,
                DELIMITER :delimiter,
                NULL :na_value,
                QUOTE :quotechar,
                ESCAPE :escapechar,
                HEADER :header,
                ENCODING :encoding
            )
            """.format(compiler.preparer.format_table(element.element))
        ).bindparams(
            path=os.path.abspath(element.csv.path),
            delimiter=element.delimiter,
            na_value=element.na_value,
            quotechar=element.quotechar,
            escapechar=element.escapechar,
            header=element.header,
            encoding=element.encoding or element.bind(
                'show client_encoding'
            ).execute().scalar()
        ),
        **kwargs
    )


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
        cmd = CopyCommand(
            table=element.element,
            data_location=element.csv.path,
            access_key_id=aws_access_key_id,
            secret_access_key=aws_secret_access_key,
            format='CSV',
            delimiter=element.delimiter,
            ignore_header=int(element.header),
            empty_as_null=True,
            blanks_as_null=False,
            compression=compression
        )
        return compiler.process(cmd)


@append.register(sa.Table, CSV)
def append_csv_to_sql_table(tbl, csv, bind=None, **kwargs):
    bind = getbind(tbl, bind)
    dialect = bind.dialect.name

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
    stmt = CopyFromCSV(tbl, csv, bind=bind, **kwargs)
    with bind.begin() as conn:
        conn.execute(stmt)
    return tbl
