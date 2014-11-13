import IPython
import os
import sqlalchemy as sa


class PrettyMixin(object):
    def __repr__(self):
        return IPython.lib.pretty.pretty(self)


def parse_connection_string(uri):
    from kdbpy.kdb import Credentials
    params = sa.engine.url.make_url(uri)
    return Credentials(username=params.username, password=params.password,
                       host=params.host, port=params.port)


def normpath(path):
    return path.replace(os.sep, '/')
