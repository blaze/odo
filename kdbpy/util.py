import IPython
import os
import sqlalchemy as sa


class PrettyMixin(object):

    def __repr__(self):
        return IPython.lib.pretty.pretty(self)

class CredsMixin(object):

    # credentials accessors

    @property
    def host(self):
        return self.credentials.host

    @property
    def port(self):
        return self.credentials.port

    @property
    def username(self):
        return self.credentials.username

    @property
    def password(self):
        return self.credentials.password

    @property
    def is_fixed_port(self):
        return self.credentials.is_fixed_port


def parse_connection_string(uri):
    from kdbpy.kdb import Credentials
    params = sa.engine.url.make_url(uri)
    return Credentials(username=params.username, password=params.password,
                       host=params.host, port=params.port)


def normpath(path):
    return path.replace(os.sep, '/')


hostname = 'localhost'
