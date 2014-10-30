import re
from sqlalchemy.engine.url import make_url

from IPython.core.magic_arguments import (argument, magic_arguments,
                                          parse_argstring)
from IPython.core.magic import (Magics, magics_class, line_cell_magic,
                                needs_local_scope)
from kdbpy.kdb import KQ, get_credentials


pattern = re.compile(r'''
            (?:(?P<name>[\w\+]+)://)?
            (?:
                (?P<username>[^:/]*)
                (?::(?P<password>.*))?
            @)?
            (?:
                (?P<ipv4host>[^/:]+)
                (?::(?P<port>[^/]*))?
            )?
            (?:/(?P<database>.*))?
            ''', re.X)


def get_url(url):
    gd = pattern.match(url).groupdict()
    username = gd.get('username')
    if username is None:
        raise ValueError('please provide a username')
    host = gd.get('ipv4host')
    if host is None:
        raise ValueError('please provide a host')
    return get_credentials(username=username,
                           host=host,
                           port=gd.get('port'),
                           password=gd.get('password'))


@magics_class
class QMagic(Magics):
    """Runs a Q statement on a table specified by SQLAlchemy connect string.

    Provides the %%q and %q magics."""

    _creds = []

    def __init__(self, shell):
        super(QMagic, self).__init__(shell=shell)

    @magic_arguments()
    @argument('-d', '--debug', action='store_true')
    @argument('-c', '--connection-string', default=None,
              help='KDB connection string')
    @argument('-r', '--restart', action='store_true',
              help='If passed, then restart the Q interpreter')
    @argument('-a', '--assign', default=None)
    @argument('code', nargs='*')
    @needs_local_scope
    @line_cell_magic
    def q(self, line, cell='', local_ns=None):
        """Run a Q program

        Examples::

          %%q -a result
          t: ([] name: `a`b`c; amount: 1.0 2.0. 3.0; id: 1 + til 4);
          x: t.amount + 1
          x

          df = %q t: ([] name: `a`b; amount: 1 2); select from t where amount > 1
        """
        args = parse_argstring(self.q, line)
        code = '%s\n%s' % (' '.join(args.code), cell)
        rx = re.compile(r'\s{2,}')
        code = code.strip().replace('\n', ' ').replace('\r', '').encode('utf8')
        code = rx.sub(' ', code)
        if args.debug:
            print(code)
        try:
            creds = get_url(args.connection_string)
        except TypeError:
            pass
        if self._creds:
            creds = self._creds[0]
        else:
            conn = make_url(args.connection_string)
            try:
                creds = get_credentials(port=conn.port, username=conn.username,
                                        host=conn.host, password=conn.password)
            except AttributeError:
                raise ValueError("please provide a connection string")
            if creds not in self._creds:
                self._creds.append(creds)

        user_ns = self.shell.user_ns
        user_ns.update(local_ns or {})
        result = KQ(creds,
                    start='restart' if args.restart else True).eval(code,
                                                                    **user_ns)
        if args.assign is not None:
            user_ns[args.assign] = result
        else:
            return result


def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(QMagic)
