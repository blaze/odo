import os
from IPython.core.magic_arguments import (argument, magic_arguments,
                                          parse_argstring)
from IPython.core.magic import (Magics, magics_class, line_magic,
                                needs_local_scope)
from kdbpy.kdb import KQ, get_credentials


@magics_class
class QMagic(Magics):
    """Runs a Q statement on a table, specified by SQLAlchemy connect string.

    Provides the %%q magic."""

    def __init__(self, shell):
        super(QMagic, self).__init__(shell=shell)

    @magic_arguments()
    @argument('-d', '--data-frame', action='store_true',
              help='Return output as a pandas DataFrame')
    @argument('-p', '--path', default=os.environ.get('QEXEC'))
    @argument('code', nargs='*')
    @needs_local_scope
    @line_magic
    def q(self, line, local_ns=None):
        """ Run a Q program
        Examples::

          %q t: ([] name: `a`b`c; amount: 1.0 2.0. 3.0; id: 1 + til 3); t

          df = %q -d t: ([] name: `a`b; amount: 1 2); select from t where amount > 1

          %q /path/to/q ([] name: `a`b; amount: 3 4)
        """
        args = parse_argstring(self.q, line)
        code = (' '.join(args.code) + '\n').encode('utf8')

        def qnative(s):
            import subprocess
            if args.path is None:
                raise ValueError("Please define the QEXEC environment variable "
                                 "to point to your Q interpreter or path the "
                                 "--path argument")
            process = subprocess.Popen([os.path.expanduser(args.path)],
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            stdout, _ = process.communicate(input=s)
            print(stdout.rstrip('\n'))

        def qpython(s):
            user_ns = self.shell.user_ns
            user_ns.update(local_ns or {})
            # TODO: add a connection string here
            return KQ(get_credentials(), start='restart').eval(s, **user_ns)

        return qpython(code) if args.data_frame else qnative(code)


def load_ipython_extension(ip):
    """Load the extension in IPython."""
    ip.register_magics(QMagic)