from __future__ import absolute_import, division, print_function

import collections
try:
    import importlib
except:
    pass   # Python <2.7
import inspect
import re

from .append import append
from .compatibility import PY2
from .convert import convert

convert_hop = collections.namedtuple(
    'ConvertHop', ['source', 'target', 'func', 'mod', 'code'])

class Path(object):
    """Contains information about the conversion path that odo will take between
    two types.

    Two types of information are captured: information on an append operation
    and information on a convert operation. Append function calls will frequently
    contain convert calls as well, so both the append and convert attributes may
    have values. If the append function calls convert, we'll do our best to
    figure out the convert function's path, but it may not always be possible
    due to namespace considerations. Users may have to construct another
    simulation manually to understand the conversions inside of append calls.

    Positional arguments:
    source -- The object odo is converting from.
    target -- The object or type odo is converting to.
    call_type -- Either "append" or "convert" - the type of function odo will
        initially call for this conversion."""

    def __init__(self, source, target, call_type):
        self.source = source
        self.target = target
        self.convert_path = []

        if call_type.lower() == 'append':
            self.append_sig = (type(target), type(source))
            self.append_func = append.dispatch(type(target), type(source))
            self.append_mod = inspect.getmodule(self.append_func)
            self.append_code = inspect.getsource(self.append_func)

            # Look for a convert call inside the append function code
            match = re.search(r'convert\((.+?),', self.append_code, flags=re.I)
            if match:
                convert_str = match.group(1)
                convert_type = None
                # Convert object name string to actual object type, if possible
                try:
                    if PY2:
                        module = __import__('__builtin__', globals(), locals(), [], -1)
                    else:
                        module = importlib.import_module('builtins')
                    convert_type = getattr(module, convert_str)
                    self.convert_sig = (convert_type, type(source))
                except:
                    try:
                        module, convert_str2 = convert_str.rsplit(".", 1)
                        module = importlib.import_module(module)
                        convert_type = getattr(module, convert_str2)
                        self.convert_sig = (convert_type, type(source))
                    except:
                        self.convert_sig = (convert_str, type(source))

                if convert_type:
                    self.simulate_convert(convert_type, source)
            else:
                self.convert_sig = None

        elif call_type.lower() == 'convert':
            self.append_sig = None
            self.append_func = None
            self.append_mod = None
            self.append_code = None
            self.convert_sig = (target, type(source))
            self.simulate_convert(target, source)
        else:
            raise ValueError('call_type must be either "append" or "convert"')

    def simulate_convert(self, tgt, src):
        """Query networkx to see what the shortest path is from source to target
        for a convert call and add that information to the Path object.

        Positional arguments:
        tgt -- Destination object or type.
        src -- Source object or type.

        Raises networkx.NetworkXNoPath if no path from src to tgt exists."""

        if not isinstance(tgt, type):
            tgt = type(tgt)
        if not isinstance(src, type):
            src = type(src)

        convert_path = convert.path(src, tgt)
        for (src, tgt, func) in convert_path:
            hop = convert_hop(src, tgt, func, inspect.getmodule(func),
                              inspect.getsource(func))
            self.convert_path.append(hop)

    def __str__(self):
        if isinstance(self.source, type):
            source = self.source.__name__ + ' type'
        else:
            source = type(self.source).__name__ + ' object'
        if isinstance(self.target, type):
            target = self.target.__name__ + ' type'
        else:
            target = type(self.target).__name__ + ' object'

        outstr = 'Odo path simulation from {src} to {tgt}\n'.format(
            src=source, tgt=target)
        outstr += '------------------------------------------------------------\n'
        if self.append_sig:
            outstr += 'Append will be called from {src} to {tgt}\n'.format(
                src=self.append_sig[1].__name__, tgt=self.append_sig[0].__name__)
            outstr += 'Append will dispatch function {func} from {mod}\n'.format(
                func=self.append_func.__name__, mod=self.append_mod.__name__)
            outstr += 'Function {func} code follows:\n'.format(
                func=self.append_func.__name__)

            for line in self.append_code.split('\n'):
                outstr += '    ' + line + '\n'

        if self.convert_sig:
            if isinstance(self.convert_sig[0], basestring):
                target = self.convert_sig[0]
            else:
                target = self.convert_sig[0].__name__
            outstr += 'Convert will be called from {src} to {tgt}\n'.format(
                src=self.convert_sig[1].__name__, tgt=target)

        if self.convert_path:
            for (n, hop) in enumerate(self.convert_path, start=1):
                outstr += 'Conversion hop {n}: {src} to {tgt}\n'.format(
                    n=n, src=hop.source.__name__, tgt=hop.target.__name__)
                outstr += 'Hop dispatches function {func} from {mod}\n'.format(
                    func=hop.func.__name__, mod=hop.mod.__name__)
                outstr += 'Function {func} code follows:\n'.format(
                    func=hop.func.__name__)

                for line in hop.code.split('\n'):
                    outstr += '    ' + line + '\n'
        elif self.convert_sig:
           outstr += ('Convert pathing could not be determined (maybe {} is not '
                    'a type in a reachable namespace?)'.format(self.convert_sig[0]))

        return outstr

    def __repr__(self):
        return 'Path(source={src}, target={tgt})'.format(
            src=repr(self.source), tgt=repr(self.target))
