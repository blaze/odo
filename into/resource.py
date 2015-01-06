from __future__ import absolute_import, division, print_function

import re
from .regex import RegexDispatcher


__all__ = 'resource'


resource = RegexDispatcher('resource')


@resource.register('.*', priority=1)
def resource_all(uri, *args, **kwargs):
    raise NotImplementedError("Unable to parse uri to data resource: " + uri)


@resource.register('.+::.+', priority=15)
def resource_split(uri, *args, **kwargs):
    uri, other = uri.rsplit('::', 1)
    return resource(uri, other, *args, **kwargs)


def resource_matches(path, name):
    """
    return my resource uri removing the head of the uri

    require a match on the name if the head of the uri is found
    """

    pat = '^(.*://)'
    m = re.search(pat,path)
    if m:
        if not path.startswith(name):
            raise NotImplementedError("cannot handle this resource match")
        path = re.sub(pat,'',path)
    return path
