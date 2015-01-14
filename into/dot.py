from __future__ import absolute_import, division, print_function

import os
import networkx as nx

from math import log
from toolz import pluck
from .convert import convert, ooc_types
from .append import append
from .utils import cls_name


# Write out as dot
def dot_graph(filename='conversions'):
    # Edges from Convert
    dg = nx.DiGraph()
    for a, b in convert.graph.edges():
        cost = convert.graph.edge[a][b]['cost']
        dg.add_edge(cls_name(a), cls_name(b),
                    cost=cost,
                    penwidth=max(log(1./(cost + 0.06)), 1))


    # Edges from Append
    for a, b in append.funcs:
        if b is not object and a != b:
            dg.add_edge(cls_name(b), cls_name(a), color='blue')


    # Color edges
    for n in convert.graph.nodes() + list(pluck(0, append.funcs)):
        if issubclass(n, tuple(ooc_types)):
            dg.node[cls_name(n)]['color'] = 'red'

    # Convert to pydot
    p = nx.to_pydot(dg)

    p.set_overlap(False)
    p.set_splines(True)

    with open(filename + '.dot', 'w') as f:
        f.write(p.to_string())

    os.system('neato -Tpdf %s.dot -o %s.pdf' % (filename, filename))
    print("Writing graph to %s.pdf" % filename)
    os.system('neato -Tpng %s.dot -o %s.png' % (filename, filename))
    print("Writing graph to %s.png" % filename)

if __name__ == '__main__':
    dot_graph()
