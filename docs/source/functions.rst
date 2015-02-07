Five Operations
===============

The Blaze project originally included ``into`` a magic function that
transformed data between containers.  This function was both sufficiently
useful and sufficiently magical that it was moved to a separate project, its
functionality separated into three operations

1.  ``convert``: Transform dataset to a new type.
    ``convert(list, (1, 2, 3))``
2.  ``append``: Append a dataset to another.
    ``append([], (1, 2, 3))``
3.  ``resource``: Obtain or create dataset from a URI string
    ``resource('myfile.csv')``

These are magically tied together as the original ``into`` function

4.  ``into``: Put stuff into other stuff (deliberately vague/magical.)
