#!/usr/bin/env python


if __name__ == '__main__':
    import os
    import kdbpy
    from kdbpy.kdb import which
    assert which('q.bat' if os.name == 'nt' else 'q')
    kdbpy.test()
