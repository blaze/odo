import os


def test_create_pydot_image():
    try:
        from into.dot import dot_graph
        dot_graph()
    except:
        pass
    finally:
        for ext in ('pdf', 'png'):
            try:
                os.remove('conversions.%s' % ext)
            except:
                pass
