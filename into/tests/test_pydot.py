

def test_create_pydot_image():
    try:
        from into.dot import dot_graph
        dot_graph()
    except:
        pass

