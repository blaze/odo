from into.append import append

def test_append_list():
    L = [1, 2, 3]
    append(L, [4, 5, 6])
    assert L == [1, 2, 3, 4, 5, 6]
