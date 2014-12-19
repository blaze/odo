def wsum(x, weights):
    """Sum of `y` weighted by `x`."""
    return (weights * x).sum()


def wmean(x, weights):
    """Mean of `y` weighted by `x`"""
    return wsum(x, weights) / weights.sum()
