def wsum(x, weights):
    """Sum of `x` weighted by `weights`."""
    return (weights * x).sum()


def wmean(x, weights):
    """Mean of `x` weighted by `weights`"""
    return wsum(x, weights) / weights.sum()
