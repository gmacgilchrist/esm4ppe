"""
Collection of miscellaneous utility functions
"""

def get_dimensionslesstime(da):
    return [dim for dim in list(da.dims) if dim not in ['time','month','lead']]

def get_dimensionslessxy(da):
    return [dim for dim in list(da.dims) if (dim!='xh') & (dim!='yh')]