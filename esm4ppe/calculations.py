"""
Collection of calculations for operating on ESM4 PPE simulations.
"""

from esm4ppe.version import sysconfig
from esm4ppe.utils import *

import xarray as xr
import numpy as np
from dask.diagnostics import ProgressBar

def calc_ensemblevariance(ds,groupby=None):
    """
    Calculate the mean ensemble variance. groupby refers to grouping the initializations,
    and the mean is taken for each group separately.
    """
    evar = (ds.std(dim='member'))**2
    if groupby is not None:
        return evar.groupby('init.'+groupby).mean()
    else:
        return evar.mean('init')

def calc_controlvariance(control,frequency,nlead):
    """
    Calculate the variance of the control simulation in a climatology relevant to the 
    specified frequency. For example, frequency='monthly' would return the seasonal climatology.
    The variance is propagated so that its length in "time" or "lead" is the same as that of
    the ensemble (given by nlead). So a monthly climatology would be repeated nlead/12 times.
    """
    if frequency=='monthly':
        tmp = (control.groupby('time.month').std()**2)
        # Broadcast to nlead/12 repeating years
        montharray = np.tile(tmp['month'].to_numpy(),int(nlead/12))
        tmp = tmp.sel(month=montharray).rename({'month':'lead'})
        cvar = tmp.assign_coords({'lead':np.arange(1,nlead+1)})
    elif frequency=='annual':
        tmp = (control.std('time')**2).expand_dims({'lead':1})
        tmp = tmp.isel(time=[0]*n)
        cvar = tmp.assign_coords({'lead':np.arange(1,nlead+1)})
    return cvar
        
def calc_ppp(ds,control,groupby,frequency,nlead):
    """
    Calculate the potential prognostic predictability.
    
    If groupby is specified, it refers to grouping the ensemble variance by its initialization 
    month. In this case, the control variance has to be expanded to include a 'groupby' 
    dimension and then shifted to match the appropriate months.
    """
    # Ensemble variance
    evarmean = calc_ensemblevariance(ds,groupby)

    # Control variance
    cvar = calc_controlvariance(control,frequency,nlead)
    # If necessary, shift control variance to align with init month
    if groupby=='month':
        cvar_month = cvar.expand_dims({'month':evarmean['month']})
        for m in cvar_month['month'].values:
            cvar_month.loc[{'month':m}]=cvar.roll(lead=-1*(m-1))
    # PPP
    ppp = 1-(evarmean/cvar_month)
    return ppp

def _calc_regionalmean(da,mask,weights):
    dims = get_dimensionslesstime(da)
    return da.where(mask,drop=True).weighted(weights.fillna(0)).mean(dims)

def calc_regionalmean(da,masks,weights,verbose=False):
    ''' Calculate regional means for [da] based on [masks].
    Return DataArray with "region" dimension corresponding to masknames.'''
    dims_out = get_dimensionslessxy(da)
    coords_out = {}
    for d in dims_out:
        coords_out[d] = da.coords[d]
    coords_out['region']=list(masks.keys())
    
    da_out = xr.DataArray(dims=coords_out.keys(),coords=coords_out,name=da.name)
        
    for name,mask in masks.items():
        if verbose:
            print(name, end = ' ')
            da_out.loc[{'region':name}] = _calc_regionalmean(da,mask,weights).transpose(*dims_out)
        else:
            da_out.loc[{'region':name}] = _calc_regionalmean(da,mask,weights).transpose(*dims_out)
    return da_out

def calc_climatology(ds,groupby):
    """
    Calculate the climatology of [ds] within groupings of [groupby].
    [groupby] should be "time.month" for monthly data and
    "time.dayofyear" for daily data.\
    """
    return ds.groupby(groupby).mean()