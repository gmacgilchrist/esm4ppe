"""
Collection of functions to load or generate datasets with spatial masks on ESM4 grid
"""

import xarray as xr
import numpy as np
import re
from esm4ppe.version import sysconfig

def get_masks(name,static):
    if name=='LME':
        return _LMEmask()
    if name=='basin':
        return _basinmask(static)

def _LMEmask():
    path = sysconfig['datasetspathroot']+'/LargeMarineEcos/derived_masks/LME66.ESM4.nc'
    return xr.open_dataset(path)

def _basinmask(static,geolon='geolon',geolat='geolat'):
    ''' Use static basin and geolon and geolat to generate mask booleans for a range of ocean regions.'''
    masks = xr.Dataset()
    flag_values = static['basin'].attrs['flag_values'].split(' ')
    flag_meanings = static['basin'].attrs['flag_meanings'].split(' ')
    latbounds = np.array([-60, -45, -20, 20, 45, 60])
    latnames = ['SoP', 'SoSubP', 'SoSubT','T','NoSubT','NoSubP','NoP']
    lonbounds = np.array([-214,-68,20])
    lonnames = ['pacific','atlantic','indian']
    lonbounds_arctic = np.array([-220 , -50])
    lonnames_arctic = ['pacific','atlantic']

    for b in flag_values:
        bi = int(b)
        name = flag_meanings[bi].split('_')[0]
        masks[name]=(static['basin']==bi)
        # Reverse boolean for land mask
        if name=='global':
            masks[name]=~masks[name]
        # Split up by latitudes
        for li,latname in enumerate(latnames):
            if (li == 0):
                newcond = (static[geolat] <= latbounds[li])
            elif (li == len(latnames)-1):
                newcond = (static[geolat] > latbounds[li-1])
            else:
                newcond = (static[geolat] > latbounds[li-1])  & (static[geolat] <= latbounds[li])
            masks[name+'_'+latname] = (masks[name]) & (newcond)
        # Split up Southern Ocean by longitude
        if name=='southern':
            for li,lonname in enumerate(lonnames):
                if li==len(lonnames)-1: # Wrap
                    newcond = (static[geolon]>lonbounds[li]) | (static[geolon]<lonbounds[0])
                else:
                    newcond = (static[geolon]>lonbounds[li]) & (static[geolon]<=lonbounds[li+1])
                masks[name+'_'+lonname] = (masks[name]) & (newcond) 
        # Split up Arctic Ocean by longitude
        if name=='arctic':
            for li,lonname in enumerate(lonnames_arctic):
                if li==len(lonnames_arctic)-1: # Wrap
                    newcond = (static[geolon]>lonbounds_arctic[li]) | (static[geolon]<lonbounds_arctic[0])
                else:
                    newcond = (static[geolon]>lonbounds_arctic[li]) & (static[geolon]<=lonbounds_arctic[li+1])
                masks[name+'_'+lonname] = (masks[name]) & (newcond)

    # Now build global basin masks
    for name in ['pacific','atlantic','indian']:
        masks[name+'_global']=masks[name].copy()
        r = re.compile(".*"+name)
        bs = list(filter(r.match, list(masks.keys())))
        for b in bs:
            masks[name+'_global']+=masks[b].copy()
            
    # Now remove any empty masks
    masksnow = masks.copy()
    for name, mask in masks.items():
        if ~mask.any():
            masksnow = masksnow.drop(name)
            
    return masksnow