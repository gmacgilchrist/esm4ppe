""" A collection of functions for opening and preliminary processing of the ESM4 PPE data. """

import xarray as xr
import numpy as np
import cftime
import datetime
import time

from esm4ppe.version import sysconfig
from esm4ppe.organization import *

def preprocess_climpred(ds,leadunits='months'):
    """
    Preprocessing function for use with xr.open_mfdataset. This processsing transforms the 
    dataset being opened into one that is compatible with the climpred format.
    """
    nt = len(ds['time'])
    leadunitsperyear = {'years':1,
                        'months':12,
                        'days':365}
    # Recover ensemble details from "title" attribute
    title = ds.attrs['title']
    member = int(title.split('-')[-1])
    ensembleid = title.split('-')[-2]
    startyear = int(ensembleid[0:4])
    startmonth = int(ensembleid[4:6])
    # Check if getting first saved pp file
    yearssincestart = ds['time'][0].dt.year.values-startyear
    if yearssincestart==0:
        leadstart = 1
    else:
        leadstart = yearssincestart*leadunitsperyear[leadunits]+1
        
    # Expand init dimension
    init = cftime.DatetimeNoLeap(startyear,startmonth,1)
    ds = ds.expand_dims({'init':[init]})
    # Expand member dimension
    ds = ds.expand_dims({'member':[member]})
    # Assign "lead" coordinate
    ds = ds.rename({'time':'lead'}).assign_coords({'lead':np.arange(leadstart,leadstart+nt)})
    ds['lead'].attrs['units']=leadunits
        
    ds = ds.chunk({'lead':-1})
    return ds

def add_controlasmember(ds,control):
    """
    Add the control simulation, over the appropriate time slice, as an ensemble member when
    opening the ensemble.
    """
    cs = ds.isel(member=0).copy().expand_dims({'member':[0]})
    for init in ds['init']:
        # Get appropriate slice
        if init.dt.month==1:
            nlead = len(ds['lead'])
        else:
            nlead = 36
        timeslice = slice(
            ds['time_bnds'].sel(init=init).isel(nv=0,member=0,lead=0).values,
            ds['time_bnds'].sel(init=init).isel(nv=1,member=0,lead=nlead-1).values)
        daslice = control.sel(time=timeslice)
        daslice = daslice.rename({'time':'lead'}).assign_coords({'lead':np.arange(1,nlead+1)})
        # Add slice to "controlmember" dataset
        da = cs.sel(init=init)
        da.loc[{'lead':np.arange(1,nlead+1)}]=daslice
        cs.loc[{'init':init}] = da
    return xr.concat([cs,ds],dim='member')

def open_control(variable,frequency,constraint=None):
    """
    Open the control simulation. The function first checks first if the files have been 
    migrated from tape and raises an exception if not.
    """
    pathDict = get_pathDict(variable,frequency,constraint=constraint)
    path = gu.core.get_pathspp(**pathDict)
    ondisk = gu.core.query_ondisk(path)
    if list(ondisk.values()).count(False)>0:
        raise Exception("Not all files (CONTROL) available on disk. Use issue_dmget_ensemble(variable,frequency,...) to migrate from tape.")
    return xr.open_mfdataset(path)

def open_static(variable,frequency,constraint=None):
    """
    Open the relevant static grid file.
    """
    pathDict = get_pathDict(variable,frequency,constraint=constraint)
    return gu.core.open_static(pathDict['pp'],pathDict['ppname'])

def open_ensemble(variable,frequency,constraint=None,startyear="*",startmonth=None,controlasmember=True):
    """
    Open the ensemble data for the given variable and frequency, and for the specified startyear and start month.
    If no start year and month are specified then the function returns the full ensemble.
    """
    pathDict = get_pathDict(variable,frequency,constraint,startyear,startmonth)
    path = gu.core.get_pathspp(**pathDict)
    # Check to see if files are on disk or tape
    ondisk = gu.core.query_ondisk(path)
    if list(ondisk.values()).count(False)>0:
        raise Exception("Not all files (ENSEMBLES) available on disk. Use issue_dmget_esm4ppe(variable,frequency,...) to migrate from tape.")
    ds = xr.open_mfdataset(path,preprocess=preprocess_climpred)
    # Add control as member
    if controlasmember:
        control = open_control(variable,frequency,constraint)
        ds = add_controlasmember(ds,control)
    return ds

def issue_dmget_esm4ppe(variable,frequency,constraint=None,startyear=None,startmonth=None,wait=False):
    """
    Issue a dmget command for the relevant variable and frequency and for the start year and month
    specified. If no start year or month is specified, this issues a dmget for the control simulation.
    The function first checks whether the data are already on disk, and only issues a dmget for
    data that are not already mitgrated.
    
    Specifying a * for the startyear and leaving startmonth=None will issue a dmget for the
    full ensemble.
    """
    pathDict = get_pathDict(variable,frequency,constraint,startyear,startmonth)
    path = gu.core.get_pathspp(**pathDict)
    ondisk = gu.core.query_ondisk(path)
    ontape = {key: value for key, value in ondisk.items() if value==False}
    if len(ontape)==0:
        print("Everything already on disk.")
        return
    else:
        print("Issuing dmget.")
        out = gu.core.issue_dmget(list(ontape.keys()))
        if wait:
            # Sleep for a moment to make sure the command has issued
            time.sleep(3)
            # Now check
            count = 0
            while gu.core.query_dmget()==1:
                count+=1
                if count%500==0:
                    print("Still in queue at :",end=" ")
                    print(datetime.datetime.now())
        return ontape.keys()

