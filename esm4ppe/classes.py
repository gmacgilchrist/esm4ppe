""" Module for the esm4ppeObj class """
import xarray as xr
import climpred as cp
import time
from dask.diagnostics import ProgressBar
import os

from esm4ppe.version import sysconfig
from esm4ppe.processing import *
from esm4ppe.calculations import *
from esm4ppe.organization import *
from esm4ppe.masks import get_masks

class esm4ppeObj:
    def __init__(self,variable,frequency,constraint=None):
        
        self.variable = variable
        self.frequency = frequency
        self.constraint = constraint
        self.modelcomponent = get_modelcomponent(variable,frequency)
        
        print("Opening static...",end=" ")
        self.static = open_static(variable,frequency,constraint)
        print("static opened.")
        
        self.coords = self.static.coords
        
    def add_ensemble(self,triggeropen=False,startyear='*',startmonth=None,controlasmember=True):
          
        zarrpath = sysconfig['zarrpathroot']+'climpred_zarr/'+get_zarrdir(self.variable,self.frequency)
        self.zarrpath_climpred = zarrpath
        
        try:
            ensemble = xr.open_zarr(zarrpath)[self.variable]
            print("Ensemble present in zarr store... opening... ensemble opened.")
        except:
            if triggeropen:
                print("Opening ensemble...",end=" ")
                start = time.time()
                ensemble = open_ensemble(self.variable,self.frequency,self.constraint,
                                         controlasmember=controlasmember,
                                         startyear=startyear,startmonth=startmonth)
                end = time.time()
                print("... ensemble opened. Elapsed time: "+str(round(end-start))+" seconds.")
                
                ensemble = ensemble.drop(['time_bnds','nv']).chunk({'member':-1,'init':-1,'lead':1,'xh':"auto"})
                print("Saving to zarr store...",end=" ")
                with ProgressBar():
                    ensemble.to_zarr(zarrpath,mode='a')
                print("zarr store saved... ensemble opened.")
            else:
                raise Exception("Zarr store not available for "+
                                self.variable+" at zarrpath "+
                                get_zarrdir(self.variable,self.frequency)+"."+
                                " Set triggeropen=True to open and save the ensemble."+
                                " This can take some time to process.")
        
        if type(ensemble)==xr.DataArray:
            ensemble = ensemble.to_dataset()
        self.ensemble = ensemble
        return self        
        
    def add_control(self,triggeropen=False):
        
        zarrpath = sysconfig['zarrpathroot']+'control_zarr/'+get_zarrdir(self.variable,self.frequency)
        self.zarrpath_control = zarrpath
        
        try:
            control = xr.open_zarr(zarrpath)[self.variable]
            print("Control present in zarr store... opening...",end=" ")
        except:
            if triggeropen:
                print("Opening control...",end=" ")
                control = open_control(self.variable,self.frequency,self.constraint)
                print("saving to zarr store...",end=" ")
                with ProgressBar():
                    control.to_zarr(zarrpath,mode='a')
                print("zarr store saved...")
            else:
                raise Exception("Zarr store not available for "+
                                self.variable+" at zarrpath "+
                                get_zarrdir(self.variable,self.frequency)+"."+
                                " Set triggeropen=True to open and save the ensemble."+
                                " This can take some time to process.")
        
        if type(control)==xr.DataArray:
            control = control.to_dataset()
        self.control = control
        print("control opened.")
        return self
        
    def verify(self,metric,saveskill=False,**pm_args):
        verifypath = get_verifypath(metric,**pm_args)
        filenamelist = get_filenamelist(self.variable,self.frequency)
        if hasattr(self,'masksname'):
            filenamelist.append(self.masksname)
        filename = build_ncfilename(filenamelist)
        path = verifypath+'/'+filename         
        try:
            vs = xr.open_dataset(path)
            print("Opening skill metric... skill metric opened.")
        except:
            vs = self._verify(metric,**pm_args)
            if saveskill:
                isExist = os.path.exists(verifypath)
                if not isExist:
                    os.mkdir(verifypath)
                print("Saving skill metric...",end=" ")
                with ProgressBar():
                    vs.to_netcdf(path)
                print("...skill metric saved")
        self.vs = vs
        self.verifypath = verifypath
        return self
    
    def _verify(self,metric,**pm_args):
        if metric=='ppp':
            if 'groupby' in pm_args.keys():
                groupby=pm_args['groupby']
            else:
                groupby=None
            return calc_ppp(self.ensemble,self.control,groupby,self.frequency,len(self.ensemble['lead']))
        else:
            pm = cp.PerfectModelEnsemble(self.ensemble)
            return pm.verify(metric=metric,**pm_args)
    
    def regionalmean(self,masksname,omit=None,saveregionalmean=False,verbose=False):
        # Get masks
        self.masksname = masksname
        masks = get_masks(self.masksname,self.static)
        # Specify file locations
        rmpathroot = sysconfig['regionalmeanpathroot']
        filenamelist = get_filenamelist(self.variable,self.frequency)
        filenamelist.append(masksname)
        filename = build_ncfilename(filenamelist)
        
        # Loop through datasets
        dsnamelist = ['control','ensemble','vs']
        if omit is not None:
            dsnamelist = [d for d in dsnamelist if d not in omit]
        for dsname in dsnamelist:
            if (hasattr(self,dsname)):
                if dsname == 'vs':
                    localdir = self.verifypath.split('/')[-1]
                else:
                    localdir = dsname
                rmpath = rmpathroot+localdir+'/'
                
                if dsname == 'control':
                    da = self.control
                elif dsname == 'ensemble':
                    da = self.ensemble
                elif dsname == 'vs':
                    da = self.vs
                    
                try:
                    da = xr.open_dataset(rmpath+filename)
                    print("Opening regional means of "+dsname+"... regional means of "+dsname+" opened.")
                except:
                    start = time.time()
                    print("Calculating regional means for "+dsname+"...",end=" ")
                    da = calc_regionalmean(da[self.variable],masks,self.static.areacello,verbose=verbose)
                    end = time.time()
                    print("...regional means for "+dsname+" calculated. Elapsed time: "+str(round(end-start))+" seconds.")
                    
                    if saveregionalmean:
                        isExist = os.path.exists(rmpath)
                        if not isExist:
                            os.mkdir(rmpath)
                        da.to_netcdf(rmpath+filename)
                    da = da.to_dataset()
                    
                if dsname == 'control':
                    self.control = da
                elif dsname == 'ensemble':
                    self.ensemble = da
                elif dsname == 'vs':
                    self.vs = da
        return self
                          
    def climatology(self,saveclimatology=True):
        climpathroot = sysconfig['climatologypathroot']
        filenamelist = get_filenamelist(self.variable,self.frequency)
        if hasattr(self,'masksname'):
            filenamelist.append(self.masksname)
        filename = build_ncfilename(filenamelist)
        climpath = climpathroot+'/'+filename
        try:
            clim = xr.open_dataset(climpath)
            print("Opening climatology... climatology opened.")
        except:
            if hasattr(self,'control'):
                print('Calculating climatology...',end=' ')
                
                if self.frequency=='monthly':
                    groupby='time.month'
                elif self.frequency=='daily':
                    groupby='time.dayofyear'
                    
                clim = calc_climatology(self.control,groupby)
                if saveclimatology:
                    print('... climatology calculated. Saving to netcdf...',end=' ')
                    with ProgressBar():
                        clim.to_netcdf(climpath)
                    print('...climatology saved.')
            else:
                raise Exception("Calculating the climatology requires that the control dataset is present in esm4ppObj.")
        self.control = clim
        return self
    
    def issue_dmget(self,dataset=None,wait=False):
        if dataset is None:
            print('Issuing dmget for both ensemble and control')
            issue_dmget_esm4ppe(self.variable,self.frequency,self.constraint,startyear='*',wait=wait)
            issue_dmget_esm4ppe(self.variable,self.frequency,self.constraint,wait=wait)
        elif dataset=='ensemble':
            issue_dmget_esm4ppe(self.variable,self.frequency,self.constraint,startyear='*',wait=wait)
        elif dataset=='control':
            issue_dmget_esm4ppe(self.variable,self.frequency,self.constraint,wait=wait)
    
