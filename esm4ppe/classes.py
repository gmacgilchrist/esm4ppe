""" Module for the esm4ppeObj class """
import xarray as xr
import climpred as cp
import time
from dask.diagnostics import ProgressBar

from esm4ppe.version import sysconfig
from esm4ppe.processing import *
from esm4ppe.calculations import *
from esm4ppe.organization import *

class esm4ppeObj:
    def __init__(self,variable,frequency,constraint=None):
        
        self.variable = variable
        self.frequency = frequency
        self.constraint = constraint
        
        print("Opening static...",end=" ")
        self.static = open_static(variable,frequency,constraint)
        print("static opened.")
        
    def add_ensemble(self,triggeropen=False,startyear='*',startmonth=None,controlasmember=True):
        
        zarrpath = sysconfig['zarrpathroot']+get_zarrdir(self.variable,self.frequency)
        self.zarrpath = zarrpath
        
        try:
            ensemble = xr.open_zarr(zarrpath)[self.variable]
            print("Opening zarr store.")
        except:
            if triggeropen:
                print("Opening ensemble...",end=" ")
                start = time.time()
                ensemble = open_ensemble(self.variable,self.frequency,self.constraint,
                                         controlasmember=controlasmember,
                                         startyear=startyear,startmonth=startmonth)
                end = time.time()
                print("...Ensemble opened. Elapsed time: "+str(round(end-start))+" seconds.")
                ensemble = ensemble.drop(['time_bnds','nv']).chunk({'member':-1,'init':-1,'lead':1,'xh':"auto"})
                print("Saving to zarr store...",end=" ")
                with ProgressBar():
                    ensemble.to_zarr(zarrpath,mode='a')
                print("zarr store saved.")
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
        
    def add_control(self):
        print("Opening control...",end=" ")
        self.control = open_control(self.variable,self.frequency,self.constraint)
        print("control opened.")
        return self
        
    def verify(self,metric,saveskill=False,**pm_args):
        verifypath = get_verifypath(metric,**pm_args)
        path = verifypath+'/'+get_ncfilename(self.args)         
        try:
            vs = xr.open_dataset(path)
            print("Opening skill metric.")
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
        self._vs = vs
        self.verifypath = verifypath
        return vs
    
    def issue_dmget(self,dataset=None):
        if dataset is None:
            print('Issuing dmget for both ensemble and control')
            issue_dmget_esm4ppe(self.variable,self.frequency,self.constraint,startyear='*')
            issue_dmget_esm4ppe(self.variable,self.frequency,self.constraint)
        elif dataset=='ensemble':
            issue_dmget_esm4ppe(self.variable,self.frequency,self.constraint,startyear='*')
        elif dataset=='control':
            issue_dmget_esm4ppe(self.variable,self.frequency,self.constraint)
    
    def _verify(self,metric,**pm_args):
        if metric=='ppp':
            if 'groupby' in pm_args.keys():
                groupby=pm_args['groupby']
            else:
                groupby=None
            return calc_ppp(self._ds,self._control,groupby,self.args['frequency'],len(self._ds['lead']))
        else:
            pm = cp.PerfectModelEnsemble(self._ds)
            return pm.verify(metric=metric,**pm_args)