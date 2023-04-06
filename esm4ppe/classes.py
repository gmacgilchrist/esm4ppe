""" Module for the esm4ppeObj class """
import xarray as xr
import climpred as cp
from dask.diagnostics import ProgressBar

from esm4ppe.version import sysconfig
from esm4ppe.processing import *

class esm4ppeObj:
    def __init__(self,variable,frequency,constraint=None,triggeropen=False,add_control=False):
        zarrpath = sysconfig['zarrpathroot']+get_zarrdir(variable,frequency)
        try:
            ds = xr.open_zarr(zarrpath)[variable]
            print("Opening zarr store.")
        except:
            if triggeropen:
                print("Opening ensemble...",end=" ")
                start = time.time()
                ds = open_ensemble(variable,frequency,constraint,controlasmember=True)
                end = time.time()
                print("...Ensemble opened. Elapsed time: "+str(round(end-start))+" seconds.")
                ds = ds.drop(['time_bnds','nv']).chunk({'member':-1,'init':-1,'lead':1,'xh':"auto"})
                print("Saving to zarr store...",end=" ")
                with ProgressBar():
                    ds.to_zarr(zarrpath,mode='a')
                print("zarr store saved.")
            else:
                raise Exception("Zarr store not available for "+
                                variable+" at zarrpath "+
                                get_zarrpath(variable,frequency)+"."+
                                "Set triggeropen=True to open and save the ensemble."+
                                " This can take some time to process.")
        
        if type(ds)==xr.DataArray:
            ds = ds.to_dataset()
        self._ds = ds
        
        self.args = {'variable':variable,'frequency':frequency,'constraint':constraint}
        self.zarrpath = zarrpath
        
        if add_control:
            print("Opening control...",end=" ")
            self._control = open_control(variable,frequency,constraint)
            print("control opened.")
        
        print("Opening static...",end=" ")
        self._static = open_static(variable,frequency,constraint)
        print("static opened.")
        
        
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