""" Collection of functions for navigating the ESM4 PPE file structure on PP/AN """

import re
import gfdl_utils as gu

from esm4ppe.version import sysconfig

### READING FROM RAW DATA ###
def get_ensembleid(startyearstr,startmonthstr):
    """
    Return the ensemble ID based on the start year and startmonth.
    """
    return startyearstr+startmonthstr+'01'

def get_memberid(startyear,startmonth,member):
    """
    Return the member ID based on the startyear, startmonth, and member number.
    If an entry is None, a wildcard is placed in the member ID in its place.
    """
    memberdict=dict(startyear=startyear,startmonth=startmonth,member=member)
    zfilldict=dict(startyear=4,startmonth=2,member=2)
    memberstrdict=memberdict.copy()
    for key,value in memberdict.items():
        if value is None:
            memberstrdict[key]='*'
        elif isinstance(value,int):
            memberstrdict[key] = str(value).zfill(zfilldict[key])
    return '-'.join([get_ensembleid(memberstrdict['startyear'],memberstrdict['startmonth']),memberstrdict['member']])

def get_configid(startyear=None,startmonth=None,member=None):
    """
    Return the config_id for either the control simulation (when all arguments
    are None) or for the specified ensemble member.
    """
    configid_control = sysconfig['configid_control']
    if all(n is None for n in [startyear,startmonth,member]):
        return configid_control
    else:
        return '-'.join([configid_control,'ensemble',get_memberid(startyear,startmonth,member)])
    
def get_pp(startyear=None,startmonth=None,member=None):
    """
    Return the postprocess path for the ensemble or control.
    """
    prod=sysconfig['prod']
    return '/'.join([sysconfig['rootdir'],get_configid(startyear,startmonth,member),prod,'pp'])

def get_refpp(startmonth):
    """
    Define a reference pp. This is for, for example, searching through available 
    diagnostics. Appropriate reference pp varies depending on ensemble's startmonth.
    """
    # Note that these could be changed or user-specified
    if startmonth is None:
        return get_pp() # Control
    else:
        return get_pp(123,startmonth,1)
    
def get_ppname(variable, frequency, pp=None, constraint=None):
    """
    Return the ppname appropriate for the given variable and frequency. When 
    there is potential for a clash (for example a variable with the same name 
    in both an ocean and an atmosphere pp file at the same time frequency), the
    user can specify a constraint: a string that must be found in the ppname.
    """
    if (pp is None) or (bool(re.search("\*",pp))):
        # Use main reference pp
        # For the purposes of finding the ppname, startmonth is irrelevant
        # so it isn't carried here
        pp = get_pp()
    ppnames = gu.core.find_variable(pp,variable)
    if len(ppnames)==1:
        ppname = ppnames[0]
        if confirm_frequency(pp,ppname,frequency):
            return ppname
        else:
            raise Exception(("Variable {"+variable
                             +"} only found in {"+ppname
                             +"}, which has frequency {"+gu.core.get_timefrequency(pp,ppname)
                             +"} but frequency {"+frequency
                             +"} was requested."))
    for ppname in ppnames:
        
        if constraint is None:
            condition = confirm_frequency(pp,ppname,frequency)
        else:
            condition = confirm_frequency(pp,ppname,frequency) & (bool(re.search(constraint,ppname)))
                
        if condition:
            return ppname
        else:
            continue
            
    exceptionstr = ("No suitable ppname found for {"+variable
                     +"} at frequency {"+frequency+"}")
    if constraint is not None:
        exceptionstr+=" with constraint {'"+constraint+"'}"
    raise Exception(exceptionstr)

def get_local(pp,ppname,startmonth):
    """
    Return the local directory stucture for the given pp and ppname.
    """
    if bool(re.search("\*",pp)):
        # Use reference pp
        pp = get_refpp(startmonth)
    local = gu.core.get_local(pp,ppname,out='ts')
    if (startmonth is None) or (startmonth=='*'):
        local = '/'.join([local.split('/')[0],'*yr'])
    return local

def confirm_frequency(pp,ppname,frequency):
    """
    Check that the frequency of the ppname matches the desired frequency.
    """
    return gu.core.get_timefrequency(pp,ppname)==frequency

def get_pathDict(variable,frequency,constraint=None,startyear=None,startmonth=None,member=None,time='*',out='ts'):
    """
    Return a pathDict dictionary for the given variable and frequency, and for the
    ensemble characteristics given. The pathDict dictionary contains all of the relevant 
    key-value pairs to be used with gfdl_utils functions.
    
    If the ensemble characteristics (startyear, startmonth, member) are left blank, the
    pathDict for the control simulation is returned.
    """
    pp = get_pp(startyear,startmonth,member)
    ppname = get_ppname(variable,frequency,constraint)
    local = get_local(pp,ppname,startmonth)
    return {'pp':pp,
            'ppname':ppname,
            'out':out,
            'local':local,
            'time':time,
            'add':variable}


### READING/WRITING DERIVED DATA ###
def get_verifypath(metric,**pm_args):
    """"
    Return the directory for a skill metric.
    """
    rootpath = '/work/gam/projects/esm4_ppe/data/processed/verify/'
    verifypathstrings = ['verify','metric-'+metric]
    for key,value in pm_args.items():
        if value==None:
            continue
        elif type(value)==list:
            string = '-'.join([key,'_'.join(value)])
        else:
            string= '-'.join([key,value])
        verifypathstrings.append(string)
    return sysconfig['verifypathroot']+'.'.join(verifypathstrings)

def get_filenamelist(variable,frequency):
    """
    Return the filename for saving a netcdf file given the variable, frequency and constraint.
    """
    modelcomponent = get_modelcomponent(variable,frequency)
    tmp = [modelcomponent,variable,frequency]
    return tmp

def build_ncfilename(filenamelist):
    return '.'.join(filenamelist)+'.nc'

def get_zarrdir(variable,frequency):
    """
    Return the zarr store directory name.
    """
    modelcomponent = get_modelcomponent(variable,frequency)
    return '.'.join([modelcomponent,frequency])

def get_modelcomponent(variable,frequency):
    """
    Return the name of the model component for the given variable.
    """
    ppname = get_ppname(variable,frequency).split('_')
    return ppname[0]