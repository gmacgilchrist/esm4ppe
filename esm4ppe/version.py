"""esm4ppe: version information"""

__version__ = "0.0.1"

global sysconfig

sysconfig = {}
sysconfig['os'] = 'della'
if sysconfig['os']=='della':
    basedirdatasets='/projects/SOCCOM/datasets'
    basedirarchive = basedirdatasets+'/archive'
    basedirwork = '/projects/SOCCOM/graemem'
elif sysconfig['os']=='ppan':
    basedirdatasets='/work/gam'
    basedirarchive = '/archive'
    basedirwork = '/work/gam'

sysconfig['rootdir'] = basedirarchive+'/Richard.Slater/xanadu_esm4_20190304_mom6_ESM4_v1.0.3_rc1'
sysconfig['prod'] = 'gfdl.ncrc4-intel18-prod-openmp'
sysconfig['configid_control'] = 'ESM4_piControl_D'
sysconfig['zarrpathroot'] = basedirwork+'/projects/esm4_ppe/data/raw'
sysconfig['verifypathroot'] = basedirwork+'/projects/esm4_ppe/data/processed/verify'
sysconfig['correlationpathroot'] = basedirwork+'/projects/esm4_ppe/data/processed/correlation'
sysconfig['regionalmeanpathroot'] = basedirwork+'/projects/esm4_ppe/data/processed/regionalmean'
sysconfig['climatologypathroot'] = basedirwork+'/projects/esm4_ppe/data/processed/climatology/seasonal'
sysconfig['otherpathroot'] = basedirwork+'/projects/esm4_ppe/data/processed/other'

sysconfig['datasetspathroot'] = basedirdatasets

# limit the amount of daily data that is opened and saved
sysconfig['nt_fordaily'] = 365 # set to None if you want all data
