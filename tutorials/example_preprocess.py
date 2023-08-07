#!/nbhome/gam/miniconda3/envs/climpred_clean/bin/python
# Python file to preprocess data for a number of variables
import esm4ppe

variables = ['tos','sos','intpp'] # Sea surface temperature, salinity, and depth-integrated primary production
frequency = 'monthly'

# Note, occasionally, dmget will fail on a few files. If this happens, the add_* command will fail. This is a
# system error and can't be avoided. The issue_dmget command has to be reissued until all of the files are
# succesfully retrieved from tape.

# While this script is running, issuing other dmget commands will confuse the waiting function of issue_dmget
# (which simply checks fo the user's name in the dmget queue.

# An alternative approach is the issue all of the dmget's at once, with `wait=False` (this can be done in a 
# notebook rather than as a batch job) and once all of the data is retrieved, run this script to do just the
# processing of ensemble and control data (i.e. comment out the issue_dmget command).

for variable in variables:
    print(variable)
    es = esm4ppe.esm4ppeObj(variable,frequency)
    es.issue_dmget(wait=True)
    # open the ensemble and the control, save to zarr
    es = es.add_ensemble(triggeropen=True).add_control(triggeropen=True)