# esm4ppe
Python package for working with ESM4 perfect predictability experiments. This is predominantly a light wrapper around [climpred](https://climpred.readthedocs.io/en/stable/), with some additional functionality to work efficiently with the PP/AN file structure.

## Notes on installation and package requirements
Most of the functionality relies only on [xarray](https://docs.xarray.dev/en/stable/), so will work in any environment with a reasonably up-to-date version of this. To use the [climpred](https://climpred.readthedocs.io/en/stable/) functionality, you need to have this installed. As of 6/4/23, there are dependencies issues between `climpred`, `numba`, and `python` (see issue #815 [here](https://github.com/pangeo-data/climpred/issues/815)).  To install a clean, workable version of `climpred`, I recommend setting up a new environment, installing `climpred`, then installing additional packages as required:
```
conda create -n climpred_clean
conda activate climpred_clean
conda install -c conda-forge climpred
```
### Required packages
Presently, the package is set up in a very basic manner and I have not set it up to recognize all of its nested dependencies. Here is a list of packages (in addition to `climpred`) that I believe to be required in the conda environment in which you're working.
```
xarray
netcdf4
zarr
numpy
cftime
```
Depending on your environment set-up, you may also need to install `jupyterlab` or `ipykernel`.

You can install these packages in the `climpred_clean` environment using `conda install -c conda-forge PACKAGE_NAMES`, where `PACKAGE_NAMES` is replaced with the required packages.

Additionally, `esm4ppe` depends on the `gfdl_utils` package, which is a basic package for navigating the filestructure on PP/AN. This package can be found [here](https://github.com/gmacgilchrist/gfdl_utils). Clone that repository to your local machine, and install it in the `climpred_clean` environment by issuing `pip install -e .` from within the repository.

## Installing the `esm4ppe` package
1. Clone this repository
2. In the `esm4ppe` subfolder, edit the `version.py` file to point towards directories that you have write permissions for. I recommend creating a directory on the `/work` filesystem on PP/AN. This is the location where `esm4ppe` can save raw and processed data, as well as check to see if those data are already saved. See the settings in the original `version.py` for an idea of this file structure. Note that you will have to make these directories locally (`esm4ppe` will not do that for you).
3. In the main `esm4ppe` repository folder (and while the `climpred_clean` environment is activated) issue ```pip install -e .```.
4. In a jupyter notebook, you should now be able to import the `esm4ppe` module.

