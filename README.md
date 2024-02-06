# esm4ppe
Python package for working with ESM4 perfect predictability experiments. This is predominantly a light wrapper around [climpred](https://climpred.readthedocs.io/en/stable/), with some additional functionality to work efficiently with the PP/AN file structure.

## Notes on installation and package requirements
Most of the functionality relies only on [xarray](https://docs.xarray.dev/en/stable/), so will work in any environment with a reasonably up-to-date version of this. You will also need to have some back-end packages installed, including `netcdf` and `zarr`, as well as `cftime` for handling calendars. To use the [climpred](https://climpred.readthedocs.io/en/stable/) functionality, you need to have this installed. Depending on your environment set-up, you may also need to install `jupyterlab` or `ipykernel`.

Additionally, `esm4ppe` depends on the `gfdl_utils` package, which is a basic package for navigating the filestructure on PP/AN. This package can be found [here](https://github.com/gmacgilchrist/gfdl_utils). Clone that repository to your local machine, and install it in your environment by issuing `pip install -e .` from within the repository.

## Installing the `esm4ppe` package
1. Clone this repository
2. In the `esm4ppe` subfolder, edit the `version.py` file to point towards directories that you have write permissions for. I recommend creating a directory on the `/work` filesystem on PP/AN. This is the location where `esm4ppe` can save raw and processed data, as well as check to see if those data are already saved. See the settings in the original `version.py` for an idea of this file structure. Note that you will have to make these directories locally (`esm4ppe` will not do that for you).
3. In the main `esm4ppe` repository folder (and while the `climpred_clean` environment is activated) issue ```pip install -e .```.
4. In a jupyter notebook, you should now be able to import the `esm4ppe` module.

