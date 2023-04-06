# esm4ppe
Python package for working with ESM4 perfect predictability experiments. This is predominantly a light wrapper around [climpred](https://climpred.readthedocs.io/en/stable/), with some additional functionality to work efficiently with the PP/AN file structure.

## Notes on installation and package requirements
Most of the functionality relies only on [xarray](https://docs.xarray.dev/en/stable/), so will work in any environment with a reasonably up-to-date version of this. To use the [climpred](https://climpred.readthedocs.io/en/stable/) functionality, you need to have this installed. As of 6/4/23, there are dependencies issues between `climpred`, `numba`, and `python` (see issue #815 [here](https://github.com/pangeo-data/climpred/issues/815)).  To install a clean, workable version of `climpred`, I recommend setting up a new environment, installing `climpred`, then installing additional packages as required:
```
conda create -n climpred_clean
conda activate climpred_clean
conda install -c conda-forge climpred
```