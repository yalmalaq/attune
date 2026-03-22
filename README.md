# ATTUNE sys

This is to explain how to prepare the environment and run Attune (attune_mobile.py / attune_odroid.py). It assumes you have already installed numpy and activated your Python virtualenv with:

source optimal_env_39/bin/activate

Follow the steps below to place model artifacts, set permissions, install any remaining dependencies, and run Attune on both mobile (Pixel/Android) and Odroid targets.

# What you need
Python virtualenv activated: source optimal_env_39/bin/activate

Model artifacts and configs placed next to the attune script:

farm_configs.npy, farm_columns.txt, farm_model_numpy.pkl

pipeline_configs.npy, pipeline_columns.txt, pipeline_model_numpy.pkl

Executables for workloads in the same folder:

farm, pipe_Nstages

Measurement wrapper used by your runs (e.g., INA219 wrapper) if you plan to run real measurements

Install recommended Python packages
Inside your activated env run:
pip install --upgrade pip
pip install numpy pandas

The scripts write CPU governor / freq sysfs entries. Run with sudo or ensure your user can write those sysfs paths (or keep GENERATE_ONLY=True for dry runs).


