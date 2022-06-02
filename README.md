# pdg_ray_workflow
Ray PDG's Viz-staging, Viz-Raster and Viz-3D in parallel using Ray Core and Ray Workflows.

### requirements

Run this to install dependencies (only tested on x86_64):
```
conda env create --file environment_cross_platform.yml
```
TODO: Reduce strictness of env.yml requirements.

### contributing

Documenting an env (best way to create environment.yml):
```
conda env export | grep -v "^prefix: " > environment.yml
```
