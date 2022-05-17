# pdg_ray_workflow
Ray PDG's Viz-staging, Viz-Raster and Viz-3D in parallel using Ray Core and Ray Workflows.

### requirements

Run this to install dependencies:
```
conda env create --file environment.yml
```
TODO: the current env has wayyy to many specific dependencies. Should clean this up to only the necessary ones. 

### contributing

Documenting an env (best way to create environment.yml):
```
conda env export | grep -v "^prefix: " > environment.yml
```
