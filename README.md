# pdg_ray_workflow
Ray PDG's Viz-staging, Viz-Raster and Viz-3D in parallel using Ray Core and Ray Workflows.

### requirements

Run this to install dependencies (only tested on x86_64):
```
conda env create --file environment_cross_platform.yml
```
TODO: Reduce strictness of env.yml requirements.


## Running 

```
# Start head node. It will also act as a worker node by default.
ray start --head --port=6379 --dashboard-port=8265
# Start worker nodes

```

### port forward ray dashboard

1. Login to a login node

```
ssh <👉YOUR_NCSA_USERNAME👈>@login.delta.ncsa.illinois.edu
```
2. Start a Slurm job. `cpus-per-task` must be large (128 maximum on Delta) for Ray to scale well.

```
# max CPU node request (for single node)
srun --account=<👉YOUR_CPU_ACCOUNT👈> --partition=cpu \
--nodes=1 --tasks=1 --tasks-per-node=1 \
--cpus-per-task=128 --mem=240g \
--pty bash
```

3. Then SSH into the compute node you have running Ray. 

```
ssh cn001 (for example)
```
Forward port from compute node to your local personal computer:
```
ssh -L 8265:localhost:8265 <local_username>@<your_locaL_machine> 

# Navigate your web browser to: localhost:8265/
```

### contributing

Documenting an env (best way to create environment.yml):
```
conda env export | grep -v "^prefix: " > environment.yml
```
