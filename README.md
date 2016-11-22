# Scaling your data analysis in Python with Pandas and Dask (21 November 2016)

The growing Python data science ecosystem, including the foundational packages Numpy and Pandas, provides powerful tools for data analysis that are widely used in a variety of applications. Typically, these libraries were designed for data that fits in memory and for computations that run on a single core.

Dask is a Python library for parallel and distributed computing, using blocked algorithms and task scheduling. By leveraging the existing Python data ecosystem, Dask enables to compute on arrays and dataframes that are larger than memory, while exploiting parallelism or distributed computing power, but in a familiar interface (mirroring Numpy arrays and Pandas dataframes).

This 1-day workshop will first give an introduction to the Python data tools, with an emphasis on Pandas, and then show with hands-on examples how those analyses can be scaled with Dask.

### Content

This workshop exists of two parts:

1. Introduction to data analysis with Pandas

  - Based on https://github.com/jorisvandenbossche/pandas-tutorial

2. Parallelizing and distributed computing with Dask

  - For a large part based on https://github.com/dask/dask-tutorial, thanks to Matthew Rocklin and contributors


## Setting-up with the HPC

- Connect with the HPC
  + `ssh vsc40xxx@login.hpc.ugent.be`
- Switch cluster
  + `ml swap cluster/golett`
- Start a job for this workshop
  + `qsub /apps/gent/tutorials/pandas_dask/job.sh` This will load the needed modules and start a jupyter notebook at port 8888
  + Check the job (`qstat -n`) and look up for the target node (e.g. `node2xxx`) were jupyter is running:

        ```
        master19.golett.gent.vsc:
                                                                                          Req'd       Req'd       Elap
        Job ID                  Username    Queue    Jobname          SessID  NDS   TSK   Memory      Time    S   Time
        ----------------------- ----------- -------- ---------------- ------ ----- ------ --------- --------- - ---------
        751301.master19.golett  vsc40xxx    short    job.sh            14392     2     48 754048716  08:00:00 R  00:03:26
           node2443+node2444
        ```
    For this example, this is node 2443.
- Connect to the jupyter notebook
  + Full instructions (especially for windows): http://hod.readthedocs.io/en/latest/Connecting_to_web_UIs.html)
  +  `ssh -L 8887:localhost:8888 node2443.golett.gent.vsc` Port forward (local computer)
  + Open a browser http://localhost:8887
- git clone this repo (in the cluster)
  + `git clone https://github.com/jorisvandenbossche/SWSC2016-pandas-dask.git`


### Scripts

**job.sh** (/apps/gent/tutorials/pandas_dask/job.sh)
```bash
#!/bin/bash
#PBS -l nodes=2:ppn=all
#PBS -l walltime=8:0:0
#PBS -W x=FLAGS:ADVRES:pandas.204

# load modules for IPython, pandas, dask, ...
source /apps/gent/tutorials/pandas_dask/modules.sh

# see https://ipywidgets.readthedocs.io/en/latest/user_install.html
jupyter nbextension install --py --user widgetsnbextension
jupyter nbextension enable --py --user widgetsnbextension

jupyter notebook --no-browser
```

**modules.sh** (/apps/gent/tutorials/pandas_dask/modules.sh)
```bash
echo "loading modules..."
module load bokeh/0.12.3-intel-2016b-Python-3.5.2
module load dask/0.12.0-intel-2016b-Python-3.5.2
module load distributed/1.14.3-intel-2016b-Python-3.5.2
module load IPython/5.1.0-intel-2016b-Python-3.5.2
module load matplotlib/1.5.2-intel-2016b-Python-3.5.2
module load pandas/0.19.1-intel-2016b-Python-3.5.2
module load PyTables/3.3.0-intel-2016b-Python-3.5.2
module load h5py/2.6.0-intel-2016b-Python-3.5.2-HDF5-1.8.17
module load Pillow/3.4.2-intel-2016b-Python-3.5.2-freetype-2.6.5
module load Graphviz/2.38.0-intel-2016b
module load graphviz/0.5.1-intel-2016b-Python-3.5.2
module list
```

### Setting-up a distributed cluster on your two nodes

- Start a scheduler on your primary node. Eg:

    ```
    ssh node2443.golett.gent.vsc
    source /apps/gent/tutorials/pandas_dask/modules.sh
    dask-scheduler
    ```

  This last command gives:

    ```
    [vsc40xxx@node2443 ~]$ dask-scheduler
    distributed.scheduler - INFO - -----------------------------------------------
    distributed.scheduler - INFO -   Scheduler at:         10.141.18.35:8786
    distributed.scheduler - INFO -        http at:         10.141.18.35:9786
    distributed.bokeh.application - INFO - Web UI: http://10.141.18.35:8787/status/
    ```

    The Scheduler address (`Scheduler at:         10.141.18.35:8786` in this example) has to be used to connect the workers and the Client in the notebook.

  - Start workers on both this node and your second node, specifying the Scheduler's address.

    ```
    ssh node2443.golett.gent.vsc
    source /apps/gent/tutorials/pandas_dask/modules.sh
    dask-worker 10.141.18.35:8786
    ```

    ```
    ssh node2444.golett.gent.vsc
    source /apps/gent/tutorials/pandas_dask/modules.sh
    dask-worker 10.141.18.35:8786
    ```

- Connect to the Scheduler in the notebook with:

    ```python
    from distributed import Client
    client = Client("10.141.18.35:8786")
    ```

As an alternative, you can also simply create a local cluster with `client = Client()` (without specifying a scheduler address), which will start a local distributed cluster on the node (or computer) you are working on.

### data

The NYCtaxi data are available on `/apps/gent/tutorials/pandas_dask/data`. When working on the HPC, you can copy them from there to a location that is available for your worker nodes (eg `$VSC_DATA`). If you are not using the HPC, you can download then (available as open data from the NYC gov, see instructions in the "04-Distributed-dataframes.ipynb" notebook).
