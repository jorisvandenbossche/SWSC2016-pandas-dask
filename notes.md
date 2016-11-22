## Setting-up

- Connect with the HPC 
  + `ssh vsc40xxx@login.hpc.ugent.be`
- Switch cluster 
  + `ml swap cluster/golett`
- Start a job for this workshop (
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

- Connect to the jupyter notebook (http://hod.readthedocs.io/en/latest/Connecting_to_web_UIs.html)
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
