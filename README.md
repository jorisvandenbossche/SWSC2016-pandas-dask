# Scaling your data analysis in Python with Pandas and Dask (21 November 2016)

The growing Python data science ecosystem, including the foundational packages Numpy and Pandas, provides powerful tools for data analysis that are widely used in a variety of applications. Typically, these libraries were designed for data that fits in memory and for computations that run on a single core.

Dask is a Python library for parallel and distributed computing, using blocked algorithms and task scheduling. By leveraging the existing Python data ecosystem, Dask enables to compute on arrays and dataframes that are larger than memory, while exploiting parallelism or distributed computing power, but in a familiar interface (mirroring Numpy arrays and Pandas dataframes).

This 1-day workshop will first give an introduction to the Python data tools, with an emphasis on Pandas, and then show with hands-on examples how those analyses can be scaled with Dask.

## Setting-up

- Connect with the HPC (`ssh vsc40xxx@login.hpc.ugent.be`)
- Start a job for this workshop (`qsub /apps/gent/tutorials/pandas_dask/job.sh -W x=FLAGS:ADVRES:pandastest.201`). This will load the needed modules and start a jupyter notebook at port 8888
- Connect to the jupyter notebook (http://hod.readthedocs.io/en/latest/Connecting_to_web_UIs.html)
