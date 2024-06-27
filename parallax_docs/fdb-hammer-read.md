# Fixing Invalid Date Issue in GRIB Files and Using `fdb-hammer`

This document provides step-by-step instructions to troubleshoot and fix invalid date issues in GRIB files and use the `fdb-hammer` tool. `ecCodes` provides tools such as `grib_ls` and `grib_dump` to inspect and manipulate GRIB files. 

## Prerequisites

Ensure you have `ecCodes` installed.

## 1.

First locate where `ecCodes` is installed and run the `grib_ls` command to inspect the grib file. For example:

```sh
 /home/user/ecmwf/bin/grib_ls /home/user/ecmwf/grib/o3_new.grib
```

## 2.

After you find the invalid date user `grib_set` to correct it. For example:

```sh
/home/user/ecmwf/bin/grib_set -s date=20240627 /home/user/ecmwf/grib/o3_new.grib /home/user/ecmwf/grib/correct_date.grib
```

## 3.

Now you can run fdb-hammer to write

```sh
fdb-hammer --statistics --expver=xxxx --class=rd --nlevels=1  --nsteps=1  --nparams=1 --nensembles=1 /home/toutou/ecmwf/grib/correct.grib
```

and read the data correctly:

```sh
fdb-hammer --statistics --expver=xxxx --class=rd --nlevels=1  --nsteps=1  --nparams=1 --nensembles=1 --read /home/toutou/ecmwf/grib/correct.grib
```