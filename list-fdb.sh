#!/bin/bash


./fdb-kv/build/bin/fdb-hammer-multi-threaded --statistics --expver=xxxx --class=rd --nlevels="$NSTEPS" --nsteps="$NSTEPS" --nparams="$NPARAMS" --nensembles="$NENSEMBLES" --list /grib/o3_new.grib

