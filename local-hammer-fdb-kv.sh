#!/bin/bash

INPUT_FILE="/home/toutou/ecmwf/grib/o3_new.grib"

cd /home/toutou/ecmwf/fdb-kv/build/tests/fdb/root
rm -f par.dat
fallocate par.dat -l 20G
cd /

cd /home/toutou/ecmwf/parallax/scripts
./mkfs.sh /home/toutou/ecmwf/fdb-kv/build/tests/fdb/root/par.dat 128
cd /home/toutou/ecmwf/fdb-kv/build/bin
# Command to run fdb-hammer
PARH5_VOLUME=/home/toutou/ecmwf/fdb-kv/build/tests/fdb/root/par.dat ./fdb-hammer \
    --statistics \
    --expver=xxxx \
    --class=rd \
    --nlevels=1 \
    --nsteps=1 \
    --nparams=1 \
    --nensembles=1 \
    "$INPUT_FILE"
