#!/bin/bash

INPUT_FILE="/grib/o3_new.grib"

cd /fdb-kv/build/tests/fdb/root
fallocate par.dat -l 20G
cd /

cd parallax/scripts
./mkfs.sh /fdb-kv/build/tests/fdb/root/par.dat 128
cd /

# Command to run fdb-hammer
PARH5_VOLUME=/fdb-kv/build/tests/fdb/root/par.dat ./fdb-kv/build/bin/fdb-hammer \
    --statistics \
    --expver=xxxx \
    --class=rd \
    --nlevels=1 \
    --nsteps=1 \
    --nparams=1 \
    --nensembles=1 \
    "$INPUT_FILE"
