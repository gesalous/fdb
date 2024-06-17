#!/bin/bash

INPUT_FILE="/grib/o3_new.grib"

cd parallax/scripts
./mkfs.sh /fdb-kv/build/tests/fdb/root/parallax 128
cd /

# Command to run fdb-hammer
PARH5_VOLUME=/fdb-kv/build/tests/fdb/root/parallax ./fdb-kv/build/bin/fdb-hammer-multi-threaded \
    --statistics \
    --expver=xxxx \
    --class=rd \
    --nlevels="$NLEVELS" \
    --nsteps="$NSTEPS" \
    --nparams="$NPARAMS" \
    --nensembles="$NENSEMBLES" \
    "$INPUT_FILE"