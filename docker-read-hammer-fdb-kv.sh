#!/bin/bash

INPUT_FILE="/tmp/1.grib"

cd /fdb-kv/build/bin
# Command to run fdb-hammer
PARH5_VOLUME=/fdb-kv/build/tests/fdb/root/parallax/default ./fdb-hammer \
    --statistics \
    --expver=xxxx \
    --class=rd \
    --nlevels="$NLEVELS" \
    --nsteps="$NSTEPS" \
    --nparams="$NPARAMS" \
    --nensembles="$NENSEMBLES" \
    --read \
    "$INPUT_FILE"
