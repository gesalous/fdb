#!/bin/bash

INPUT_FILE="/grib/o3_new.grib"

# Command to run fdb-hammer
./fdb-kv/build/bin/fdb-hammer-multi-threaded \
    --statistics \
    --expver=xxxx \
    --class=rd \
    --nlevels="$NLEVELS" \
    --nsteps="$NSTEPS" \
    --nparams="$NPARAMS" \
    --nensembles="$NENSEMBLES" \
    "$INPUT_FILE"
