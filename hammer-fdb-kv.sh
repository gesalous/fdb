#!/bin/bash

# Define variables
EXPVER="xxxx"
CLASS="rd"
NLEVELS=201
NSTEPS=1
NPARAMS=5
NENSEMBLES=1
INPUT_FILE="/grib/o3_new.grib"

# Command to run fdb-hammer
./fdb-kv/build/bin/fdb-hammer \
    --statistics \
    --expver="$EXPVER" \
    --class="$CLASS" \
    --nlevels="$NLEVELS" \
    --nsteps="$NSTEPS" \
    --nparams="$NPARAMS" \
    --nensembles="$NENSEMBLES" \
    "$INPUT_FILE"
