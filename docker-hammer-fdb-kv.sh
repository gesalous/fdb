#!/bin/bash

INPUT_FILE="/tmp/1.grib"

rm -rf /fdb-kv/build/tests/fdb/root/parallax
mkdir /fdb-kv/build/tests/fdb/root/parallax
cd /fdb-kv/build/tests/fdb/root/parallax
fallocate default -l 400G
cd /

cd /parallax/scripts
./mkfs.sh /fdb-kv/build/tests/fdb/root/parallax/default 128
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
    "$INPUT_FILE"
