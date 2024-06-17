# Stage 1: God created the heavens and the earth
FROM ubuntu:22.04 AS builder

ENV eckit_ROOT /root/local
ENV CMAKE_PREFIX_PATH /usr/local
ENV metkit_ROOT /root/local

RUN apt-get update && apt-get upgrade -y

RUN DEBIAN_FRONTEND=noninteractive TZ=Europe/Athens apt install -y wget git cmake g++ gfortran python3 python3-pip libnuma-dev libboost-all-dev bc
RUN apt install -y vim gdb

RUN pip3 install eccodes

# fdb dependecies
RUN wget https://gitlab.dkrz.de/k202009/libaec/-/archive/v1.1.1/libaec-v1.1.1.tar.gz && \
    tar -xzf libaec-v1.1.1.tar.gz && \
    cd libaec-v1.1.1 && \
    mkdir build && \
    cd build && \
    cmake .. # -CMAKE_INSTALL_PREFIX=~/local
    
RUN cd libaec-v1.1.1/build && \
    make install && \
    cd /

RUN wget https://confluence.ecmwf.int/download/attachments/45757960/eccodes-2.35.0-Source.tar.gz?api=v2 && \
    tar -xzf eccodes-2.35.0-Source.tar.gz?api=v2 && \
    cd eccodes-2.35.0-Source && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local && \
    make -j10 && \
    ctest && \
    make install && \
    cd / 

RUN git clone https://github.com/ecmwf/ecbuild && \
    cd ecbuild && \
    mkdir bootstrap && \
    cd bootstrap && \
    ../bin/ecbuild --prefix=/usr/local .. && \
    ctest && \
    make install && \
    cd /

RUN git clone https://github.com/gesalous/eckit.git && \
    cd eckit && \
    mkdir build && \
    cd build && \
    ecbuild --prefix=$HOME/local -- ../$pwd && \
    make -j10 && \
    make install && \
    cd /

RUN git clone https://github.com/ecmwf/metkit.git && \
    cd metkit && \
    mkdir build && \
    cd build && \
    ecbuild --prefix=$HOME/local -- -DECKIT_PATH=/usr/local ../$pwd && \
    make -j10 && \
    make install && \
    cd /
# Parallax
RUN git clone https://github.com/CARV-ICS-FORTH/parallax.git && \
    cd parallax && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/root/local -DBUILD_SHARED_LIBS=ON && \
    make -j10 && \
    make install 

RUN cd / && \
    git clone https://github.com/Toutou98/grib.git

RUN cp /grib/o3_new.grib /tmp/0.grib && \
    cp /grib/o3_new.grib /tmp/1.grib && \
    cp /grib/o3_new.grib /tmp/2.grib && \
    cp /grib/o3_new.grib /tmp/3.grib && \
    cp /grib/o3_new.grib /tmp/4.grib && \
    cp /grib/o3_new.grib /tmp/5.grib && \
    cp /grib/o3_new.grib /tmp/6.grib && \
    cp /grib/o3_new.grib /tmp/7.grib && \
    cp /grib/o3_new.grib /tmp/8.grib && \
    cp /grib/o3_new.grib /tmp/9.grib && \
    cp /grib/o3_new.grib /tmp/10.grib && \
    cp /grib/o3_new.grib /tmp/11.grib && \
    cp /grib/o3_new.grib /tmp/12.grib && \
    cp /grib/o3_new.grib /tmp/13.grib && \
    cp /grib/o3_new.grib /tmp/14.grib && \
    cp /grib/o3_new.grib /tmp/15.grib

# fdb-kv
COPY . /fdb-kv

RUN cd fdb-kv && \
    git checkout multi_tenant_index && \
    mkdir build && \
    cd build && \
    ecbuild --prefix=$HOME/local -- -DCMAKE_BUILD_TYPE="Debug" -DCMAKE_INSTALL_PREFIX=/root/local -DCMAKE_FIND_LIBRARY_PREFIXES="lib" -DCMAKE_FIND_LIBRARY_SUFFIXES=".so;.a" .. && \
    make -j10

COPY list-fdb-kv.sh /list-fdb-kv.sh
COPY hammer-fdb-kv.sh /hammer-fdb-kv.sh
COPY list-fdb.sh /list-fdb.sh
COPY hammer-fdb.sh /hammer-fdb.sh

RUN chmod +x /hammer-fdb-kv.sh
RUN chmod +x /list-fdb-kv.sh
RUN chmod +x /hammer-fdb.sh
RUN chmod +x /list-fdb.sh
# the great purge


