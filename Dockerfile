# Stage 1: God created the heavens and the earth
FROM ubuntu:22.04 AS builder

ENV eckit_ROOT=/usr/local
ENV CMAKE_PREFIX_PATH=/usr/local
ENV metkit_ROOT=/usr/local

RUN apt-get update && apt-get upgrade -y

RUN DEBIAN_FRONTEND=noninteractive TZ=Europe/Athens apt install -y wget git cmake g++ gfortran python3 python3-pip libnuma-dev libboost-all-dev bc RUN uuid-dev
RUN apt install -y vim gdb

RUN pip3 install eccodes

# fdb dependecies
RUN wget https://gitlab.dkrz.de/k202009/libaec/-/archive/v1.1.1/libaec-v1.1.1.tar.gz && \
    tar -xzf libaec-v1.1.1.tar.gz && \
    cd libaec-v1.1.1 && \
    mkdir build && \
    cd build && \
    cmake .. # -CMAKE_INSTALL_PREFIX=/usr/local
    
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

RUN git clone https://github.com/ecmwf/eckit.git && \
    cd eckit && \
    mkdir build && \
    cd build && \
    ecbuild --prefix=/usr/local -- .. && \
    make -j10 && \
    make install && \
    cd /

RUN git clone https://github.com/ecmwf/metkit.git && \
    cd metkit && \
    mkdir build && \
    cd build && \
    ecbuild --prefix=/usr/local -- -DECKIT_PATH=/usr/local ../$pwd && \
    make -j10 && \
    make install && \
    cd /


COPY correct_date.grib /grib/o3_new.grib

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

RUN git clone -b embed_bloom_filters_in_sst https://github.com/CARV-ICS-FORTH/parallax.git && \
    cd parallax && \
    rm -rf build && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_BUILD_TYPE="Debug" -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_SHARED_LIBS=ON -DKV_MAX_SIZE_64K=ON && \
    make -j10 && \
    make install 

# fdb-kv

RUN mkdir /tmp/parallax
COPY . /fdb-kv
RUN cd fdb-kv && \
    rm -rf build && \
    mkdir build && \
    cd build && \
    ecbuild --prefix=/usr/local -- -DCMAKE_BUILD_TYPE="Debug" -DCMAKE_INSTALL_PREFIX=/usr/local -DUSE_PARALLAX=ON -DCMAKE_FIND_LIBRARY_PREFIXES="lib" -DCMAKE_FIND_LIBRARY_SUFFIXES=".so;.a" .. && \
    make -j10


