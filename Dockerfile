# Stage 1: God created the heavens and the earth
FROM ubuntu:22.04 AS builder

ENV eckit_ROOT /root/local
ENV CMAKE_PREFIX_PATH /usr/local
ENV metkit_ROOT /root/local

COPY hammer-fdb-kv.sh /hammer-fdb-kv.sh

RUN apt-get update && apt-get upgrade -y

RUN DEBIAN_FRONTEND=noninteractive TZ=Europe/Athens apt-get install -y wget git cmake g++ gfortran python3 python3-pip libnuma-dev libboost-all-dev
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

RUN git clone https://github.com/ecmwf/eckit.git && \
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
    git checkout extra_hdf5_functionality && \
    mkdir build && \
    cd build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=/root/local -DBUILD_SHARED_LIBS=ON && \
    make -j10 && \
    make install 

# fdb-kv
RUN git clone https://github.com/Toutou98/fdb-kv.git && \
    cd fdb-kv && \
    mkdir build && \
    cd build && \
    ecbuild --prefix=$HOME/local -- -DCMAKE_INSTALL_PREFIX=/root/local -DCMAKE_FIND_LIBRARY_PREFIXES="lib" -DCMAKE_FIND_LIBRARY_SUFFIXES=".so;.a" .. && \
    make -j10

RUN cd / && \
    git clone https://github.com/Toutou98/grib.git

RUN chmod +x /hammer-fdb-kv.sh
# the great purge
RUN apt purge -y wget git cmake g++ gfortran python3 python3-pip libnuma-dev libboost-all-dev && \
    apt autoremove -y

# Stage 2: The second coming
FROM ubuntu:22.04

ENV eckit_ROOT /root/local
ENV CMAKE_PREFIX_PATH /usr/local
ENV metkit_ROOT /root/local

COPY --from=builder /usr /usr
COPY --from=builder /root /root
COPY --from=builder /fdb-kv /fdb-kv
COPY --from=builder /hammer-fdb-kv.sh /hammer-fdb-kv.sh
COPY --from=builder /grib /grib

RUN chmod +x /hammer-fdb-kv.sh

