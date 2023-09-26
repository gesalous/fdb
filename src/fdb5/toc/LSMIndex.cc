/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <openssl/md5.h>
#include <parallax.h>
#include <signal.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include "ParallaxSerDes.h"
#include "eckit/config/Resource.h"
#include "eckit/io/Offset.h"
#include "eckit/log/BigNum.h"
#include "eckit/persist/DumpLoad.h"
#include "fdb5/toc/BTreeIndex.h"
#include "fdb5/toc/FieldRef.h"
#include "fdb5/toc/TocIndex.h"

#define PARALLAX_VOLUME_ENV_VAR "PARH5_VOLUME"
#define PARALLAX_VOLUME_FORMAT_ENV_VAR "PARH5_VOLUME_FORMAT"
#define PARALLAX_VOLUME "par.dat"
#define PARALLAX_L0_SIZE (16 * 1024 * 1024);
#define PARALLAX_GROWTH_FACTOR 8
/* The value must be between 256 and 65535 (inclusive) */
#define PARALLAX_VOL_CONNECTOR_VALUE ((H5VL_class_value_t)12202)
#define PARALLAX_VOL_CONNECTOR_NAME "parallax_vol_connector"
#define PARALLAX_VOL_CONNECTOR_NAME_SIZE 128

#include <cstdio>
#include <iostream>
#include <string>


namespace fdb5 {
#define LSM_DEBUG(...)                                                       \
    do {                                                                     \
        char buffer[1024];                                                   \
        snprintf(buffer, sizeof(buffer), __VA_ARGS__);                       \
        ::std::cout << __FILE__ << ":" << __func__ << ":" << __LINE__ << " " \
                    << " DEBUG: " << buffer << ::std::endl;                  \
    } while (0);


#define LSM_FATAL(...)                                                \
    do {                                                              \
        char buffer[1024];                                            \
        snprintf(buffer, sizeof(buffer), __VA_ARGS__);                \
        ::std::cout << __FILE__ << ":" << __func__ << ":" << __LINE__ \
                    << " FATAL: " << buffer << ::std::endl;           \
        _exit(EXIT_FAILURE);                                          \
    } while (0);


//----------------------------------------------------------------------------------------------------------------------
class ParallaxStore {
    const char* par_volume_name;

public:
    static const char* getVolumeName(void) {
        static ParallaxStore instance;
        return instance.par_volume_name;
    }

    // Disallow copying and assignment
    ParallaxStore(const ParallaxStore&)            = delete;
    ParallaxStore& operator=(const ParallaxStore&) = delete;

    void initializeStore() {
        par_volume_name = getenv(PARALLAX_VOLUME_ENV_VAR);
        if (NULL == par_volume_name)
            par_volume_name = PARALLAX_VOLUME;

        const char* parh5_format_volume = getenv(PARALLAX_VOLUME_FORMAT_ENV_VAR);
        if (NULL != par_volume_name) {
            const char* error = par_format((char*)par_volume_name, 128);
            if (error) {
                LSM_FATAL("Failed to format volume %s", par_volume_name);
            }
        }
    }

private:
    ParallaxStore() {
        // Private constructor
        initializeStore();
    }

    ~ParallaxStore() {
        // Cleanup if necessary
    }
};


class LSMIndex : public BTreeIndex {
    par_handle parallax_handle;

public:
    LSMIndex(const eckit::PathName& path, bool readOnly, off_t offset) {

        // create the dummy index file so fdb-hammer does not nag
        std::ofstream outfile(path.asString());
        // ... write to the file or do other things with it.
        std::string dbName = md5(path.asString());
        std::cout << "DB name is " << path.asString() << "hash  name: " << dbName << std::endl;

        par_db_options db_options               = {.volume_name = (char*)ParallaxStore::getVolumeName(),
                                                   .db_name     = dbName.c_str(),
                                                   .create_flag = PAR_CREATE_DB,
                                                   .options     = par_get_default_options()};
        db_options.options[LEVEL0_SIZE].value   = PARALLAX_L0_SIZE;
        db_options.options[GROWTH_FACTOR].value = PARALLAX_GROWTH_FACTOR;
        db_options.options[PRIMARY_MODE].value  = 1;

        const char* error_message = NULL;

        parallax_handle = par_open(&db_options, &error_message);
        if (error_message)
            LSM_DEBUG("Parallax says: %s", error_message);

        if (parallax_handle == NULL && error_message)
            LSM_FATAL("Error uppon opening the DB, error %s", error_message);
    }

    ~LSMIndex() {
        LSM_DEBUG("Destroying LSM index.");
    }

    bool get(const ::std::string& key, FieldRef& data) const {
        LSM_FATAL("LSM get operation (unimplemented XXX TODO XXX).");
        return true;
    }

    bool set(const std::string& key, const FieldRef& data) {
        LSM_DEBUG("LSM set operation. %s", key.c_str());
        std::cout << "Data lentgh " << data.length() << std::endl;
        fdb5::ParallaxSerDes<128> serializer;
        eckit::DumpLoad& baseRef      = serializer;
        FieldRefLocation::UriID uriId = data.uriId();
        const eckit::Offset& offset   = data.offset();
        const eckit::Length& length   = data.length();
        baseRef.beginObject("test");
        offset.dump(baseRef);
        length.dump(baseRef);
        baseRef.dump(uriId);
        baseRef.endObject();
        const char* error_msg = NULL;
        const char* key_str   = key.c_str();
        par_key_value KV;
        KV.k.size       = strlen(key_str) + 1;
        KV.k.data       = key_str;
        KV.v.val_size   = serializer.getSize();
        KV.v.val_buffer = (char*)serializer.getBuffer();
        par_put(this->parallax_handle, &KV, &error_msg);
        if (error_msg) {
            std::cout << "Sorry Parallax put failed reason: " << error_msg << std ::endl;
            _exit(EXIT_FAILURE);
        }
        std::cout << "Success!" << std::endl;

        return true;
    }

    void flush() {
        LSM_DEBUG("LSM flush operation.");
    }

    void sync() {
        LSM_DEBUG("LSM sync operation.");
    }

    void flock() {
        LSM_DEBUG("LSM flock operation.");
    }

    void funlock() {
        LSM_DEBUG("LSM funlock operation.");
    }

    void visit(BTreeIndexVisitor& visitor) const {
        LSM_DEBUG("LSM visit operation.");
    }
    void preload() {
        LSM_DEBUG("PRELOAD bitches");
    }

private:
    std::string md5(const std::string& data) {
        unsigned char digest[MD5_DIGEST_LENGTH];

        // Calculate the MD5 hash of the input data
        MD5(reinterpret_cast<const unsigned char*>(data.c_str()), data.size(), digest);

        // Convert the binary hash to a hexadecimal string
        std::ostringstream oss;
        for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
            oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
        }

        return oss.str();
    }
};


class LSMIndexVisitor {
    BTreeIndexVisitor& visitor_;

public:
    LSMIndexVisitor(BTreeIndexVisitor& visitor) :
        visitor_(visitor) {
        LSM_DEBUG("Initializing Index Visitor.");
    }

    void clear() {
    }

    void push_back(const std::pair<std::string, FieldRef>& kv) {
        visitor_.visit(kv.first, kv.second);
    }
};

static BTreeIndexBuilder<LSMIndex> lsmIndexBuilder("LSMIndex");

}  // namespace fdb5
