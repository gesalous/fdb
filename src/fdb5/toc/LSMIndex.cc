/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include <assert.h>
#include <parallax.h>
#include <signal.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <shared_mutex>
#include <unordered_map>
#include "ParallaxSerDes.h"
#include "eckit/config/Resource.h"
#include "eckit/io/Offset.h"
#include "eckit/log/BigNum.h"
#include "eckit/persist/DumpLoad.h"
#include "fdb5/toc/BTreeIndex.h"
#include "fdb5/toc/FieldRef.h"
#include "fdb5/toc/TocIndex.h"
#include "structures.h"

#define PARALLAX_VOLUME_ENV_VAR "PARH5_VOLUME"
#define PARALLAX_VOLUME_FORMAT "PARH5_VOLUME_FORMAT"
#define PARALLAX_VOLUME "par.dat"
#define PARALLAX_GLOBAL_DB "GES"
#define PARALLAX_MAX_KEY_SIZE 256
#define PARALLAX_L0_SIZE (16 * 1024 * 1024UL);
#define PARALLAX_GROWTH_FACTOR 8
/* The value must be between 256 and 65535 (inclusive) */
#define PARALLAX_VOL_CONNECTOR_VALUE ((H5VL_class_value_t)12202)
#define PARALLAX_VOL_CONNECTOR_NAME "parallax_vol_connector"
#define PARALLAX_VOL_CONNECTOR_NAME_SIZE 128
#define PARALLAX_NUM_KEYS 4
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
    std::unordered_map<std::string, void*> valueMap;
    mutable std::shared_mutex mapMutex;


public:
    static ParallaxStore& getInstance(void) {
        static ParallaxStore instance;
        return instance;
    }

    const char* getVolumeName(void) {
        return par_volume_name;
    }

    void* getParallaxVolume(const std::string& key) {
        std::shared_lock<std::shared_mutex> lock(mapMutex);
        auto it = valueMap.find(key);
        return (it != valueMap.end()) ? it->second : nullptr;
    }

    void setParallaxVolume(const std::string& key, void* value) {
        std::unique_lock<std::shared_mutex> lock(mapMutex);
        valueMap[key] = value;
    }


    // Disallow copying and assignment
    ParallaxStore(const ParallaxStore&)            = delete;
    ParallaxStore& operator=(const ParallaxStore&) = delete;

    void initializeStore() {
        par_volume_name = getenv(PARALLAX_VOLUME_ENV_VAR);
        if (NULL == par_volume_name)
            par_volume_name = PARALLAX_VOLUME;

        const char* parh5_format_volume = getenv(PARALLAX_VOLUME_FORMAT);
        if (NULL != parh5_format_volume && strcmp(parh5_format_volume,"ON") == 0) {
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

// class LSMIndexVisitor : public fdb5::BTreeIndexVisitor {
// public:
//     virtual ~LSMIndexVisitor() override;

//     virtual void visit(const std::string& key, const fdb5::FieldRef&) override {
//     }
// };

class LSMIndex : public BTreeIndex {
    par_handle parallax_handle;
private:
  size_t index_id = 999;
public:
    LSMIndex(const eckit::PathName& path, bool readOnly, off_t offset) {

        ParallaxStore& parallax_store = ParallaxStore::getInstance();
        parallax_handle               = parallax_store.getParallaxVolume(path.asString());

        // create the dummy index file so fdb-hammer does not nag
        std::ifstream file(path.asString());
        if (file.good() == false)
            std::ofstream outfile(path.asString());
        index_id = std::hash<std::string>{}(path.asString());
        // std::cerr << "Index name is " << path.asString() << " unique index_id: " << index_id << "\n" << std::endl;
        if (parallax_handle)
            return;

        const char* volume_name = ParallaxStore::getInstance().getVolumeName();
        std::string dbName = PARALLAX_GLOBAL_DB;

        par_db_options db_options               = {.volume_name = (char*)volume_name,
                                                   .db_name     = dbName.c_str(),
                                                   .create_flag = PAR_CREATE_DB,
                                                   .options     = par_get_default_options()};
        db_options.options[LEVEL0_SIZE].value   = PARALLAX_L0_SIZE;
        db_options.options[GROWTH_FACTOR].value = PARALLAX_GROWTH_FACTOR;
        db_options.options[PRIMARY_MODE].value  = 1;
        db_options.options[ENABLE_BLOOM_FILTERS].value  = 1;

        const char* error_message = NULL;

        parallax_handle = par_open(&db_options, &error_message);
        if (error_message)
            LSM_DEBUG("Parallax says: %s", error_message);

        if (parallax_handle == NULL && error_message)
            LSM_FATAL("Error uppon opening the DB, error %s", error_message);
        parallax_store.setParallaxVolume(path.asString(), parallax_handle);
        // std::cerr << "OK for Index  " << path.asString() << " unique index_id: " << index_id << "\n"
        //           << std::endl;
    }

    ~LSMIndex() {
        // LSM_DEBUG("Destroying LSM index.");
    }

    bool get(const ::std::string& key, FieldRef& data) const {
        LSM_DEBUG("Unsupported operation");
        const char* error_msg = NULL;
        const char* key_str   = key.c_str();
        struct par_key parallax_key;
        parallax_key.size       = strlen(key_str) + 1;
        parallax_key.data       = key_str;

        struct par_value value;
        // Obtaining a char* pointer to the copy
        value.val_buffer = reinterpret_cast<char*>(&data);
        value.val_buffer_size = sizeof(FieldRef);
        const char *error = NULL;
        par_get(this->parallax_handle, &parallax_key, &value, &error);
        if (error) {
            LSM_DEBUG("Key not found!");
            return false;
        }
        return true;
    }

    bool set(const std::string& key, const FieldRef& data) {
        // LSM_DEBUG("LSM set operation. %s for index_id: %lu", key.c_str(),index_id);
        // fdb5::ParallaxSerDes<32> serializer;
        // eckit::DumpLoad& baseRef      = serializer;
        // FieldRefLocation::UriID uriId = data.uriId();
        // const eckit::Offset& offset   = data.offset();
        // const eckit::Length& length   = data.length();
        // baseRef.beginObject("test");
        // offset.dump(baseRef);
        // length.dump(baseRef);
        // baseRef.dump(uriId);
        // baseRef.endObject();
        const char* error_msg = NULL;
        //old school staff
        //const char* key_str   = key.c_str();

        //Prepend the index id
        // Calculate the total size needed
        size_t key_len = key.length();
        size_t total_size = sizeof(index_id) + key_len;
        // Allocate memory for the new key
        char new_key[PARALLAX_MAX_KEY_SIZE] = {0};
        // Copy the hash_id to the beginning of the new_key_data
        std::memcpy(new_key, &index_id, sizeof(index_id)); 
        // Copy the original key after the hash_id bytes
        std::memcpy(&new_key[sizeof(index_id)], key.c_str(), key_len);
        // LSM_DEBUG("Prepend index id: %lu", index_id);

        par_key_value KV;
        KV.k.size       = total_size;
        KV.k.data       = new_key;
        // KV.v.val_size   = serializer.getSize();
        // KV.v.val_buffer = (char*)serializer.getBuffer();
        KV.v.val_size = sizeof(FieldRef);
        // Creating a copy of FieldRef
        FieldRef dataCopy = data;  // Copy constructor is used here
        // Obtaining a char* pointer to the copy
        KV.v.val_buffer = reinterpret_cast<char*>(&dataCopy);
        
        // LSM_DEBUG("LSM par_put operation...");
        par_put(this->parallax_handle, &KV, &error_msg);
        if (error_msg) {
            std::cout << "Sorry Parallax put failed reason: " << error_msg << std ::endl;
            _exit(EXIT_FAILURE);
        }
        // static size_t keys_written = 0;
        // std::cout << "Keys written " << ++keys_written << std::endl;
        return true;
    }

    void flush() {
        // LSM_DEBUG("LSM flush operation.");
        par_sync(this->parallax_handle);
    }

    void sync() {
        // LSM_DEBUG("LSM sync operation.");
        par_sync(this->parallax_handle);
    }

    void flock() {
        // LSM_DEBUG("LSM flock operation.");
    }

    void funlock() {
        LSM_DEBUG("LSM funlock operation.");
    }
    void visit(BTreeIndexVisitor& visitor) const {
        uint64_t keys_touched = 0;
        // LSM_DEBUG("Searching staff index_id: %lu",index_id); 
        struct par_key start = {.size = sizeof(index_id), .data = (char *)&index_id};
        const char* error    = nullptr;
        par_scanner scanner  = par_init_scanner(parallax_handle, &start, PAR_GREATER_OR_EQUAL, &error);
        if (error)
            LSM_FATAL("Init of scanner failed: reason: %s",error);
    
        while (par_is_valid(scanner)) {
            struct par_key parallax_key     = par_get_key(scanner);
            if (std::memcmp(parallax_key.data, &index_id, sizeof(index_id)) != 0){
                // LSM_DEBUG("Done with index id: %lu parallax brought key size: %d key data prefix: %lu", index_id, parallax_key.size, *(size_t*)parallax_key.data);
                break;
            }
            struct par_value parallax_value = par_get_value(scanner);
            const std::string key           = std::string(&parallax_key.data[sizeof(index_id)], parallax_key.size-sizeof(index_id));
            if (parallax_value.val_size != sizeof(FieldRef))
                LSM_FATAL("Values of FieldRef is wrong");

            // Cast parallax_value.data to a FieldRef pointer
            const FieldRef* fieldRefPtr = reinterpret_cast<const FieldRef*>(parallax_value.val_buffer);

            // Make a copy of the FieldRef object
            FieldRef fieldRefCopy = *fieldRefPtr;
            visitor.visit(key, fieldRefCopy);
            if (++keys_touched == PARALLAX_NUM_KEYS)
                break;
            par_get_next(scanner);
        }
        par_close_scanner(scanner);
    }

    void preload() {
        // LSM_DEBUG("Nothing to preload here we are PARALLAX");
    }

};



static BTreeIndexBuilder<LSMIndex> lsmIndexBuilder("LSMIndex");

}  // namespace fdb5
