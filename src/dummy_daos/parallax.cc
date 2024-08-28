#include <cstring>
#include <string>
#include <iomanip>
#include <unistd.h>
#include <limits.h>
#include <set>
#include <unordered_map>

#include "eckit/runtime/Main.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/io/Length.h"
#include "eckit/io/FileHandle.h"
#include "eckit/types/UUID.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/utils/MD5.h"

#include "daos.h"
#include "dummy_daos.h"
#include "parallax.h"
#include "structures.h"
#define print 0
#define PARALLAX_L0_SIZE (16 * 1024 * 1024UL);
#define PARALLAX_GROWTH_FACTOR 8
#define PARALLAX_VOLUME_ENV_VAR "PARH5_VOLUME"
#define PARALLAX_MAX_KEY_SIZE 256

using eckit::PathName;

namespace {
    void deldir(eckit::PathName& p) {
        if (!p.exists()) {
            return;
        }

        std::vector<eckit::PathName> files;
        std::vector<eckit::PathName> dirs;
        p.children(files, dirs);

        for (auto& f : files) {
            f.unlink();
        }
        for (auto& d : dirs) {
            deldir(d);
        }

        p.rmdir();
    };
}

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

extern "C" {

//----------------------------------------------------------------------------------------------------------------------

typedef struct daos_handle_internal_t {
    PathName path;
} daos_handle_internal_t;

// std::set<std::string> created_dbs;
std::unordered_map<std::string, par_handle> parallax_db_map;

//----------------------------------------------------------------------------------------------------------------------

par_handle par_get_db(const std::string& db_name) {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;

    // Check if the database is already opened
    auto it = parallax_db_map.find(db_name);
    if (it != parallax_db_map.end()) {
            return it->second;  // Return the existing handle
    }

    // Database is not opened yet, proceed to open it
    const char* volume_name = getenv(PARALLAX_VOLUME_ENV_VAR);

    par_db_options db_options                      = {.volume_name = (char*)volume_name,
                                                      .db_name     = db_name.c_str(),
                                                      .create_flag = PAR_CREATE_DB,
                                                      .options     = par_get_default_options()};
    db_options.options[LEVEL0_SIZE].value          = PARALLAX_L0_SIZE;
    db_options.options[GROWTH_FACTOR].value        = PARALLAX_GROWTH_FACTOR;
    db_options.options[PRIMARY_MODE].value         = 1;
    db_options.options[ENABLE_BLOOM_FILTERS].value = 1;

    const char* error_message = NULL;
    par_handle handle         = par_open(&db_options, &error_message);

    if (error_message) {
            LSM_DEBUG("Parallax says: %s", error_message);
    }

    if (handle == NULL && error_message) {
            LSM_FATAL("Error upon opening the DB, error %s", error_message);
    }

    // Insert the new handle into the hash table
    parallax_db_map[db_name] = handle;

    return handle;
}


// static bool lsm_open(par_handle* handle, std::string db_name){
//     std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
//     const char* volume_name = getenv(PARALLAX_VOLUME_ENV_VAR);

//     par_db_options db_options               = {.volume_name = (char*)volume_name,
//                                                 .db_name     = db_name.c_str(),
//                                                 .create_flag = PAR_CREATE_DB,
//                                                 .options     = par_get_default_options()};
//     db_options.options[LEVEL0_SIZE].value   = PARALLAX_L0_SIZE;
//     db_options.options[GROWTH_FACTOR].value = PARALLAX_GROWTH_FACTOR;
//     db_options.options[PRIMARY_MODE].value  = 1;
//     db_options.options[ENABLE_BLOOM_FILTERS].value  = 1;

//     const char* error_message = NULL;

//     *handle = par_open(&db_options, &error_message);
//     if (error_message)
//         LSM_DEBUG("Parallax says: %s", error_message);

//     if (*handle == NULL && error_message)
//         LSM_FATAL("Error uppon opening the DB, error %s", error_message);

//     created_dbs.insert(db_name);

//     return true;
// }

int daos_init() {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    const char* argv[2] = {"dummy-parallax-api", 0};
    eckit::Main::initialise(1, const_cast<char**>(argv));
    PathName root = dummy_daos_root();
    return 0;
}

int daos_fini() {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;
}


bool lsm_put(par_handle handle, const char* key, const char* value, daos_size_t size) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    par_key_value KV;
    KV.k.size       = strlen(key) + 1;
    KV.k.data       = key;


    KV.v.val_size = size;
    KV.v.val_buffer = (char *)value;
    // memset(KV.v.val_buffer, '\0', size);
    // std::memcpy(KV.v.val_buffer, value, size);
    const char* error_msg_put = NULL;
    par_put(handle, &KV, &error_msg_put);
    if (error_msg_put) {
        LSM_FATAL("Parallax put failed reason: %s", error_msg_put);
        _exit(EXIT_FAILURE);
    }

    return true;
}

par_value lsm_get(par_handle handle, const char* key) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    struct par_key parallax_key;
    parallax_key.size       = strlen(key) + 1;
    parallax_key.data       = key;

    struct par_value value = {0};

    const char* error_msg_get = NULL;
    par_get(handle, &parallax_key, &value, &error_msg_get);
    if (error_msg_get) {
        LSM_DEBUG("Parallax get failed reason: %s", error_msg_get);
        value.val_buffer = NULL;
        value.val_size = 0;
    } 
    
    return value;
}

std::string get_path_after_default(const std::string &fullPath) {
    // Find the position of "default/"
    size_t pos = fullPath.find("default/");

    // +8 to skip "default/ because default/ has 8 characters"
    return fullPath.substr(pos + 8); 
}

std::string get_path_after_last_slash(const std::string &fullPath) {
    // Find the position of the last "/"
    size_t pos = fullPath.find_last_of("/");
    if (pos != std::string::npos){
        return fullPath.substr(pos + 1);
    }
    return "no slash found";
}

// set path for parallax file
int daos_pool_connect(const char *pool, const char *sys, unsigned int flags,
                      daos_handle_t *poh, daos_pool_info_t *info, daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    poh->impl = nullptr;

    if (sys != NULL) NOTIMP;
    if (flags != DAOS_PC_RW) NOTIMP;
    if (info != NULL) NOTIMP;
    if (ev != NULL) NOTIMP;

    eckit::PathName path = dummy_daos_root() / pool;
    std::cout << "Path is " << path.asString() << std::endl;
    if (!path.exists()) return -1;

    std::unique_ptr<daos_handle_internal_t> impl(new daos_handle_internal_t);
    impl->path = path;
    poh->impl = impl.release();

    // Initialize parallax databases
    par_get_db("metadata");
    par_get_db("tenants");
    return 0;

}

int daos_pool_disconnect(daos_handle_t poh, daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    ASSERT(poh.impl);
    delete poh.impl;

    if (ev != NULL) NOTIMP;

    for (auto it = parallax_db_map.begin(); it != parallax_db_map.end(); ++it) {
        par_sync(it->second);
        const char* error = par_close(it->second);
        if (error) {
            LSM_FATAL("Failed to close DB");
        }
    }
    parallax_db_map.clear();
    return 0;

}

int daos_pool_list_cont(daos_handle_t poh, daos_size_t *ncont,
                        struct daos_pool_cont_info *cbuf, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_cont_create_internal(daos_handle_t poh, uuid_t *uuid) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;

    ASSERT(poh.impl);

    std::string hostname = eckit::Main::hostname();

    static unsigned long long n = (((unsigned long long)::getpid()) << 32);

    static std::string format = "%Y%m%d.%H%M%S";
    std::ostringstream os;
    os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;

    std::string name = os.str();

    while (::access(name.c_str(), F_OK) == 0) {
        std::ostringstream os;
        os << eckit::TimeStamp(format) << '.' << hostname << '.' << n++;
        name = os.str();
    }

    uuid_t new_uuid = {0};
    eckit::MD5 md5(name);
    uint64_t hi = std::stoull(md5.digest().substr(0, 8), nullptr, 16);
    uint64_t lo = std::stoull(md5.digest().substr(8, 16), nullptr, 16);
    ::memcpy(&new_uuid[0], &hi, sizeof(hi));
    ::memcpy(&new_uuid[8], &lo, sizeof(lo));

    char cont_uuid_cstr[37] = "";
    uuid_unparse(new_uuid, cont_uuid_cstr);
    eckit::PathName cont_path = poh.impl->path / cont_uuid_cstr;


    if (uuid != NULL) uuid_copy(*uuid, new_uuid);

    return 0;

}

int daos_cont_create(daos_handle_t poh, uuid_t *uuid, daos_prop_t *cont_prop, daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_cont_create_with_label(daos_handle_t poh, const char *label,
                                daos_prop_t *cont_prop, uuid_t *uuid,
                                daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;

    ASSERT(poh.impl);
    if (cont_prop != NULL) NOTIMP;
    if (ev != NULL) NOTIMP;

    ASSERT(std::string{label}.rfind("__dummy_daos_uuid_", 0) != 0);
    ASSERT(strlen(label) <= DAOS_PROP_LABEL_MAX_LEN);

    eckit::PathName label_symlink_path = poh.impl->path / label;

    // open parallax
    par_handle handle = par_get_db("metadata");

    // check if label exists
    std::string buffer;
    par_value value = lsm_get(handle, label);
    if(value.val_size == 0)
        return 0;
    

    // Create uuid for the "container"
    uuid_t new_uuid = {0};
    ASSERT(daos_cont_create_internal(poh, &new_uuid) == 0);

    char cont_uuid_cstr[37] = "";
    uuid_unparse(new_uuid, cont_uuid_cstr);

    if (uuid != NULL) uuid_copy(*uuid, new_uuid);

    // put parallax entry in metadata region for the match of containerName and uuid
    lsm_put(handle, label, cont_uuid_cstr, strlen(cont_uuid_cstr));

    if (uuid != NULL) uuid_copy(*uuid, new_uuid);

    return 0;

}

int daos_cont_destroy(daos_handle_t poh, const char *cont, int force, daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;
}

int daos_cont_open(daos_handle_t poh, const char *cont, unsigned int flags, daos_handle_t *coh,
                    daos_cont_info_t *info, daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    ASSERT(poh.impl);
    if (flags != DAOS_COO_RW) NOTIMP;
    if (info != NULL) NOTIMP;
    if (ev != NULL) NOTIMP;
    
    ASSERT(std::string{cont}.rfind("__dummy_daos_uuid_", 0) != 0);

    eckit::PathName path = poh.impl->path;
    uuid_t uuid = {0};

    if (uuid_parse(cont, uuid) == 0) {
        path /= std::string("__dummy_daos_uuid_") + cont;
    } else {
        path /= cont;
    }
    // open parallax
    par_handle handle = par_get_db("metadata");

    // check if key exists
    struct par_key search_key;
    search_key.size       = strlen(cont) + 1;
    search_key.data       = cont;

    if (par_exists(handle, &search_key) != 0) return -DER_NONEXIST;

    par_value value = lsm_get(handle, cont);
    std::string string_value = std::string(value.val_buffer, 0, value.val_size);

    eckit::PathName realPath{poh.impl->path};
    realPath /= string_value;

    std::unique_ptr<daos_handle_internal_t> impl(new daos_handle_internal_t);
    impl->path = realPath;
    coh->impl = impl.release();
    return 0;

}

int daos_cont_close(daos_handle_t coh, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    ASSERT(coh.impl);
    delete coh.impl;
    if (ev != NULL) NOTIMP;
    return 0;

}

int daos_cont_alloc_oids(daos_handle_t coh, daos_size_t num_oids, uint64_t *oid,
                         daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_obj_generate_oid(daos_handle_t coh, daos_obj_id_t *oid,
                          enum daos_otype_t type, daos_oclass_id_t cid,
                          daos_oclass_hints_t hints, uint32_t args) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;

    ASSERT(coh.impl);
    if (type != DAOS_OT_KV_HASHED && type != DAOS_OT_ARRAY && type != DAOS_OT_ARRAY_BYTE) NOTIMP;
    if (cid != OC_S1) NOTIMP;
    if (hints != 0) NOTIMP;
    if (args != 0) NOTIMP;

    oid->hi &= (uint64_t) 0x00000000FFFFFFFF;
    oid->hi |= ((((uint64_t) type) & OID_FMT_TYPE_MAX) << OID_FMT_TYPE_SHIFT);
    oid->hi |= ((((uint64_t) cid) & OID_FMT_CLASS_MAX) << OID_FMT_CLASS_SHIFT);

    return 0;

}

int daos_kv_open(daos_handle_t coh, daos_obj_id_t oid, unsigned int mode,
                 daos_handle_t *oh, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    ASSERT(coh.impl);
    if (mode != DAOS_OO_RW) NOTIMP;
    if (ev != NULL) NOTIMP;

    std::stringstream os;
    os << std::setw(16) << std::setfill('0') << std::hex << oid.hi;
    os << ".";
    os << std::setw(16) << std::setfill('0') << std::hex << oid.lo;

    std::unique_ptr<daos_handle_internal_t> impl(new daos_handle_internal_t);
    impl->path = coh.impl->path / os.str();

    oh->impl = impl.release();
    return 0;

}

int daos_kv_destroy(daos_handle_t oh, daos_handle_t th, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_obj_close(daos_handle_t oh, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    ASSERT(oh.impl);
    delete oh.impl;

    if (ev != NULL) NOTIMP;

    return 0;

}

int daos_kv_put(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key,
                daos_size_t size, const void *buf, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    
    par_handle handle = par_get_db("tenants");
    
    std::string lsm_key = get_path_after_default(oh.impl->path) + "/" + key;
    lsm_put(handle, lsm_key.c_str(), (const char*)buf, size);
    
    return 0;

}

int daos_kv_get(daos_handle_t oh, daos_handle_t th, uint64_t flags, const char *key,
                daos_size_t *size, void *buf, daos_event_t *ev) {


    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    ASSERT(oh.impl);
    if (th.impl != DAOS_TX_NONE.impl) NOTIMP;
    if (flags != 0) NOTIMP;
    if (ev != NULL) NOTIMP;
     
    std::string region_name = "tenants";
    std::string lsm_key = get_path_after_default(oh.impl->path) + "/" + key;

    // check if "container" exits
    par_handle handle =  par_get_db(region_name); 

    par_value value = lsm_get(handle, lsm_key.c_str());
    
    if (value.val_buffer == NULL && value.val_size == 0 && buf != NULL) return -DER_NONEXIST;

    daos_size_t dest_size = *size;
    *size = 0;
    if (value.val_buffer == NULL && value.val_size == 0) return 0;    

    *size = value.val_size;

    if (buf == NULL) return 0;

    if (*size > dest_size) return -1;

    ::memcpy(buf, value.val_buffer, *size);

    return 0;

}

int daos_kv_remove(daos_handle_t oh, daos_handle_t th, uint64_t flags,
                   const char *key, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_kv_list(daos_handle_t oh, daos_handle_t th, uint32_t *nr,
                 daos_key_desc_t *kds, d_sg_list_t *sgl, daos_anchor_t *anchor,
                 daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    ASSERT(oh.impl);
    static std::vector<std::string> ongoing_req;
    static std::string req_hash;
    static unsigned long long n = (((unsigned long long)::getpid()) << 32);

    if (th.impl != DAOS_TX_NONE.impl) NOTIMP;
    if (ev != NULL) NOTIMP;

    if (nr == NULL) return -1;
    if (kds == NULL) return -1;
    if (sgl == NULL) return -1;
    if (sgl->sg_nr != 1) NOTIMP;
    if (sgl->sg_iovs == NULL) return -1;
    if (anchor == NULL) return -1;


    const char* error_message = NULL;
    struct par_key it_key = {0};
    std::string start_key = get_path_after_default(oh.impl->path);
    it_key.size = get_path_after_default(oh.impl->path).length();
    it_key.data = start_key.c_str();
    par_handle handle = par_get_db("tenants");
    par_scanner scanner = par_init_scanner(handle, &it_key, PAR_GREATER_OR_EQUAL, &error_message);
    
    size_t sgl_pos = 0;
    *nr = 0;
    std::string key_prefix = get_path_after_default(oh.impl->path);
    while (par_is_valid(scanner)) {
        it_key = par_get_key(scanner);
        if (strncmp(it_key.data, key_prefix.c_str(), key_prefix.length()) == 0){
            std::string key_postfix = get_path_after_last_slash(std::string(it_key.data, 0, it_key.size));
            size_t next_size = key_postfix.length();

            ::memcpy((char*) sgl->sg_iovs[0].iov_buf + sgl_pos, key_postfix.c_str(), next_size);
            kds[*nr].kd_key_len = next_size;
            sgl_pos += next_size;
            *nr += 1;
        }else {
            break;
        }
        par_get_next(scanner);
        
    }

    par_close_scanner(scanner);
    anchor->da_type = DAOS_ANCHOR_TYPE_EOF;
    return 0;

}

int daos_array_generate_oid(daos_handle_t coh, daos_obj_id_t *oid, bool add_attr, daos_oclass_id_t cid,
                            daos_oclass_hints_t hints, uint32_t args) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_array_create(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th,
                      daos_size_t cell_size, daos_size_t chunk_size,
                      daos_handle_t *oh, daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_array_destroy(daos_handle_t oh, daos_handle_t th, daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_array_open(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th,
                    unsigned int mode, daos_size_t *cell_size,
                    daos_size_t *chunk_size, daos_handle_t *oh, daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_array_open_with_attr(daos_handle_t coh, daos_obj_id_t oid, daos_handle_t th,
                              unsigned int mode, daos_size_t cell_size, daos_size_t chunk_size,
                              daos_handle_t *oh, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_array_close(daos_handle_t oh, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

// daos_array_write and read have the same limitations and transactional behavior as 
// daos_kv_put and get

int daos_array_write(daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod,
                     d_sg_list_t *sgl, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_array_get_size(daos_handle_t oh, daos_handle_t th, daos_size_t *size,
                        daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;
    
}

int daos_array_read(daos_handle_t oh, daos_handle_t th, daos_array_iod_t *iod,
                    d_sg_list_t *sgl, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_cont_create_snap_opt(daos_handle_t coh, daos_epoch_t *epoch, char *name,
                              enum daos_snapshot_opts opts, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_cont_destroy_snap(daos_handle_t coh, daos_epoch_range_t epr,
                           daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_oit_open(daos_handle_t coh, daos_epoch_t epoch,
                  daos_handle_t *oh, daos_event_t *ev) {
    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_oit_close(daos_handle_t oh, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

int daos_oit_list(daos_handle_t oh, daos_obj_id_t *oids, uint32_t *oids_nr,
                  daos_anchor_t *anchor, daos_event_t *ev) {

    //std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return 0;

}

//----------------------------------------------------------------------------------------------------------------------

}  // extern "C"
