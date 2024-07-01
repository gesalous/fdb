/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/fdb5_config.h"

#include "eckit/config/Resource.h"
#include "eckit/log/Log.h"
#include "eckit/log/Bytes.h"
#include "eckit/io/EmptyHandle.h"

#include "fdb5/database/EntryVisitMechanism.h"
#include "fdb5/io/FDBFileHandle.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/parallax/ParallaxCatalogueWriter.h"
#include "fdb5/parallax/ParallaxFieldLocation.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/io/LustreSettings.h"

using namespace eckit;

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

ParallaxCatalogueWriter::ParallaxCatalogueWriter(const Key &key, const fdb5::Config& config) :
    ParallaxCatalogue(key, config)
    //, umask_(config.umask()) 
    {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    
    const char* volume_name = getenv(PARALLAX_VOLUME_ENV_VAR);;
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

    schema_handle = par_open(&db_options, &error_message);
    if (error_message)
        LSM_DEBUG("Parallax says: %s", error_message);

    if (schema_handle == NULL && error_message)
        LSM_FATAL("Error uppon opening the DB, error %s", error_message);
    const eckit::PathName& path = config.schemaPath();
    //std::cout << "schema path: " << path << std::endl;


    std::stringstream schema_buffer;
    std::ifstream file(path.asString());
    if (file.is_open()) {
        schema_buffer << file.rdbuf();
        file.close();
    } else {
        std::cout << "Error opening file: " << path << std::endl;
        _exit(EXIT_FAILURE);
    }

    std::string schema_str = schema_buffer.str();
    uint32_t max_value_size = par_get_max_kv_pair_size() - PARALLAX_MAX_KEY_SIZE;
    schema_size = schema_str.size();
    size_t num_buffers = (schema_size + max_value_size - 1) / max_value_size;

    //std::cout << "number of buffers: " << num_buffers << std::endl;

    for (size_t i = 0; i < num_buffers; i++){
        const char* error_msg = NULL;
        par_key_value schema_kv;
        std::string key_str = "schema" + std::to_string(i);

        //std::cout << "schema key: " << key_str << std::endl;

        schema_kv.k.size = strlen(key_str.c_str());
        schema_kv.k.data = key_str.c_str();

        size_t start_position = i * max_value_size;
        size_t segment_size = std::min(static_cast<size_t>(max_value_size), schema_size - start_position);

        //std::cout << "segment size: " << segment_size << std::endl;

        schema_kv.v.val_size = segment_size;
        schema_kv.v.val_buffer = new char[segment_size];
        std::memcpy(schema_kv.v.val_buffer, schema_str.data() + start_position, segment_size);

        par_put(schema_handle, &schema_kv, &error_msg);
        if (error_msg) {
            std::cout << "Sorry Parallax put failed reason: " << error_msg << std ::endl;
            _exit(EXIT_FAILURE);
        }
        delete[] schema_kv.v.val_buffer;
    }

    eckit::Log::debug<LibFdb5>() << "Copy schema from "
                                      << config_.schemaPath()
                                      << " to "
                                      << volume_name
                                      << " at key 'schema'."
                                      << std::endl;

    //writeInitRecord(key);
    ParallaxCatalogue::loadSchema();
    //ParallaxCatalogue::checkUID();
}

ParallaxCatalogueWriter::ParallaxCatalogueWriter(const eckit::URI &uri, const fdb5::Config& config) :
    ParallaxCatalogue(uri.path(), ControlIdentifiers{}, config)
    //, umask_(config.umask()) 
    {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    writeInitRecord(ParallaxCatalogue::key());
    ParallaxCatalogue::loadSchema();
    ParallaxCatalogue::checkUID();
}

ParallaxCatalogueWriter::~ParallaxCatalogueWriter() {
    clean();
    close();
}

bool ParallaxCatalogueWriter::selectIndex(const Key& key) {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    currentIndexKey_ = key;

    //std::cout <<"selectIndex key: " << key << std::endl;
    if (indexes_.find(key) == indexes_.end()) {        

        //indexes_[key] = Index(new TocIndex(key, indexPath, 0, TocIndex::WRITE));
    }
    current_ = indexes_[key];

    return true;
}

void ParallaxCatalogueWriter::deselectIndex() {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    current_ = Index();
    currentFull_ = Index();
    currentIndexKey_ = Key();
}

bool ParallaxCatalogueWriter::open() {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return true;
}

void ParallaxCatalogueWriter::clean() {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    //LOG_DEBUG_LIB(LibFdb5) << "Closing path " << directory_ << std::endl;

    flush(); // closes the TOC entries & indexes but not data files

    //compactSubTocIndexes();

    deselectIndex();
}

void ParallaxCatalogueWriter::close() {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    closeIndexes();
}

// void ParallaxCatalogueWriter::index(const Key &key, const eckit::URI &uri, eckit::Offset offset, eckit::Length length) {
//     dirty_ = true;
//     std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
//     if (current_.null()) {
//         ASSERT(!currentIndexKey_.empty());
//         selectIndex(currentIndexKey_);
//     }

//     Field field(ParallaxFieldLocation(uri, offset, length, Key()), currentIndex().timestamp());

//     current_.put(key, field);

//     if (useSubToc())
//         currentFull_.put(key, field);
// }

// void ParallaxCatalogueWriter::reconsolidateIndexesAndTocs() {
//     std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
//     // TODO: This tool needs to be rewritten to reindex properly using the schema.
//     //       Currently it results in incomplete indexes if data has been written
//     //       in a context that has optional values in the schema.

//     // Visitor class for reindexing

//     class ConsolidateIndexVisitor : public EntryVisitor {
//     public:
//         ConsolidateIndexVisitor(ParallaxCatalogueWriter& writer) :
//             writer_(writer) {}
//         ~ConsolidateIndexVisitor() override {}
//     private:
//         void visitDatum(const Field& field, const Key& key) override {
//             // TODO: Do a sneaky schema.expand() here, prepopulated with the current DB/index/Rule,
//             //       to extract the full key, including optional values.
//             const ParallaxFieldLocation& location(static_cast<const ParallaxFieldLocation&>(field.location()));
//             writer_.index(key, location.uri(), location.offset(), location.length());

//         }
//         void visitDatum(const Field& field, const std::string& keyFingerprint) override {
//             EntryVisitor::visitDatum(field, keyFingerprint);
//         }

//         ParallaxCatalogueWriter& writer_;
//     };

//     // Visit all tocs and indexes

//     std::set<std::string> subtocs;
//     std::vector<bool> indexInSubtoc;
//     std::vector<Index> readIndexes = loadIndexes(false, &subtocs, &indexInSubtoc);
//     size_t maskable_indexes = 0;

//     ConsolidateIndexVisitor visitor(*this);

//     ASSERT(readIndexes.size() == indexInSubtoc.size());

//     for (size_t i = 0; i < readIndexes.size(); i++) {
//         Index& idx(readIndexes[i]);
//         selectIndex(idx.key());
//         idx.entries(visitor);

//         Log::info() << "Visiting index: " << idx.location().uri() << std::endl;

//         // We need to explicitly mask indexes in the master TOC
//         if (!indexInSubtoc[i]) maskable_indexes += 1;
//     }

//     // Flush the new indexes and add relevant entries!
//     clean();
//     close();

//     // Add masking entries for all the indexes and subtocs visited so far

//     Buffer buf(sizeof(TocRecord) * (subtocs.size() + maskable_indexes));
//     size_t combinedSize = 0;

//     for (size_t i = 0; i < readIndexes.size(); i++) {
//         // We need to explicitly mask indexes in the master TOC
//         if (!indexInSubtoc[i]) {
//             Index& idx(readIndexes[i]);
//             TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_CLEAR);
//             combinedSize += roundRecord(*r, buildClearRecord(*r, idx));
//             Log::info() << "Masking index: " << idx.location().uri() << std::endl;
//         }
//     }

//     for (const std::string& subtoc_path : subtocs) {
//         TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_CLEAR);
//         combinedSize += roundRecord(*r, buildSubTocMaskRecord(*r, subtoc_path));
//         Log::info() << "Masking sub-toc: " << subtoc_path << std::endl;
//     }

//     // And write all the TOC records in one go!

//     appendBlock(buf, combinedSize);
// }

const Index& ParallaxCatalogueWriter::currentIndex() {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }
    return current_;
}

// const TocSerialisationVersion& ParallaxCatalogueWriter::serialisationVersion() const {
//     std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
//     return TocHandler::serialisationVersion();
// }

// void ParallaxCatalogueWriter::overlayDB(const Catalogue& otherCat, const std::set<std::string>& variableKeys, bool unmount) {
//     std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
//     const ParallaxCatalogue& otherCatalogue = dynamic_cast<const ParallaxCatalogue&>(otherCat);
//     const Key& otherKey(otherCatalogue.key());

//     if (otherKey.size() != ParallaxCatalogue::dbKey_.size()) {
//         std::stringstream ss;
//         ss << "Keys insufficiently matching for mount: " << ParallaxCatalogue::dbKey_ << " : " << otherKey;
//         throw UserError(ss.str(), Here());
//     }

//     // Build the difference map from the old to the new key

//     for (const auto& kv : ParallaxCatalogue::dbKey_) {

//         auto it = otherKey.find(kv.first);
//         if (it == otherKey.end()) {
//             std::stringstream ss;
//             ss << "Keys insufficiently matching for mount: " << ParallaxCatalogue::dbKey_ << " : " << otherKey;
//             throw UserError(ss.str(), Here());
//         }

//         if (kv.second != it->second) {
//             if (variableKeys.find(kv.first) == variableKeys.end()) {
//                 std::stringstream ss;
//                 ss << "Key " << kv.first << " not allowed to differ between DBs: " << ParallaxCatalogue::dbKey_ << " : " << otherKey;
//                 throw UserError(ss.str(), Here());
//             }
//         }
//     }

//     // And append the mount link / unmount mask
//     if (unmount) {

//         // First sanity check that we are already mounted

//         std::set<std::string> subtocs;
//         loadIndexes(false, &subtocs);

//         eckit::PathName stPath(otherCatalogue.tocPath());
//         if (subtocs.find(stPath) == subtocs.end()) {
//             std::stringstream ss;
//             ss << "Cannot unmount DB: " << otherCatalogue << ". Not currently mounted";
//             throw UserError(ss.str(), Here());
//         }

//         writeSubTocMaskRecord(otherCatalogue);
//     } else {
//         writeSubTocRecord(otherCatalogue);
//     }
// }

// void ParallaxCatalogueWriter::hideContents() {
//     writeClearAllRecord();
// }

// bool ParallaxCatalogueWriter::enabled(const ControlIdentifier& controlIdentifier) const {
//     if (controlIdentifier == ControlIdentifier::List || controlIdentifier == ControlIdentifier::Retrieve) {
//         return false;
//     }
//     return ParallaxCatalogue::enabled(controlIdentifier);
// }

void ParallaxCatalogueWriter::lsm_put(const char* tenantId, const FieldRef& ref) {
    const char* error_msg = NULL;
    //std::cout << "lsm_put ref: " << ref << std::endl;
    
    par_key_value KV;
    const char* key_str = tenantId;
    
    KV.k.size = strlen(key_str);
    KV.k.data = key_str;

    // std::cout << "buffer size: " << schema_buffer.str().size() << std::endl;
    // std::cout << "buffer: " << schema_buffer.str() << std::endl;
    FieldRef dataCopy = ref;
    //std::cout << "lsm_put dataCopy: " << dataCopy << std::endl;
    KV.v.val_size = sizeof(FieldRef);
    KV.v.val_buffer = reinterpret_cast<char*>(&dataCopy);
    FieldRef* ptr = reinterpret_cast<FieldRef*>(KV.v.val_buffer);

    //std::cout << "KV.v.val_buffer (values): " << *ptr << std::endl;
    par_put(this->schema_handle, &KV, &error_msg);

    if (error_msg) {
        std::cout << "Sorry Parallax put failed reason: " << error_msg << std ::endl;
        _exit(EXIT_FAILURE);
    }

    
}


void ParallaxCatalogueWriter::archive(const Key& key, std::unique_ptr<FieldLocation> fieldLocation) {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    dirty_ = true;
    if (current_.null()) {
        ASSERT(!currentIndexKey_.empty());
        selectIndex(currentIndexKey_);
    }

    //std::cout << "archive() key: " <<key.valuesToString() << std::endl;
    std::string tenantId = this->tenantId(key) + "/" + key.valuesToString();
    std::cout << "archive() tenantId: " << tenantId << std::endl;

    eckit::PathName tocPath ( directory_ );
    UriStoreWrapper uriStore(tocPath);
    Field field(std::move(fieldLocation), currentIndex().timestamp());
    FieldRef ref(uriStore.files_, field);
    std::cout << "archive ref: " << ref << std::endl;
    //current_.put(key, field);

    lsm_put(tenantId.c_str(), ref);

}

void ParallaxCatalogueWriter::flush() {
    if (!dirty_) {
        return;
    }

    flushIndexes();

    dirty_ = false;
    current_ = Index();
    currentFull_ = Index();
}

std::string ParallaxCatalogueWriter::tenantId(const Key &key) const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    
    eckit::PathName tocPath ( directory_ );  
    tocPath /= currentIndexKey_.valuesToString();
    tocPath = eckit::PathName::unique(tocPath);
    
    std::string tocPathStr = tocPath.asString();
    std::string rootPath = "/root/";
    std::string result = "";

    size_t pos = tocPathStr.find(rootPath);
    if (pos != std::string::npos) {
        result = tocPathStr.substr(pos + rootPath.length());
    } else {
        std::cerr << "\"/root/\" not found in the path." << std::endl;
    }

    //std::cout << "result: " << result << std::endl;

    return result;
}

// n.b. We do _not_ flush the fullIndexes_ set of indexes.
// The indexes pointed to in the indexes_ list get written out each time there is
// a flush (i.e. every step). The indexes stored in fullIndexes then contain _all_
// the data that is indexes thorughout the lifetime of the DBWriter, which can be
// compacted later for read performance.
void ParallaxCatalogueWriter::flushIndexes() {
    for (IndexStore::iterator j = indexes_.begin(); j != indexes_.end(); ++j ) {
        Index& idx = j->second;

        if (idx.dirty()) {
            idx.flush();
            writeIndexRecord(idx);
            idx.reopen(); // Create a new btree
        }
    }
}


void ParallaxCatalogueWriter::closeIndexes() {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    for (IndexStore::iterator j = indexes_.begin(); j != indexes_.end(); ++j ) {
        Index& idx = j->second;
        idx.close();
    }

    for (IndexStore::iterator j = fullIndexes_.begin(); j != fullIndexes_.end(); ++j ) {
        Index& idx = j->second;
        idx.close();
    }

    indexes_.clear(); // all indexes instances destroyed
    fullIndexes_.clear(); // all indexes instances destroyed
}

void ParallaxCatalogueWriter::compactSubTocIndexes() {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    // In this routine, we write out indexes that correspond to all of the data in the
    // subtoc, written by this process. Then we append a masking entry.

    Buffer buf(sizeof(TocRecord) * (fullIndexes_.size() + 1));
    size_t combinedSize = 0;

    // n.b. we only need to compact the subtocs if we are actually writing something...

    if (useSubToc() && anythingWrittenToSubToc()) {

        LOG_DEBUG_LIB(LibFdb5) << "compacting sub tocs" << std::endl;

        for (IndexStore::iterator j = fullIndexes_.begin(); j != fullIndexes_.end(); ++j) {
            Index& idx = j->second;

            if (idx.dirty()) {

                idx.flush();
                TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_INDEX);
                combinedSize += roundRecord(*r, buildIndexRecord(*r, idx));
            }
        }

        // And add the masking record for the subtoc

        TocRecord* r = new (&buf[combinedSize]) TocRecord(serialisationVersion().used(), TocRecord::TOC_CLEAR);
        combinedSize += roundRecord(*r, buildSubTocMaskRecord(*r));

        // Write all of these  records to the toc in one go.

        appendBlock(buf, combinedSize);
    }
}


void ParallaxCatalogueWriter::print(std::ostream &out) const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    out << "ParallaxCatalogueWriter(" << directory() << ")";
}

static CatalogueBuilder<ParallaxCatalogueWriter> builder("parallax.writer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
