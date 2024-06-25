/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Timer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/parallax/ParallaxCatalogue.h"
//#include "fdb5/toc/TocPurgeVisitor.h"
#include "fdb5/toc/TocStats.h"
//#include "fdb5/toc/TocWipeVisitor.h"
//#include "fdb5/toc/TocMoveVisitor.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ParallaxCatalogue::ParallaxCatalogue(const Key& key, const fdb5::Config& config) :
    ParallaxCatalogue(key, CatalogueRootManager(config).directory(key), config) {
        std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    }

ParallaxCatalogue::ParallaxCatalogue(const Key& key, const TocPath& tocPath, const fdb5::Config& config) :
    Catalogue(key, tocPath.controlIdentifiers_, config),
    TocHandler(tocPath.directory_, config) {
        std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    }

ParallaxCatalogue::ParallaxCatalogue(const eckit::PathName& directory, const ControlIdentifiers& controlIdentifiers, const fdb5::Config& config) :
    Catalogue(Key(), controlIdentifiers, config),
    TocHandler(directory, config) {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    // Read the real DB key into the DB base object
    dbKey_ = databaseKey();
}

bool ParallaxCatalogue::exists() const {
    return TocHandler::exists();
}

const std::string ParallaxCatalogue::DUMP_PARAM_WALKSUBTOC = "walk";

void ParallaxCatalogue::dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    bool walkSubToc = false;
    conf.get(DUMP_PARAM_WALKSUBTOC, walkSubToc);

    TocHandler::dump(out, simple, walkSubToc);
}

eckit::URI ParallaxCatalogue::uri() const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return eckit::URI(TocEngine::typeName(), basePath());
}

const Schema& ParallaxCatalogue::schema() const {
    return schema_;
}

const eckit::PathName& ParallaxCatalogue::basePath() const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return directory_;
}

std::vector<PathName> ParallaxCatalogue::metadataPaths() const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    std::vector<PathName> paths(subTocPaths());

    paths.emplace_back(schemaPath());
    paths.emplace_back(tocPath());

    std::vector<PathName>&& lpaths(lockfilePaths());
    paths.insert(paths.end(), lpaths.begin(), lpaths.end());

    return paths;
}

void ParallaxCatalogue::visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    std::vector<Index> all = indexes(sorted);

    // Allow the visitor to selectively reject this DB.
    if (visitor.visitDatabase(*this, store)) {
        if (visitor.visitIndexes()) {
            for (const Index& idx : all) {
                if (visitor.visitEntries()) {
                    idx.entries(visitor); // contains visitIndex
                } else {
                    visitor.visitIndex(idx);
                }
            }
        }
    }
    visitor.catalogueComplete(*this);
}

void ParallaxCatalogue::loadSchema() {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    Timer timer("ParallaxCatalogue::loadSchema()", Log::debug<LibFdb5>());
    //schema_ = &SchemaRegistry::instance().get(schemaPath());


    std::vector<char> schema_read(schema_size, '\0');

    const char* error_get = NULL;
    struct par_key parallax_key;
    parallax_key.size = strlen("schema");
    parallax_key.data = "schema";

    struct par_value value;
    value.val_buffer = &schema_read[0];
    value.val_buffer_size = schema_size;

    par_get(this->parallax_handle, &parallax_key, &value, &error_get);
    if (error_get) {
        std::cout << "Sorry Parallax get failed reason: " << error_get << std ::endl;
        _exit(EXIT_FAILURE);
    }
    
    // std::cout << "Retrieved buffer (byte by byte): ";
    // for (size_t i = 0; i < value.val_buffer_size; ++i) {
    //     std::cout << value.val_buffer[i];
    // }
    // std::cout << std::endl;

    std::istringstream stream{std::string(schema_read.begin(), schema_read.end())};
    schema_.load(stream);
}

// StatsReportVisitor* ParallaxCatalogue::statsReportVisitor() const {
//     return new TocStatsReportVisitor(*this);
// }

// PurgeVisitor *ParallaxCatalogue::purgeVisitor(const Store& store) const {
//     return new TocPurgeVisitor(*this, store);
// }

// WipeVisitor* ParallaxCatalogue::wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const {
//     return new TocWipeVisitor(*this, store, request, out, doit, porcelain, unsafeWipeAll);
// }

// MoveVisitor* ParallaxCatalogue::moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const {
//     return new TocMoveVisitor(*this, store, request, dest, queue);
// }

void ParallaxCatalogue::maskIndexEntry(const Index &index) const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    TocHandler handler(basePath(), config_);
    handler.writeClearRecord(index);
}

std::vector<Index> ParallaxCatalogue::indexes(bool sorted) const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return loadIndexes(sorted);
}

void ParallaxCatalogue::allMasked(std::set<std::pair<URI, Offset>>& metadata,
                      std::set<URI>& data) const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    enumerateMasked(metadata, data);
}

std::string ParallaxCatalogue::type() const
{
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    return ParallaxCatalogue::catalogueTypeName();
}

// void ParallaxCatalogue::checkUID() const {
//     std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
//     TocHandler::checkUID();
// }

void ParallaxCatalogue::remove(const eckit::PathName& path, std::ostream& logAlways, std::ostream& logVerbose, bool doit) {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    if (path.isDir()) {
        logVerbose << "rmdir: ";
        logAlways << path << std::endl;
        if (doit) path.rmdir(false);
    } else {
        logVerbose << "Unlinking: ";
        logAlways << path << std::endl;
        if (doit) path.unlink(false);
    }
}

void ParallaxCatalogue::control(const ControlAction& action, const ControlIdentifiers& identifiers) const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    TocHandler::control(action, identifiers);
}

// bool ParallaxCatalogue::enabled(const ControlIdentifier& controlIdentifier) const {
//     std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
//     //return Catalogue::enabled(controlIdentifier) && TocHandler::enabled(controlIdentifier);
//     return true;
// }

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
