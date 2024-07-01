/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   ParallaxCatalogue.h
/// @author Michail Toutoudakis
/// @author Giorgos Saloustros
/// @date   June 2024

#ifndef fdb5_ParallaxDB_H
#define fdb5_ParallaxDB_H

#define PARALLAX_VOLUME_ENV_VAR "PARH5_VOLUME"
#define PARALLAX_GLOBAL_DB "GES"
#define PARALLAX_MAX_KEY_SIZE 256
#define PARALLAX_L0_SIZE (16 * 1024 * 1024UL);
#define PARALLAX_GROWTH_FACTOR 8
/* The value must be between 256 and 65535 (inclusive) */
#define PARALLAX_VOL_CONNECTOR_VALUE ((H5VL_class_value_t)12202)
#define PARALLAX_VOL_CONNECTOR_NAME "parallax_vol_connector"
#define PARALLAX_VOL_CONNECTOR_NAME_SIZE 128
#define PARALLAX_NUM_KEYS 4

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/toc/FileSpace.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/parallax/ParallaxEngine.h"
#include <shared_mutex>
#include <algorithm>
#include "parallax.h"
#include "structures.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on Parallax KV store

class ParallaxCatalogue : public Catalogue, public TocHandler {

public: // methods

    ParallaxCatalogue(const Key& key, const fdb5::Config& config);
    ParallaxCatalogue(const eckit::PathName& directory, const ControlIdentifiers& controlIdentifiers, const fdb5::Config& config);

    ~ParallaxCatalogue() override {}

    static const char* catalogueTypeName() { return ParallaxEngine::typeName(); }
    const eckit::PathName& basePath() const override;
    eckit::URI uri() const override;
    const Key& indexKey() const override { return currentIndexKey_; }

    static void remove(const eckit::PathName& path, std::ostream& logAlways, std::ostream& logVerbose, bool doit);

    //bool enabled(const ControlIdentifier& controlIdentifier) const override;

public: // constants
    static const std::string DUMP_PARAM_WALKSUBTOC;

protected: // methods

    ParallaxCatalogue(const Key& key, const TocPath& tocPath, const fdb5::Config& config);

    std::string type() const override;

    void checkUID() const override ;
    bool exists() const override;
    void visitEntries(EntryVisitor& visitor, const Store& store, bool sorted) override;
    void dump(std::ostream& out, bool simple, const eckit::Configuration& conf) const override;
    std::vector<eckit::PathName> metadataPaths() const override;
    const Schema& schema() const override;

    StatsReportVisitor* statsReportVisitor() const override { NOTIMP; };
    PurgeVisitor* purgeVisitor(const Store& store) const override { NOTIMP; }; 
    WipeVisitor* wipeVisitor(const Store& store, const metkit::mars::MarsRequest& request, std::ostream& out, bool doit, bool porcelain, bool unsafeWipeAll) const override { NOTIMP;};
    MoveVisitor* moveVisitor(const Store& store, const metkit::mars::MarsRequest& request, const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const override{ NOTIMP;};
    void maskIndexEntry(const Index& index) const override;

    void loadSchema() override;

    std::vector<Index> indexes(bool sorted=false) const override;

    void allMasked(std::set<std::pair<eckit::URI, eckit::Offset>>& metadata,
                   std::set<eckit::URI>& data) const override;

    // Control access properties of the DB
    void control(const ControlAction& action, const ControlIdentifiers& identifiers) const override;

protected: // members

    Key currentIndexKey_;
    size_t schema_size;
    par_handle schema_handle;
    par_handle index_handle;

private: // members

    friend class TocWipeVisitor;
    friend class TocMoveVisitor;

    // non-owning
    Schema schema_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif
