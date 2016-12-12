/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/config/Resource.h"
#include "eckit/log/Timer.h"

#include "fdb5/config/MasterConfig.h"
#include "fdb5/database/Key.h"
#include "fdb5/pmem/PMemDB.h"
#include "fdb5/pmem/PMemFieldLocation.h"
#include "fdb5/rules/Rule.h"
#include "fdb5/toc/RootManager.h"

#include "pmem/PersistentString.h"

#include <pwd.h>

using namespace eckit;
using namespace pmem;

namespace fdb5 {
namespace pmem {

//----------------------------------------------------------------------------------------------------------------------

PMemDB::PMemDB(const Key& key) :
    DB(key),
    // Utilise the RootManager from the TocDB to get a sensible location. Note that we are NOT
    // using this as a directory, but rather as a pool file.
    poolDir_(RootManager::directory(key)),
    currentIndex_(0) {

    // If opened with a key in this manner, it is for write, so should open up fully.
    initialisePool();
}


PMemDB::PMemDB(const PathName& poolDir) :
    DB(Key()),
    poolDir_(poolDir),
    currentIndex_(0) {}

PMemDB::~PMemDB() {
    close();
}


void PMemDB::initialisePool() {

    ASSERT(currentIndex_ == 0);

    // Get (or create) the pool
    pool_.reset(Pool::obtain(poolDir_, Resource<size_t>("fdbPMemPoolSize", 1024 * 1024 * 1024), dbKey_));

    root_ = &pool_->root();

    dataPoolMgr_.reset(new DataPoolManager(poolDir_, *root_, pool_->baseRoot().uuid()));

    // Initialise the schema object for comparison against the global schema.

    const PersistentString& schemaBuf(root_->schema());
    std::string s(schemaBuf.c_str(), schemaBuf.length());
    std::istringstream iss(s);
    schema_.load(iss);

    if (dbKey_.empty())
        dbKey_ = root_->databaseKey();
}


//----------------------------------------------------------------------------------------------------------------------

bool PMemDB::open() {

    if (!pool_)
        initialisePool();

    return true;
}

bool PMemDB::exists() const {
    return Pool::exists(poolDir_);
}

void PMemDB::archive(const Key &key, const void *data, Length length) {
    Log::error() << "archive not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::visitEntries(EntryVisitor& visitor) {

    Log::error() << dbKey_ << std::endl;


    ASSERT(pool_ && root_);
    root_->visitLeaves(visitor, *dataPoolMgr_, schema());
}

eckit::DataHandle * PMemDB::retrieve(const Key &key) const {
    Log::error() << "retrieve not implemented for " << *this << std::endl;
    NOTIMP;
}

void PMemDB::flush() {
    // Intentionally left blank.
    // The libpmemobj functionality works with atomic writes, and everything is flushed before it returns success.
}

void PMemDB::close() {

    // Close any open indices

    for (IndexStore::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
        Index* idx = it->second;
        idx->close();
        delete idx;
    }
}

void PMemDB::checkSchema(const Key &key) const {
    eckit::Timer timer("PMemDB::checkSchema()");
    ASSERT(key.rule());
    schema_.compareTo(key.rule()->schema());
}

void PMemDB::axis(const std::string &keyword, eckit::StringSet &s) const {
    Log::error() << "axis not implemented for " << *this << std::endl;
    NOTIMP;
}

bool PMemDB::selectIndex(const Key &key) {
    NOTIMP;
}

void PMemDB::dump(std::ostream& out, bool simple) {

    class PMemLocationPrinter : public FieldLocationVisitor {
    public:
        PMemLocationPrinter(std::ostream& out, DataPoolManager& poolMgr) : out_(out), poolMgr_(poolMgr) {}
        virtual void operator() (const PMemFieldLocation& location) {
            out_ << "  pool_uuid: " << location.node().uuid() << std::endl;
            out_ << "  data_pool: " << poolMgr_.dataPoolPath(location.node().uuid()) << std::endl;
            out_ << "  offset: " << location.node().offset() << std::endl;
        }
    private:
        std::ostream& out_;
        DataPoolManager& poolMgr_;
    };

    // ----------------------------------------------------------------------

    class DumpVisitor : public fdb5::EntryVisitor {

    public:
        DumpVisitor(std::ostream& out, DataPoolManager& poolMgr, Schema& schema, Key& dbKey) :
            out_(out), poolMgr_(poolMgr), schema_(schema), dbKey_(dbKey) {}

    private:
        virtual void visit(const fdb5::Index &index,
                           const std::string &indexFingerprint,
                           const std::string &fieldFingerprint,
                           const fdb5::Field &field) {


            out_ << "PMEM_ENTRY" << std::endl;

            fdb5::Key key(fieldFingerprint, schema_.ruleFor(dbKey_, index.key()));
            out_ << "  Key: " << dbKey_ << index.key() << key;

            PMemLocationPrinter locPrinter(out_, poolMgr_);
            field.location().visit(locPrinter);

            out_ << std::endl;
        }

    private: // members
        std::ostream& out_;
        DataPoolManager& poolMgr_;
        Schema& schema_;
        Key& dbKey_;
    };

    // ----------------------------------------------------------------------

    // Check that things are open
    ASSERT(pool_);

    // Output details of the DB itself

    out << std::endl << "PMEM_DB" << std::endl;
    out << "  Version: " << root_->version() << std::endl;
    out << "  Created: " << root_->created() << std::endl;

    out << "  uid: ";
    struct passwd* p = getpwuid(root_->uid());
    if (p)
        out << p->pw_name;
    else
        out << root_->uid();
    out << std::endl;

    Log::info() << "  Key: " << dbKey_ << std::endl << std::endl;

    // And dump the rest of the stuff

    DumpVisitor visitor(out, *dataPoolMgr_, schema_, dbKey_);

    visitEntries(visitor);
}

void PMemDB::visit(DBVisitor &visitor) {
    visitor(*this);
}

const Schema& PMemDB::schema() const {

    // We must have opened the DB for the schema to have been loaded.
    ASSERT(pool_);
    return schema_;
}

void PMemDB::deselectIndex() {

    // This is essentially a NOP, as we don't have any files to open, etc.
    currentIndex_ = 0;
}

eckit::PathName PMemDB::basePath() const {
    return poolDir_;
}

size_t PMemDB::schemaSize() const {
    ASSERT(pool_);
    return root_->schema().length();
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace pmem
} // namespace fdb5
