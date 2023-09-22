/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
#include <signal.h>
#include <iostream>
#include "eckit/config/Resource.h"
#include "eckit/log/BigNum.h"
#include "fdb5/toc/BTreeIndex.h"
#include "fdb5/toc/FieldRef.h"
#include "fdb5/toc/TocIndex.h"
namespace fdb5 {

#define LOG_DEBUG(msg)                                                 \
    std::cout << __FILE__ << ":" << __func__ << ":" << __LINE__ << " " \
              << " DEBUG: " << msg << std::endl;


#define LOG_FATAL(msg)                                                                             \
    std::cout << __FILE__ << ":" << __func__ << ":" << __LINE__ << " FATAL: " << msg << std::endl; \
    _exit(EXIT_FAILURE);
//----------------------------------------------------------------------------------------------------------------------

class LSMIndex : public BTreeIndex {

public:
    LSMIndex(const eckit::PathName& path, bool readOnly, off_t offset) {
        LOG_DEBUG("Initializing LSM index: " + path.asString());
    }

    ~LSMIndex() {
        LOG_DEBUG("Destroying LSM index.");
    }

    bool get(const std::string& key, FieldRef& data) const {
        LOG_DEBUG("LSM get operation.");
        return true;
    }

    bool set(const std::string& key, const FieldRef& data) {
        LOG_DEBUG("LSM set operation.");
        return true;
    }

    void flush() {
        LOG_DEBUG("LSM flush operation.");
    }

    void sync() {
        LOG_DEBUG("LSM sync operation.");
    }

    void flock() {
        LOG_DEBUG("LSM flock operation.");
    }

    void funlock() {
        LOG_DEBUG("LSM funlock operation.");
    }

    void visit(BTreeIndexVisitor& visitor) const {
        LOG_DEBUG("LSM visit operation.");
    }
};


class LSMIndexVisitor {
    BTreeIndexVisitor& visitor_;

public:
    LSMIndexVisitor(BTreeIndexVisitor& visitor) :
        visitor_(visitor) {
        LOG_DEBUG("Initializing Index Visitor.");
    }

    void clear() {
    }

    void push_back(const std::pair<std::string, FieldRef>& kv) {
        visitor_.visit(kv.first, kv.second);
    }
};

static BTreeIndexBuilder<LSMIndex> lsmIndexBuilder("LSMIndex");

}  // namespace fdb5
