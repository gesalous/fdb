/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/log/Log.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/parallax/ParallaxCatalogueReader.h"
#include "fdb5/toc/TocIndex.h"
#include "fdb5/toc/TocStats.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ParallaxCatalogueReader::ParallaxCatalogueReader(const Key& key, const fdb5::Config& config) :
    ParallaxCatalogue(key, config) {
    loadIndexesAndRemap();
}

ParallaxCatalogueReader::ParallaxCatalogueReader(const eckit::URI& uri, const fdb5::Config& config) :
    ParallaxCatalogue(uri.path(), ControlIdentifiers{}, config) {
    loadIndexesAndRemap();
}

ParallaxCatalogueReader::~ParallaxCatalogueReader() {
    LOG_DEBUG_LIB(LibFdb5) << "Closing DB " << *dynamic_cast<ParallaxCatalogue*>(this) << std::endl;
}

void ParallaxCatalogueReader::loadIndexesAndRemap() {
    std::vector<Key> remapKeys;
    std::vector<Index> indexes = loadIndexes(false, nullptr, nullptr, &remapKeys);

    ASSERT(remapKeys.size() == indexes.size());
    indexes_.reserve(remapKeys.size());
    for (size_t i = 0; i < remapKeys.size(); ++i) {
        indexes_.emplace_back(indexes[i], remapKeys[i]);
    }
}

bool ParallaxCatalogueReader::selectIndex(const Key &key) {

    if(currentIndexKey_ == key) {
        return true;
    }

    currentIndexKey_ = key;
    matching_.clear();

    for (auto idx = indexes_.begin(); idx != indexes_.end(); ++idx) {
        if (idx->first.key() == key) {
            matching_.push_back(&(*idx));
        }
    }

    LOG_DEBUG_LIB(LibFdb5) << "ParallaxCatalogueReader::selectIndex " << key << ", found "
                                << matching_.size() << " matche(s)" << std::endl;

    return (matching_.size() != 0);
}

void ParallaxCatalogueReader::deselectIndex() {
    NOTIMP; //< should not be called
}

bool ParallaxCatalogueReader::open() {

    // This used to test if indexes_.empty(), but it is perfectly valid to have a DB with no indexes
    // if it has been created with fdb-root --create.
    // See MARS-

    if (!ParallaxCatalogue::exists()) {
        return false;
    }

    ParallaxCatalogue::loadSchema();
    return true;
}

bool ParallaxCatalogueReader::axis(const std::string &keyword, eckit::StringSet &s) const {
    bool found = false;
    for (auto m = matching_.begin(); m != matching_.end(); ++m) {
        if ((*m)->first.axes().has(keyword)) {
            found = true;
            const eckit::DenseSet<std::string>& a = (*m)->first.axes().values(keyword);
            s.insert(a.begin(), a.end());
        }
    }
    return found;
}

void ParallaxCatalogueReader::close() {
    for (auto m = indexes_.begin(); m != indexes_.end(); ++m) {
        m->first.close();
    }
}

bool ParallaxCatalogueReader::retrieve(const Key& key, Field& field) const {
    LOG_DEBUG_LIB(LibFdb5) << "Trying to retrieve key " << key << std::endl;
    LOG_DEBUG_LIB(LibFdb5) << "Scanning indexes " << matching_.size() << std::endl;

    for (auto m = matching_.begin(); m != matching_.end(); ++m) {
        const Index& idx((*m)->first);
        Key remapKey = (*m)->second;

        if (idx.mayContain(key)) {
            const_cast<Index&>(idx).open();
            if (idx.get(key, remapKey, field)) {
                return true;
            }
        }
    }
    return false;
}

void ParallaxCatalogueReader::print(std::ostream &out) const {
    out << "ParallaxCatalogueReader(" << directory() << ")";
}

std::vector<Index> ParallaxCatalogueReader::indexes(bool sorted) const {

    std::vector<Index> returnedIndexes;
    returnedIndexes.reserve(indexes_.size());
    for (auto idx = indexes_.begin(); idx != indexes_.end(); ++idx) {
        returnedIndexes.emplace_back(idx->first);
    }

    // If required, sort the indexes by file, and location within the file, for efficient iteration.
    if (sorted) {
        std::sort(returnedIndexes.begin(), returnedIndexes.end(), TocIndexFileSort());
    }

    return returnedIndexes;
}

static CatalogueBuilder<ParallaxCatalogueReader> builder("parallax.reader");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
