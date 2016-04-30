/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date Sep 2012

#ifndef fdb5_BTreeIndex_H
#define fdb5_BTreeIndex_H

#include "eckit/eckit.h"

#include "eckit/container/BTree.h"
#include "eckit/io/Length.h"
#include "eckit/io/Offset.h"
#include "eckit/memory/NonCopyable.h"
#include "eckit/memory/ScopedPtr.h"

#include "eckit/types/FixedString.h"

#include "fdb5/database/Index.h"

namespace fdb5 {

class BTreeIndexVisitor {
public:
    virtual void visit(const std::string& key, const FileStore::FieldRef&) = 0;
};

class BTreeIndex {
public:
    virtual ~ BTreeIndex();
    virtual bool get(const std::string& key, FileStore::FieldRef& data) const = 0;
    virtual bool set(const std::string& key, const FileStore::FieldRef& data)= 0;
    virtual void flush() = 0;
    virtual void visit(BTreeIndexVisitor& visitor) const = 0;


    static const std::string& defaulType();

    static BTreeIndex* build(const std::string& type, const eckit::PathName& path, bool readOnly, off_t offset);

};


} // namespace fdb5

#endif