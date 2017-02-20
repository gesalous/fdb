/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/utils/Translator.h"

#include "eckit/types/Date.h"
#include "marslib/MarsRequest.h"

#include "fdb5/types/TypesFactory.h"
#include "fdb5/types/TypeExpver.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TypeExpver::TypeExpver(const std::string &name, const std::string &type) :
    Type(name, type) {
}

TypeExpver::~TypeExpver() {
}


std::string TypeExpver::tidy(const std::string&,
                             const std::string& value) const {
    std::stringstream oss;
    oss << std::setfill('0') << std::setw(4) << value;
    return oss.str();
}


void TypeExpver::print(std::ostream &out) const {
    out << "TypeExpver[name=" << name_ << "]";
}

static TypeBuilder<TypeExpver> type("Expver");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
