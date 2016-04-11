/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "fdb5/Predicate.h"
#include "fdb5/Matcher.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Predicate::Predicate(const std::string& keyword, Matcher* matcher) :
    matcher_(matcher),
    keyword_(keyword)
{
//    dump(std::cout);
//    std::cout << std::endl;
}

Predicate::~Predicate()
{
}

bool Predicate::match(const Key& key) const
{
    return matcher_->match(keyword_, key);
}

void Predicate::dump(std::ostream& s) const
{
    matcher_->dump(s, keyword_);
}

void Predicate::print(std::ostream& out) const
{
    out << "Predicate()";
}

std::ostream& operator<<(std::ostream& s, const Predicate& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5