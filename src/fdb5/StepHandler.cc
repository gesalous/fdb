/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "eckit/utils/Translator.h"

#include "marslib/MarsTask.h"
#include "marslib/StepRange.h"

#include "fdb5/KeywordType.h"
#include "fdb5/StepHandler.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

StepHandler::StepHandler(const std::string& name) :
    KeywordHandler(name)
{
}

StepHandler::~StepHandler()
{
}

void StepHandler::getValues(const MarsRequest& request,
                               const std::string& keyword,
                               StringList& values,
                               const MarsTask& task,
                               const DB* db) const
{
    std::vector<std::string> steps;

    request.getValues(keyword, steps);

    Translator<StepRange, std::string> t;

    values.reserve(steps.size());

    for(std::vector<std::string>::const_iterator i = steps.begin(); i != steps.end(); ++i) {
        values.push_back(t(StepRange(*i)));
    }
}

void StepHandler::print(std::ostream &out) const
{
    out << "StepHandler(" << name_ << ")";
}

static KeywordHandlerBuilder<StepHandler> handler("Step");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5