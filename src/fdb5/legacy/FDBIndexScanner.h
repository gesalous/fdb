/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#ifndef fdb5_legacy_FDBIndexScanner_H
#define fdb5_legacy_FDBIndexScanner_H

#include <cstdio>

#include "eckit/thread/ThreadPool.h"
#include "eckit/filesystem/PathName.h"

namespace fdb5 {
namespace legacy {

class IndexCache;

//----------------------------------------------------------------------------------------------------------------------

class FDBIndexScanner : public eckit::ThreadPoolTask {
public:

    FDBIndexScanner(IndexCache& cache, const eckit::PathName& path);

    virtual ~FDBIndexScanner();

    virtual void execute();

private:

    void process(FILE*);

    IndexCache& cache_;
    eckit::PathName path_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace legacy
} // namespace fdb5

#endif