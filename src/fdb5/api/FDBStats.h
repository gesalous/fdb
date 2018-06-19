/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   FDBStats.h
/// @author Simon Smart
/// @date   June 2018

#ifndef fdb5_FDBStats_H
#define fdb5_FDBStats_H

#include <iosfwd>

#include "eckit/log/Statistics.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

class FDBStats : public eckit::Statistics {
public:

    FDBStats();
    ~FDBStats();

    void addArchive(size_t length, eckit::Timer& timer);
    void addRetrieve(size_t length, eckit::Timer& timer);
    void addFlush(eckit::Timer& timer);

    void report(std::ostream& out, const char* indent) const;

private: // members

    size_t numArchive_;
    size_t numFlush_;
    size_t numRetrieve_;

    size_t bytesArchive_;
    size_t bytesRetrieve_;

    size_t sumBytesArchiveSquared_;
    size_t sumBytesRetrieveSquared_;

    double elapsedArchive_;
    double elapsedFlush_;
    double elapsedRetrieve_;

    double sumArchiveTimingSquared_;
    double sumRetrieveTimingSquared_;
    double sumFlushTimingSquared_;
};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif