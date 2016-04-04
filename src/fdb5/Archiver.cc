/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <algorithm>

#include "eckit/io/DataHandle.h"
#include "eckit/log/Timer.h"

#include "marslib/MarsTask.h"

#include "fdb5/Archiver.h"
#include "fdb5/MasterConfig.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------


Archiver::Archiver()
{
}


Archiver::~Archiver()
{
    flush(); // certify that all sessions are flushed before closing them
}


void Archiver::archive(DataBlobPtr blob)
{
    NOTIMP;
}


void Archiver::archive(const MarsTask& task, eckit::DataHandle& source)
{
    NOTIMP;
}


void Archiver::archive(const Key& key, const void* data, eckit::Length length)
{
    DB& db = session(key);

    db.archive(key, data, length);
}


struct SessionFlusher {
    void operator()(const eckit::SharedPtr<DB>& session) { return session->flush(); }
};

void Archiver::flush()
{
    std::for_each(sessions_.begin(), sessions_.end(), SessionFlusher() );
}


struct SessionMatcher {
    SessionMatcher(const Key& key) : key_(key) {}
    bool operator()(const eckit::SharedPtr<DB>& session) {
        return session->match(key_);
    }
    const Key& key_;
};


DB& Archiver::session(const Key& key)
{
    store_t::iterator i = std::find_if(sessions_.begin(), sessions_.end(), SessionMatcher(key) );

    if(i != sessions_.end() )
        return **i;

    eckit::SharedPtr<DB> newSession = MasterConfig::instance().openSessionDB(key);
    ASSERT(newSession);
    sessions_.push_back(newSession);
    return *newSession;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5