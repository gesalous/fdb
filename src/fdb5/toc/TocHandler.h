/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Tiago Quintino
/// @date Dec 2014

#ifndef fdb5_TocHandler_H
#define fdb5_TocHandler_H

#include "eckit/config/LocalConfiguration.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/Length.h"
#include "eckit/memory/ScopedPtr.h"

#include "fdb5/io/LustreFileHandle.h"
#include "fdb5/database/DbStats.h"
#include "fdb5/toc/TocRecord.h"

namespace eckit {
class Configuration;
}

namespace fdb5 {

class Key;
class Index;

//-----------------------------------------------------------------------------

class TocHandler : private eckit::NonCopyable {

public: // typedefs

    typedef std::vector<TocRecord> TocVec;
    typedef std::vector< eckit::PathName > TocPaths;

public: // methods

    TocHandler( const eckit::PathName &dir, const eckit::Configuration& config=eckit::LocalConfiguration());

    /// For initialising sub tocs or diagnostic interrogation. Bool just for identification.
    TocHandler(const eckit::PathName& path, bool);

    ~TocHandler();

    bool exists() const;
    void checkUID() const;

    void writeInitRecord(const Key &tocKey);
    void writeClearRecord(const Index &);
    void writeSubTocRecord(const TocHandler& subToc);
    void writeIndexRecord(const Index &);

    std::vector<Index> loadIndexes() const;

    Key databaseKey();
    size_t numberOfRecords() const;

    const eckit::PathName& directory() const;
    const eckit::PathName& tocPath() const;
    const eckit::PathName& schemaPath() const;

    void dump(std::ostream& out, bool simple = false);
    void listToc(std::ostream& out);
    std::string dbOwner();

    DbStats stats() const;

protected: // members

    const eckit::PathName directory_;
    mutable long dbUID_;
    long userUID_;

    long dbUID() const;

protected: // methods

    static bool stripeLustre();

    static LustreStripe stripeIndexLustreSettings();

    static LustreStripe stripeDataLustreSettings();

private: // members

    friend class TocHandlerCloser;

    void openForAppend();
    void openForRead() const;
    void close() const;

    void append(TocRecord &r, size_t payloadSize);
    bool readNext(TocRecord &r) const;
    bool readNextInternal(TocRecord &r) const;

    std::string userName(long) const;

    eckit::PathName tocPath_;
    eckit::PathName schemaPath_;

    bool useSubToc_;
    bool isSubToc_;

    mutable int fd_;      ///< file descriptor, if zero file is not yet open.

    /// The sub toc is initialised in the read or write pathways for maintaining state.
    mutable eckit::ScopedPtr<TocHandler> subTocRead_;
    mutable eckit::ScopedPtr<TocHandler> subTocWrite_;
    mutable size_t count_;
};


//-----------------------------------------------------------------------------

} // namespace fdb5

#endif // fdb_TocHandler_H
