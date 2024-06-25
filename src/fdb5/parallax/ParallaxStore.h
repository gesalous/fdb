/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   ParallaxStore.h
/// @author Giorgos Saloustros
/// @author Michail Toutoudakis
/// @date   June 2024

#ifndef fdb5_ParallaxStore_H
#define fdb5_ParallaxStore_H

#include "fdb5/database/DB.h"
#include "fdb5/database/Index.h"
#include "fdb5/database/Store.h"
#include "fdb5/rules/Schema.h"
#include "fdb5/parallax/ParallaxCommon.h"
#include "fdb5/toc/TocEngine.h"

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

/// DB that implements the FDB on Parallax

class ParallaxStore : public Store, public ParallaxCommon {

public: // methods

    ParallaxStore(const Schema& schema, const Key& key, const Config& config);
    ParallaxStore(const Schema& schema, const eckit::URI& uri, const Config& config);

    ~ParallaxStore() override {}

    eckit::URI uri() const override;

    bool open() override { return true; }
    void flush() override;
    void close() override;

    void checkUID() const override { /*ParallaxCommon::checkUID();*/ }

    bool canMoveTo(const Key& key, const Config& config, const eckit::URI& dest) const override;
    void moveTo(const Key& key, const Config& config, const eckit::URI& dest, eckit::Queue<MoveElement>& queue) const override;
    void remove(const Key& key) const override;

protected: // methods

    std::string type() const override { return "file"; }

    bool exists() const override;

    eckit::DataHandle* retrieve(Field& field) const override;
    std::unique_ptr<FieldLocation> archive(const Key &key, const void *data, eckit::Length length) override;

    void remove(const eckit::URI& uri, std::ostream& logAlways, std::ostream& logVerbose, bool doit) const override;

    eckit::DataHandle *getCachedHandle( const eckit::PathName &path ) const;
    void closeDataHandles();
    eckit::DataHandle *createFileHandle(const eckit::PathName &path);
    eckit::DataHandle *createAsyncHandle(const eckit::PathName &path);
    eckit::DataHandle *createDataHandle(const eckit::PathName &path);
    eckit::DataHandle& getDataHandle( const eckit::PathName &path );
    eckit::PathName generateDataPath(const Key &key) const;
    eckit::PathName getDataPath(const Key &key) const;
    void flushDataHandles();

    void print( std::ostream &out ) const override;

private: // types

    typedef std::map< std::string, eckit::DataHandle * >  HandleStore;
    typedef std::map< Key, std::string > PathStore;

private: // members

    HandleStore handles_;    ///< stores the DataHandles being used by the Session

    mutable PathStore   dataPaths_;

};

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5

#endif //fdb5_TocStore_H
