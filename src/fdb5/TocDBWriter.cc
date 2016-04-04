/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "mars_server_config.h"

#include <cstring>
#include <fcntl.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>

#ifdef EC_HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#ifdef EC_HAVE_SYS_MOUNT_H
#include <sys/param.h>
#include <sys/mount.h>
#endif

#include "eckit/exception/Exceptions.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/config/Resource.h"
#include "eckit/log/Bytes.h"
#include "eckit/io/AIOHandle.h"

#include "fdb5/Error.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/TocActions.h"
#include "fdb5/TocDBWriter.h"
#include "fdb5/TocSchema.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

TocDBWriter::TocDBWriter(const Key& key) : TocDB(key)
{
    indexType_ = eckit::Resource<std::string>( "fdbIndexType", "BTreeIndex" );

    const PathName& tocPath = schema_.tocPath();
    if( !tocPath.exists() )
    {
        TocInitialiser init(tocPath);
    }

    blockSize_ = eckit::Resource<long>( "blockSize", -1 );

    if( blockSize_ < 0 ) // take blockSize_ from statfs
    {
        struct statfs s;
        SYSCALL2( statfs(const_cast<char*>( tocPath.localPath() ), &s ), tocPath );
        if( s.f_bsize > 0 )
            blockSize_ = s.f_bsize;
        /// @note should we use s.f_iosize, optimal transfer block size, on macosx ?
    }

    if( blockSize_ > 0 ) {
        padding_.resize(blockSize_,'\0');
    }

    aio_ = eckit::Resource<bool>("fdbAsyncWrite",false);

    Log::info() << "TocDBWriter for TOC [" << tocPath << "] with block size of " << Bytes(blockSize_) << std::endl;
}

TocDBWriter::~TocDBWriter()
{
    // ensure consistent state before writing Toc entry

    flush();

    // close all data handles followed by all indexes, before writing Toc entry

    closeDataHandles();
    closeIndexes();

    // finally write all Toc entries

    closeTocEntries();
}

void TocDBWriter::archive(const Key& userkey, const void *data, Length length)
{
    ASSERT( match(userkey) ); // paranoic check

    TocIndex& toc = getTocIndex(userkey);

    Index& index = getIndex( toc.index() );

    /// @todo CHANGE THIS : CREATES A NEW FILE PER FIELD

    PathName dataPath = getDataPath(userkey);

    eckit::DataHandle& dh = getDataHandle(dataPath);

    Offset position = dh.position();

    dh.write( data, length );

    if( blockSize_ > 0 ) // padding?
    {
        long long len = length;
        size_t paddingSize       =  (( len + blockSize_-1 ) / blockSize_ ) * blockSize_ - len;
        if(paddingSize)
            dh.write( &padding_[0], paddingSize );
    }

    Index::Key key = schema_.dataIdx(userkey); // reduced key with only index entries

    if( MasterConfig::instance().IgnoreOnOverwrite() ||
        MasterConfig::instance().WarnOnOverwrite()   ||
        MasterConfig::instance().FailOnOverwrite() )
    {
        bool duplicated = index.exists(key);
        if( duplicated )
        {
            if( MasterConfig::instance().IgnoreOnOverwrite() ) return;

            std::ostringstream msg;
            msg << "Overwrite to FDB with key: " << key << std::endl;

            Log::error() << msg.str() << std::endl;

            if( MasterConfig::instance().FailOnOverwrite() )
                throw fdb5::Error( Here(), msg.str() );
        }
    }

    Index::Field field (dataPath, position, length);

    Log::debug(2) << " pushing {" << key << "," << field << "}" << std::endl;

    index.put(key,field);
}

void TocDBWriter::flush()
{
    flushIndexes();
    flushDataHandles();
}

Index& TocDBWriter::getIndex(const PathName& path)
{
    Index* idx = getCachedIndex(path);
    if( !idx )
    {
        idx = openIndex( path );
        ASSERT(idx);
        indexes_[ path ] = idx;
    }
    return *idx;
}

Index* TocDBWriter::openIndex(const PathName& path) const
{
    return Index::create( indexType_, path, Index::WRITE );
}

Index* TocDBWriter::getCachedIndex( const PathName& path ) const
{
    IndexStore::const_iterator itr = indexes_.find( path );
    if( itr != indexes_.end() )
        return itr->second;
    else
        return 0;
}

eckit::DataHandle* TocDBWriter::getCachedHandle( const PathName& path ) const
{
    HandleStore::const_iterator itr = handles_.find( path );
    if( itr != handles_.end() )
        return itr->second;
    else
        return 0;
}

void TocDBWriter::closeDataHandles()
{
    for( HandleStore::iterator itr = handles_.begin(); itr != handles_.end(); ++itr )
    {
        eckit::DataHandle* dh = itr->second;
        if( dh )
        {
            dh->close();
            delete dh;
            itr->second = 0;
        }
    }
}

void TocDBWriter::closeIndexes()
{
    for( IndexStore::iterator itr = indexes_.begin(); itr != indexes_.end(); ++itr )
    {
        Index* idx = itr->second;
        if( idx )
        {
            delete idx;
            itr->second = 0;
        }
    }
}

eckit::DataHandle* TocDBWriter::createFileHandle(const PathName& path)
{
    return path.fileHandle();
}

DataHandle* TocDBWriter::createAsyncHandle(const PathName& path)
{
    size_t nbBuffers  = eckit::Resource<unsigned long>("fdbNbAsyncBuffers",4);
    size_t sizeBuffer = eckit::Resource<unsigned long>("fdbSizeAsyncBuffer",64*1024*1024);

    return new AIOHandle(path,nbBuffers,sizeBuffer);
}

eckit::DataHandle* TocDBWriter::createPartHandle(const PathName& path, Offset offset, Length length)
{
    return path.partHandle(offset,length);
}

DataHandle& TocDBWriter::getDataHandle( const PathName& path )
{
    eckit::DataHandle* dh = getCachedHandle( path );
    if( !dh )
    {
        dh = aio_ ? createAsyncHandle( path ) : createFileHandle( path );
        handles_[path] = dh;
        ASSERT( dh );
        dh->openForAppend(0);
    }
    return *dh;
}

TocIndex& TocDBWriter::getTocIndex(const Key& key)
{
    TocIndex* toc = 0;

    std::string tocEntry = schema_.tocEntry(key);

    TocIndexStore::const_iterator itr = tocEntries_.find( tocEntry );
    if( itr != tocEntries_.end() )
    {
        toc = itr->second;
    }
    else
    {
        toc = new TocIndex(schema_, key);
        tocEntries_[ tocEntry ] = toc;
    }

    ASSERT( toc );

    return *toc;
}

PathName TocDBWriter::getDataPath(const Key& key)
{
    std::string prefix = schema_.dataFilePrefix(key);

    PathStore::const_iterator itr = dataPaths.find( prefix );
    if( itr != dataPaths.end() )
        return itr->second;

    PathName dataPath = schema_.generateDataPath(key);

    dataPaths[ prefix ] = dataPath;

    return dataPath;
}

void TocDBWriter::flushIndexes()
{
    IndexStore::iterator itr = indexes_.begin();
    for( ; itr != indexes_.end(); ++itr )
    {
        Index* idx = itr->second;
        if( idx )
            idx->flush();
    }
}

void TocDBWriter::flushDataHandles()
{
    HandleStore::iterator itr = handles_.begin();
    size_t i = 0;
    for( ; itr != handles_.end(); ++itr, ++i )
    {
        eckit::DataHandle* dh = itr->second;
        if( dh )
            dh->flush();
    }
}

void TocDBWriter::closeTocEntries()
{
    for( TocIndexStore::iterator itr = tocEntries_.begin(); itr != tocEntries_.end(); ++itr )
    {
        TocIndex* toc = itr->second;
        ASSERT( toc );
        delete toc;
    }
    tocEntries_.clear();
}

void TocDBWriter::print(std::ostream &out) const
{
    out << "TocDBWriter()";
}

DBBuilder<TocDBWriter> TocDBWriter_Builder("toc.writer");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5