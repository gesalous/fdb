/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <sys/file.h>

#include "fdb5/toc/ExpverFileSpaceHandler.h"

#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/thread/AutoLock.h"
#include "eckit/io/FileLock.h"

#include "fdb5/toc/FileSpace.h"
#include "fdb5/database/Key.h"
#include "fdb5/LibFdb.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

ExpverFileSpaceHandler::ExpverFileSpaceHandler() :
    fdbExpverFileSystems_(Resource<PathName>("fdbExpverFileSystems", "~fdb/etc/fdb/expver_filesystems")) {
}

ExpverFileSpaceHandler::~ExpverFileSpaceHandler() {
}

void ExpverFileSpaceHandler::load() const {

    Log::debug<LibFdb>() << "Loading " << fdbExpverFileSystems_ << std::endl;

    std::ifstream in(fdbExpverFileSystems_.localPath());

    if(!in) {
        std::ostringstream oss;
        oss <<  fdbExpverFileSystems_ << Log::syserr;
        Log::error() << oss.str() << std::endl;
        throw CantOpenFile(oss.str(), Here());
    }

    char line[1024];
    size_t lineNo = 0;
    Tokenizer parse(" ");
    std::vector<std::string> s;

    while(in.getline(line, sizeof(line)))
    {
        ++lineNo;
        s.clear();

        parse(line,s);

        size_t i = 0;
        while( i < s.size() ) /* cleanup entries that are empty */
        {
            if(s[i].length() == 0)
                s.erase(s.begin()+i);
            else
                i++;
        }

        if(s.size() == 0 || s[0][0] == '#')
            continue;

        if(s.size() != 2) {
            std::ostringstream oss;
            oss << "Bad line (" << lineNo << ") in configuration file " << fdbExpverFileSystems_ << " -- should have format 'expver filesystem'";
            throw ReadError(oss.str(), Here());
        }

        table_[ s[0] ] = PathName(s[1]);
    }
}

eckit::PathName ExpverFileSpaceHandler::append(const std::string& expver, const PathName& path) const
{
    // obtain exclusive lock to file

    PathName lockFile = fdbExpverFileSystems_ + ".lock";

    eckit::FileLock locker(lockFile);
    eckit::AutoLock<eckit::FileLock> lock(locker);

    // read the file first to check that this expver hasn't been inserted yet by another process

    std::ifstream fi(fdbExpverFileSystems_.localPath());

    if(!fi) {
        std::ostringstream oss;
        oss <<  fdbExpverFileSystems_ << Log::syserr;
        Log::error() << oss.str() << std::endl;
        throw CantOpenFile(oss.str(), Here());
    }

    char line[4*1024];
    size_t lineNo = 0;
    Tokenizer parse(" ");
    std::vector<std::string> s;

    while(fi.getline(line, sizeof(line)))
    {
        ++lineNo;
        s.clear();

        parse(line,s);

        size_t i = 0;
        while( i < s.size() ) /* cleanup entries that are empty */
        {
            if(s[i].length() == 0)
                s.erase(s.begin()+i);
            else
                i++;
        }

        if(s.size() == 0 || s[0][0] == '#')
            continue;

        if(s.size() != 2) {
            std::ostringstream oss;
            oss << "Bad line (" << lineNo << ") in configuration file " << fdbExpverFileSystems_ << " -- should have format 'expver filesystem'";
            throw ReadError(oss.str(), Here());
        }

        if(s[0] == expver) {
            Log::debug<LibFdb>() << "Found expver " << expver << " " << path << " in " << fdbExpverFileSystems_ << std::endl;
            return PathName(s[1]);
        }
    }

    fi.close();

    std::ofstream of(fdbExpverFileSystems_.localPath(), std::ofstream::app);

    if(!of) {
        std::ostringstream oss;
        oss <<  fdbExpverFileSystems_ << Log::syserr;
        Log::error() << oss.str() << std::endl;
        throw WriteError(oss.str(), Here());
    }

    // append to the file

    Log::debug<LibFdb>() << "Appending expver " << expver << " " << path << " to " << fdbExpverFileSystems_ << std::endl;

    of << expver << " " << path << std::endl;

    of.close();

    return path;
}

PathName ExpverFileSpaceHandler::select(const Key& key, const FileSpace& fs) const
{
    return FileSpaceHandler::lookup("WeightedRandom").selectFileSystem(key, fs);
}

eckit::PathName ExpverFileSpaceHandler::selectFileSystem(const Key& key, const FileSpace& fs) const {

    AutoLock<Mutex> lock(mutex_);

    // check if key is mapped already to a filesystem

    if(table_.empty()) load();

    std::string expver = key.get("expver");

    PathTable::const_iterator itr = table_.find(expver);
    if(itr != table_.end()) {
        Log::debug<LibFdb>() << "Found expver " << expver << " " << itr->second << " in " << fdbExpverFileSystems_ << std::endl;
        return itr->second;
    }

    // if not, assign a filesystem

    PathName maybe = select(key, fs);

    PathName selected = append(expver, maybe);

    table_[expver] = selected;

    return selected;
}

FileSpaceHandlerRegister<ExpverFileSpaceHandler> expver("expver");

//----------------------------------------------------------------------------------------------------------------------

}  // namespace fdb5