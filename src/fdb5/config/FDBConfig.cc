/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/config/FDBConfig.h"


namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

FDBConfig::FDBConfig() {}


FDBConfig::FDBConfig(const eckit::Configuration& config) :
    LocalConfiguration(config) {}


FDBConfig::~FDBConfig() {}


// TODO: We could add this to expandTilde.

eckit::PathName FDBConfig::expandPath(const std::string &path) const {

    // If path starts with ~, split off the first component. If that is supplied in
    // the configuration, then use that instead!

    ASSERT(path.size() > 0);
    if (path[0] == '~') {
        if (path.length() > 1 && path[1] != '/') {
            size_t slashpos = path.find('/');
            if (slashpos != std::string::npos) {
                std::string key = path.substr(1, slashpos-1) + "_home";
                std::transform(key.begin(), key.end(), key.begin(), ::tolower);

                if (has(key)) {
                    std::string newpath = getString(key) + path.substr(slashpos);
                    return eckit::PathName(newpath);
                }
            }
        }

    }

    /// use the default expansion if unspecified (eckit::Main will expand FDB_HOME)
    return eckit::PathName(path);
}


//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5