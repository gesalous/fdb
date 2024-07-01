/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/parallax/ParallaxCommon.h"

#include <pwd.h>
#include <unistd.h>

#include "eckit/config/Resource.h"
#include "eckit/filesystem/URIManager.h"
#include "eckit/log/Timer.h"

#include "fdb5/LibFdb5.h"
#include "fdb5/toc/RootManager.h"
#include "fdb5/io/LustreSettings.h"

namespace fdb5 {

eckit::PathName ParallaxCommon::findRealPath(const eckit::PathName& path) {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;  

    // realpath only works on existing paths, so work back up the path until
    // we find one that does, get the realpath on that, then reconstruct.
    if (path.exists()) return path.realName();

    return findRealPath(path.dirName()) / path.baseName();
}

ParallaxCommon::ParallaxCommon(const eckit::PathName& directory) :
    directory_(findRealPath(directory)),
    schemaPath_(directory_ / "schema"),
    dbUID_(static_cast<uid_t>(-1)),
    userUID_(::getuid()),
    dirty_(false) {}

void ParallaxCommon::checkUID() const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    static bool fdbOnlyCreatorCanWrite = eckit::Resource<bool>("fdbOnlyCreatorCanWrite", true);
    if (!fdbOnlyCreatorCanWrite) {
        return;
    }

    static std::vector<std::string> fdbSuperUsers =
        eckit::Resource<std::vector<std::string> >("fdbSuperUsers", "", true);

    if (dbUID() != userUID_) {
        if (std::find(fdbSuperUsers.begin(), fdbSuperUsers.end(), userName(userUID_)) ==
            fdbSuperUsers.end()) {
            std::ostringstream oss;
            oss << "Only user '" << userName(dbUID())
                << "' can write to FDB " << directory_ << ", current user is '"
                << userName(userUID_) << "'";

            throw eckit::UserError(oss.str());
        }
    }
}

uid_t ParallaxCommon::dbUID() const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    if (dbUID_ == static_cast<uid_t>(-1))
        dbUID_ = directory_.owner();

    return dbUID_;
}

std::string ParallaxCommon::userName(uid_t uid) {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    struct passwd* p = getpwuid(uid);

    if (p) {
        return p->pw_name;
    }
    else {
        return eckit::Translator<long, std::string>()(uid);
    }
}

}