/*
 * (C) Copyright 1996-2017 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/memory/ScopedPtr.h"
#include "eckit/option/CmdArgs.h"

#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBTool.h"

using namespace eckit;

//----------------------------------------------------------------------------------------------------------------------

class FDBLsToc : public fdb5::FDBTool {

  public: // methods

    FDBLsToc(int argc, char **argv) :
        fdb5::FDBTool(argc, argv) {}

  private: // methods

    virtual void usage(const std::string &tool) const;
    virtual void execute(const eckit::option::CmdArgs& args);

    void process(const eckit::PathName& path) const;
};

void FDBLsToc::usage(const std::string &tool) const {
    Log::info() << std::endl
                << "Usage: " << tool << " [path1] [path2] ..." << std::endl;
    fdb5::FDBTool::usage(tool);
}


void FDBLsToc::execute(const eckit::option::CmdArgs& args) {

    for (size_t i = 0; i < args.count(); i++) {

        eckit::PathName path(args(i));

        process(path);
    }
}

void FDBLsToc::process(const eckit::PathName& path) const {

    eckit::Log::info() << "Listing " << path << std::endl;

    fdb5::TocHandler handler(path, true);

    handler.dump(eckit::Log::info());
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBLsToc app(argc, argv);
    return app.start();
}

