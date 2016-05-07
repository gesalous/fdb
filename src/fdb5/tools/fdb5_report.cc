/*
 * (C) Copyright 1996-2013 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/option/CmdArgs.h"
#include "fdb5/toc/TocHandler.h"
#include "fdb5/tools/FDBInspect.h"
#include "fdb5/database/Index.h"
#include "fdb5/toc/Statistics.h"
#include "fdb5/toc/ReportVisitor.h"
#include "eckit/log/BigNum.h"

//----------------------------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------------------------

class FDBReport : public fdb5::FDBInspect {

  public: // methods

    FDBReport(int argc, char **argv) :
        fdb5::FDBInspect(argc, argv),
        count_(0) {


    }

  private: // methods

    virtual void process(const eckit::PathName &, const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual void init(const eckit::option::CmdArgs &args);
    virtual void finish(const eckit::option::CmdArgs &args);
    fdb5::Statistics stats_;
    size_t count_;

};

void FDBReport::usage(const std::string &tool) const {

    eckit::Log::info() << std::endl << "Usage: " << tool << " [path1|request1] [path2|request2] ..." << std::endl;
    FDBInspect::usage(tool);
}

void FDBReport::init(const eckit::option::CmdArgs &args) {
}

void FDBReport::process(const eckit::PathName &path, const eckit::option::CmdArgs &args) {

    eckit::Log::info() << "Scanning " << path << std::endl;

    fdb5::TocHandler handler(path);
    eckit::Log::info() << "Database key " << handler.databaseKey() << std::endl;

    fdb5::ReportVisitor visitor(path);
    // handler.visitor(visitor);

    std::vector<fdb5::Index *> indexes = handler.loadIndexes();


    for (std::vector<fdb5::Index *>::const_iterator i = indexes.begin(); i != indexes.end(); ++i) {
        (*i)->entries(visitor);
    }

    // if(details)
    // visitor.report(eckit::Log::info());

    handler.freeIndexes(indexes);

    stats_ += visitor.totals();
    count_ ++;
}


void FDBReport::finish(const eckit::option::CmdArgs &args) {

        eckit::Log::info() << std::endl
        << "Number of databases          : " << eckit::BigNum(count_) << std::endl;
        stats_.report(eckit::Log::info());
        eckit::Log::info() << std::endl;

}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char **argv) {
    FDBReport app(argc, argv);
    return app.start();
}