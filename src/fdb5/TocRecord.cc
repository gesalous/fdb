/*
 * (C) Copyright 1996-2013 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "mars_server_version.h"

#include "eckit/types/DateTime.h"
#include "fdb5/TocRecord.h"

using namespace eckit;

namespace fdb5 {

//-----------------------------------------------------------------------------

void TocRecord::init()
{
	head_.fdbVersion_ = ::mars_server_version_int();

	SYSCALL( ::gettimeofday( &head_.timestamp_, 0 ) );

	head_.pid_       = ::getpid();
	head_.uid_       = ::getuid();

	SYSCALL( ::gethostname( head_.hostname_.data(), head_.hostname_.static_size() ) );

	marker_[0] = '4';
	marker_[1] = '2';
}

PathName TocRecord::path() const
{
	return PathName( payload_.data() );
}

TocRecord::MetaData TocRecord::metadata() const
{
    return metadata_;
}

void TocRecord::print(std::ostream& out) const
{
  out << "TocRecord("
      << "tag:" << head_.tag_ << ","
      << "tagVersion:" << head_.tagVersion_ << ","
      << "fdbVersion:" << head_.fdbVersion_ << ","
      << "timestamp:" << head_.timestamp_.tv_sec << "." << head_.timestamp_.tv_usec << ","
      << "pid:" << head_.pid_ << ","
      << "uid:" << head_.uid_ << ","
      << "hostname:" << head_.hostname_ << ","
      << "metadata:" << metadata_ << ","
      << "payload:" << payload_ << ")";
}

//-----------------------------------------------------------------------------

} // namespace fdb5
