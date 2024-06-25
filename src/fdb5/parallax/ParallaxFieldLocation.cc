/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/parallax/ParallaxFieldLocation.h"
#include "fdb5/LibFdb5.h"
#include "fdb5/fdb5_config.h"

#if fdb5_HAVE_GRIB
#include "fdb5/io/SingleGribMungePartFileHandle.h"
#endif

namespace fdb5 {

::eckit::ClassSpec ParallaxFieldLocation::classSpec_ = {&FieldLocation::classSpec(), "ParallaxFieldLocation",};
::eckit::Reanimator<ParallaxFieldLocation> ParallaxFieldLocation::reanimator_;

//----------------------------------------------------------------------------------------------------------------------

//ParallaxFieldLocation::ParallaxFieldLocation() {}

ParallaxFieldLocation::ParallaxFieldLocation(const eckit::PathName path, eckit::Offset offset, eckit::Length length, const Key& remapKey) :
    FieldLocation(eckit::URI("parallax", path), offset, length, remapKey) {}

ParallaxFieldLocation::ParallaxFieldLocation(const eckit::URI &uri) :
    FieldLocation(uri) {}

ParallaxFieldLocation::ParallaxFieldLocation(const eckit::URI &uri, eckit::Offset offset, eckit::Length length, const Key& remapKey) :
    FieldLocation(uri, offset, length, remapKey) {}

ParallaxFieldLocation::ParallaxFieldLocation(const ParallaxFieldLocation& rhs) :
    FieldLocation(rhs.uri_, rhs.offset_, rhs.length_, rhs.remapKey_) {}

ParallaxFieldLocation::ParallaxFieldLocation(const UriStore &store, const FieldRef &ref) :
    FieldLocation(store.get(ref.uriId()), ref.offset(), ref.length(), Key()) {}

ParallaxFieldLocation::ParallaxFieldLocation(eckit::Stream& s) :
    FieldLocation(s) {}

std::shared_ptr<FieldLocation> ParallaxFieldLocation::make_shared() const {
    return std::make_shared<ParallaxFieldLocation>(std::move(*this));
}

eckit::DataHandle *ParallaxFieldLocation::dataHandle() const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    if (remapKey_.empty()) {
        return uri_.path().partHandle(offset(), length());
    } else {
#if fdb5_HAVE_GRIB
        return new SingleGribMungePartFileHandle(uri_.path(), offset(), length(), remapKey_);
#else
        NOTIMP;
#endif
    }
}

void ParallaxFieldLocation::print(std::ostream &out) const {
    out << "ParallaxFieldLocation[uri=" << uri_ << ",offset=" << offset() << ",length=" << length() << ",remapKey=" << remapKey_ << "]";
}

void ParallaxFieldLocation::visit(FieldLocationVisitor& visitor) const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    visitor(*this);
}

void ParallaxFieldLocation::encode(eckit::Stream& s) const {
    std::cout << "File: " << __FILE__ << ", Line: " << __LINE__ << ", Function: " << __func__ << std::endl;
    LOG_DEBUG(LibFdb5::instance().debug(), LibFdb5) << "ParallaxFieldLocation encode URI " << uri_.asRawString() << std::endl;

    FieldLocation::encode(s);
}

static FieldLocationBuilder<ParallaxFieldLocation> builder("parallax");

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
