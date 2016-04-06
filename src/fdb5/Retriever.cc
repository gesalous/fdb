/*
 * (C) Copyright 1996-2016 ECMWF.
 * 
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
 * In applying this licence, ECMWF does not waive the privileges and immunities 
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/memory/ScopedPtr.h"
#include "eckit/io/MultiHandle.h"
#include "eckit/types/Types.h"
#include "eckit/log/Timer.h"

#include "marslib/MarsTask.h"

#include "fdb5/Retriever.h"
#include "fdb5/RetrieveOp.h"
#include "fdb5/ForwardOp.h"
#include "fdb5/UVOp.h"
#include "fdb5/MasterConfig.h"
#include "fdb5/DB.h"
#include "fdb5/Key.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

Retriever::Retriever(const MarsTask& task) :
    task_(task),
    winds_(task.request())
{
}

Retriever::~Retriever()
{
}

eckit::DataHandle* Retriever::retrieve()
{
    eckit::ScopedPtr<MultiHandle> result( new MultiHandle() );

    VecDB dbs = MasterConfig::instance().openSessionDBs(task_);

    for( VecDB::const_iterator jdb = dbs.begin(); jdb != dbs.end(); ++jdb) {

        const DB& db = **jdb;

        eckit::ScopedPtr<MultiHandle> partial( new MultiHandle() );

        Key key;

        RetrieveOp op(db, *partial);
        retrieve(key, db.schema(), db.schema().begin(), op);

        *result += partial.release();
    }

    /// @TODO eventually we want to sort the multihandle
    ///       compress() is called in saveInto() however MultiHandle isn't calling sort() on children

    return result.release();
}

void Retriever::retrieve(Key& key,
                         const Schema& schema,
                         Schema::const_iterator pos,
                         Op& op)
{
    if(pos != schema.end()) {

        std::vector<std::string> values;

        const std::string& param = *pos;
        task_.request().getValues(param, values);

        Log::info() << "Expanding parameter (" << param << ") with " << values << std::endl;

        ASSERT(values.size());

        /// @TODO once Schema / Param are typed, we can call:
        ///         Param p = pos->param();
        ///         values = p->getValues(userReq);
        ///         p->makeOp(op);

        eckit::ScopedPtr<Op> newOp( (param == "param") ?
                                        static_cast<Op*>(new UVOp(op, winds_)) :
                                        static_cast<Op*>(new ForwardOp(op)) );

        Schema::const_iterator next = pos;
        ++next;

        for(std::vector<std::string>::const_iterator j = values.begin(); j != values.end(); ++j) {

            op.enter();

            key.set(param, *j);
            retrieve(key, schema, next, *newOp);
            key.unset(param);

            op.leave();
        }
    }
    else {
        op.execute(task_, key, op);
    }
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
