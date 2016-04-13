/*
 * (C) Copyright 1996-2016 ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/log/Log.h"

#include "marslib/MarsRequest.h"

#include "fdb5/Predicate.h"
#include "fdb5/Rule.h"
#include "fdb5/Key.h"
#include "fdb5/ReadVisitor.h"
#include "fdb5/WriteVisitor.h"

using namespace eckit;

namespace fdb5 {

//----------------------------------------------------------------------------------------------------------------------

const Rule* matchFirst(const std::vector<Rule*>& rules, const Key& key, size_t depth)
{
    ASSERT(depth == 0);
    for(std::vector<Rule*>::const_iterator i = rules.begin(); i != rules.end(); ++i ) {
        if( (*i)->match(key) ) {
            return *i;
        }
    }
    return 0;
}

Rule::Rule(std::vector<Predicate*>& predicates, std::vector<Rule*>& rules)
{
    std::swap(predicates, predicates_);
    std::swap(rules, rules_);
}

Rule::~Rule()
{
    for(std::vector<Predicate*>::iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        delete *i;
    }

    for(std::vector<Rule*>::iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        delete *i;
    }
}

void Rule::expand( const MarsRequest& request,
                   std::vector<Predicate*>::const_iterator cur,
                   size_t depth,
                   std::vector<Key>& keys,
                   Key& full,
                   ReadVisitor& visitor) const {

    ASSERT(depth < 3);

    if(cur == predicates_.end()) {
        if(rules_.empty()) {
            ASSERT(depth == 2); /// we have 3 levels ATM
            if(!visitor.selectDatum( keys[2], full)) {
                return; // This it not useful
            }
        }
        else {

            switch(depth) {
                case 0:
                    if(!visitor.selectDatabase(keys[0], full)) {
                        return;
                    };
                    break;

                case 1:
                    if(!visitor.selectIndex(keys[1], full)) {
                        return;
                    }
                    break;

                default:
                    ASSERT(depth == 0 || depth == 1);
                    break;
            }

            for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
                (*i)->expand(request, visitor, depth+1, keys, full);
            }
        }
        return;
    }

    std::vector<Predicate*>::const_iterator next = cur;
    ++next;

    const std::string& keyword = (*cur)->keyword();

    StringList values;

    visitor.values(request, keyword, values);

    Key& k = keys[depth];

    for(StringList::const_iterator i = values.begin(); i != values.end(); ++i) {

        k.push(keyword, *i);
        full.push(keyword, *i);

        if((*cur)->match(k)) {
            expand(request, next, depth, keys, full, visitor);
        }

        full.pop(keyword);
        k.pop(keyword);

    }

}

void Rule::expand(const MarsRequest& request, ReadVisitor& visitor, size_t depth, std::vector<Key>& keys, Key& full) const
{
    ASSERT(keys.size() == 3);
    expand(request, predicates_.begin(), depth, keys, full, visitor);
}

void Rule::expand( const Key& field,
                   std::vector<Predicate*>::const_iterator cur,
                   size_t depth,
                   std::vector<Key>& keys,
                   std::vector<Key>& prev,
                   Key& full,
                   WriteVisitor& visitor) const {

    ASSERT(depth < 3);

    if(cur == predicates_.end()) {
        if(rules_.empty()) {
            ASSERT(depth == 2); /// we have 3 levels ATM
            if(!visitor.selectDatum( keys[2], full)) {
                return; // This it not useful
            }
        }
        else {

            switch(depth) {
                case 0:
                    if(keys[0] != prev[0]) {
                        if(!visitor.selectDatabase(keys[0], full)) {
                            return;
                        };
                        prev[0] = keys[0];
                    }
                    break;

                case 1:
                    if(keys[1] != prev[1]) {
                        if(!visitor.selectIndex(keys[1], full)) {
                            return;
                        }
                        prev[1] = keys[1];
                    }
                    break;

                default:
                    ASSERT(depth == 0 || depth == 1);
                    break;
            }

            for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
                (*i)->expand(field, visitor, depth+1, keys, prev, full);
            }
        }
        return;
    }

    std::vector<Predicate*>::const_iterator next = cur;
    ++next;

    const std::string& keyword = (*cur)->keyword();

    StringList values(1, field.get(keyword));

    // visitor.enterKeyword(request, keyword, values);

    Key& k = keys[depth];

    for(StringList::const_iterator i = values.begin(); i != values.end(); ++i) {

        k.push(keyword, *i);
        full.push(keyword, *i);

        if((*cur)->match(k)) {
            // visitor.enterValue(keyword, *i);
            expand(field, next, depth, keys, prev, full, visitor);
            // visitor.leaveValue();
        }

        full.pop(keyword);
        k.pop(keyword);

    }

    // visitor.leaveKeyword();
}
void Rule::expand(const Key& field, WriteVisitor& visitor, size_t depth, std::vector<Key>& keys, std::vector<Key>& prev, Key& full) const
{
    ASSERT(keys.size() == 3);
    ASSERT(prev.size() == 3);
    expand(field, predicates_.begin(), depth, keys, prev, full, visitor);
}

bool Rule::match(const Key& key) const
{
    for(std::vector<Predicate*>::const_iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        if(!(*i)->match(key)) {
            return false;
        }
    }
    return true;
}

void Rule::dump(std::ostream& s, size_t depth) const
{
    s << "[";
    const char* sep = "";
    for(std::vector<Predicate*>::const_iterator i = predicates_.begin(); i != predicates_.end(); ++i ) {
        s << sep;
        (*i)->dump(s);
        sep = ",";
    }

    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        (*i)->dump(s, depth+1);
    }
    s << "]";
}

size_t Rule::depth() const
{
    size_t result = 0;
    for(std::vector<Rule*>::const_iterator i = rules_.begin(); i != rules_.end(); ++i ) {
        result = std::max(result, (*i)->depth());
    }
    return result+1;
}

void Rule::print(std::ostream& out) const
{
    out << "Rule()";
}

std::ostream& operator<<(std::ostream& s, const Rule& x)
{
    x.print(s);
    return s;
}

//----------------------------------------------------------------------------------------------------------------------

} // namespace fdb5
