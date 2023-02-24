/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <memory>

#include "eckit/testing/Test.h"
#include "eckit/filesystem/URI.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/TmpFile.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/MemoryHandle.h"
#include "eckit/config/YAMLConfiguration.h"

#include "fdb5/daos/DaosSession.h"
#include "fdb5/daos/DaosPool.h"
#include "fdb5/daos/DaosContainer.h"
#include "fdb5/daos/DaosObject.h"
#include "fdb5/daos/DaosName.h"
#include "fdb5/daos/DaosArrayHandle.h"
#include "fdb5/daos/DaosException.h"

using namespace eckit::testing;
using namespace eckit;

namespace fdb {
namespace test {

// TODO check all calls to DaosOID and DaosObject and DaosHandle and objectCreate in unit tests
// TODO: assert in unit tests that an object created from a not-generated oid does not work
// TODO: unit tests for createArray from kv
// TODO: is the private ctor of DaosArray and DaosKeyValue kept private?

/// @todo: change all pre-condition checks to ASSERTs
//   question: in the future ASSERTs will default to EcKit abortion. Not what we want in many pre-condition checks

CASE( "DaosPool" ) {

//     /// @todo: currently, all pool and container connections are cached and kept open for the duration of the process. Would
//     // be nice to close container connections as they become unused. However the DaosContainer instances are managed by the 
//     // DaosPools/DaosSession, so we never know when the user has finished using a certain container. My current thought is
//     // we don't need to fix this, as each process will only use a single pool and 2 * (indices involved) containers.
//     // However in a large parallel application, while all client processes are running, there may be a large number of
//     // open containers in the DAOS system. One idea would be to use shared pointers to count number of uses.

//     /// @todo: given that pool_cache_ are owned by session, should more caches be implemented in FDB as in RadosStore?

//     /// @todo: A declarative approach would be better in my opinion.
//     // The current approach is an imperative one, where DaosObject and DaosContainer instances always represent existing entities in DAOS from the instant they are created.
//     // In highly parallel workflows, validity of such instances will be ephemeral, and by the time we perform an action on them, the DAOS entity they represent may
//     // no longer exist. In the declarative approach, the containers and objects would be opened right before the action and fail if they don't exist. In the imperative
//     // approach they would fail as well, but the initial checks performed to ensure existence of the DAOS entities would be useless and degrade performance.

//     /// @todo: issues in DaosContainer::create and destroy
    
//     /// @todo: issues in DaosPool::create and destroy




//     /// @todo: small TODOs in DaosHandle

//     /// @todo: solve question on default constructor of DaosOID

//     /// @todo: should daos_size_t be exposed as eckit::Length to user?

//     /// @todo: think about DaosName::dataHandle overwrite parameter

//     /// @todo: rule of three for classes with destructor?



//     /// @todo: properly implement DaosPool::exists(), DaosContainer::exists(), DaosObject::exists()

//     /// @todo: DaosHandle serialisation

//     /// @todo: implement missing methods in DaosName

//     /// @todo: cpp uuid wrapper, to avoid weird headers

//     /// @todo: use of uuid_generate_md5 can be removed completely

//     /// @todo: use of container and pool UUIDs can be removed completely

//     /// @todo: do not return iterators in e.g. DaosSession::getCachedPool. Return DaosPool&

//     /// @todo: replace deque by map

//     /// @todo: expose hi_ and lo_ in DaosOID



    // using hard-coded config defaults in DaosManager
    fdb5::DaosSession s{};

    SECTION("unnamed pool") {

        fdb5::DaosPool& pool = s.createPool();  // admin function, not usually called in the client code
        fdb5::AutoPoolDestroy destroyer(pool);

        std::cout << pool.name() << std::endl;

        EXPECT(pool.name().size() == 36);

        /// @todo: there's an attempt to close unopened pool here due to the pool destruction mechanism

    }

    SECTION("named pool") {

        fdb5::DaosPool& pool = s.createPool("pool");
        fdb5::AutoPoolDestroy destroyer(pool);

        std::cout << pool.name() << std::endl;

        EXPECT(pool.name() == "pool");

        fdb5::DaosPool& pool_h = s.getPool("pool");

        EXPECT(&pool == &pool_h);

    }

    SECTION("pool uuid actions") {

        fdb5::DaosPool& pool = s.createPool();
        fdb5::AutoPoolDestroy destroyer(pool);

        uuid_t pool_uuid = {0};
        pool.uuid(pool_uuid);

        char uuid_cstr[37];
        uuid_unparse(pool_uuid, uuid_cstr);
        std::cout << uuid_cstr << std::endl;

    }

    /// @todo: test passing some session ad-hoc config for DAOS client

    /// @todo: there's an extra pair of daos_init and daos_fini happening here

}

CASE( "DaosContainer, DaosArray and DaosKeyValue" ) {

    // again using hard-coded config defaults in DaosManager
    fdb5::DaosSession s{};

    fdb5::DaosPool& pool = s.createPool("pool");
    fdb5::AutoPoolDestroy destroyer(pool);

    fdb5::DaosContainer& cont = pool.createContainer("cont");

    SECTION("named container") {

        std::cout << cont.name() << std::endl;

        EXPECT(cont.name() == "cont");

        fdb5::DaosContainer& cont_h = pool.getContainer("cont");

        EXPECT(&cont == &cont_h);

        EXPECT_THROWS_AS(pool.getContainer("cont2"), fdb5::DaosEntityNotFoundException);

        std::vector<std::string> cont_list(pool.listContainers());
        EXPECT(cont_list.size() == 1);
        EXPECT(cont_list.front() == "cont");

        /// @todo: two attempts to close unopened containers here. This is due to mechanism triggered upon
        ///   pool destroy to ensure all matching container handles in the session cache are closed.
        ///   One way to address this would be to have a flag expectOpen = true in DaosContainer::close().
        ///   Whenever close() is called as part of pool destroy or batch container close, a false value 
        ///   could be passed to the close() method to avoid logging a warning message.

    }

    SECTION("unnamed object") {

        // create new object with new automatically allocated oid
        fdb5::DaosArray arr = cont.createArray();
        std::cout << "New automatically allocated Array OID: " << arr.name() << std::endl; 
        fdb5::DaosKeyValue kv = cont.createKeyValue();
        std::cout << "New automatically allocated KeyValue OID: " << kv.name() << std::endl; 

        /// @todo:
        // arr.destroy();
        // kv.destroy();

    }

    SECTION("named object") {

        // create new object with oid generated from user input
        uint32_t hi = 0x00000001;
        uint64_t lo = 0x0000000000000002;
        fdb5::DaosOID oid{hi, lo, DAOS_OT_ARRAY, OC_S1};
        fdb5::DaosArray write_obj = cont.createArray(oid);

        std::string id_string = write_obj.name();
        std::cout << "New user-spec-based OID: " << id_string << std::endl;
        EXPECT(id_string.length() == 32);
        /// @todo: do these checks numerically. Also test invalid characters, etc.
        std::string end{"000000010000000000000002"};
        EXPECT(0 == id_string.compare(id_string.length() - end.length(), end.length(), end));

        // represent existing object with known oid
        fdb5::DaosOID read_id{id_string};
        fdb5::DaosArray read_obj{cont, read_id};

        // attempt access non-existing object
        fdb5::DaosArrayOID oid_ne{0, 0, OC_S1};
        oid_ne.generate(cont);
        EXPECT_THROWS_AS(fdb5::DaosArray obj(cont, oid_ne), fdb5::DaosEntityNotFoundException);

        // attempt access object via (user-defined) non-generated OID
        EXPECT_THROWS_AS(fdb5::DaosArray obj(cont, oid), eckit::AssertionFailed);

        /// @todo:
        //write_obj.destroy();

    }

    /// @todo: DaosArray write and read

    SECTION("DaosKeyValue put and get") {

        fdb5::DaosKeyValue kv = cont.createKeyValue();

        std::string test_key{"test_key"};

        char data[] = "test";
        kv.put(test_key, data, (long) sizeof(data));

        long size = kv.size(test_key);
        EXPECT(size == (long) sizeof(data));

        long res;
        char read_data[10] = "";
        res = kv.get(test_key, read_data, sizeof(read_data));
        EXPECT(res == size);
        EXPECT(std::memcmp(data, read_data, sizeof(data)) == 0);

        EXPECT(!kv.has("nonexisting"));
        EXPECT(kv.size("nonexisting") == 0);
        EXPECT_THROWS_AS(kv.get("nonexisting", nullptr, 0), fdb5::DaosEntityNotFoundException);

        // TODO
        //kv.destroy();

    }

//     SECTION("DAOS NAME") {

//         std::string test_oid_str{"00000000000000010000000000000002"};
//         fdb5::DaosOID test_oid{test_oid_str};

//         fdb5::DaosName n1("a", "b", test_oid);
//         EXPECT(n1.asString() == "a/b/" + test_oid_str);

//         fdb5::DaosName n2("a/b/" + test_oid_str);
//         EXPECT(n2.asString() == "a/b/" + test_oid_str);

//         eckit::URI u1("daos", "a/b/" + test_oid_str);
//         fdb5::DaosName n3(u1);
//         EXPECT(n3.asString() == "a/b/" + test_oid_str);
//         EXPECT(n3.URI() == u1);
        
//         uint32_t hi = 0x00000001;
//         uint64_t lo = 0x0000000000000002;
//         fdb5::DaosArray obj = cont.createArray(fdb5::DaosOID(hi, lo, DAOS_OT_ARRAY));
//         fdb5::DaosName name{obj};

//         std::string name_str = name.asString();
//         std::string start{"pool/cont/"};
//         std::string end{"000000010000000000000002"};
//         EXPECT(0 == name_str.compare(0, start.length(), start));
//         EXPECT(0 == name_str.compare(name_str.length() - end.length(), end.length(), end));

//         eckit::URI uri = name.URI();
//         EXPECT(uri.asString() == "daos://pool/cont/" + test_oid_str);
//         EXPECT(obj.URI() == uri);

//         /// @todo: test name.exists and others

//         /// @todo: serialise

//         /// @todo: deserialise
//         fdb5::DaosName deserialisedname(std::string("pool"), std::string("cont"), test_oid);
    
//         std::cout << "Object size is: " << deserialisedname.size() << std::endl;
//         /// @todo: daos_fini for the session for the name's owned object happens before daos_cont_close and daos_pool_disconnect

//         // TODO
//         //obj.destroy();

//     }

//     // TODO: make section names nicer
//     SECTION("DAOS HANDLE, WRITE, APPEND AND READ") {

//         uint32_t hi = 0x00000001;
//         uint64_t lo = 0x0000000000000002;
//         fdb5::DaosOID test_oid{hi, lo, DAOS_OT_ARRAY, OC_S1};
//         fdb5::DaosArray obj = cont.createArray(test_oid);
//         fdb5::DaosOID test_oid_gen = obj.OID();
//         std::string test_oid_gen_str{test_oid_gen.asString()};

//         /// @todo: isn't openForWrite / Append re-creating already existing objects? (they must exist if instantiated)

//         char data[] = "test";
//         long res;

//         TODO: change to name
//         fdb5::DaosHandle h(std::move(obj));
//         /// @todo: this triggers array create if needed (not here) but does not wipe existing array if any
//         h.openForWrite(Length(sizeof(data)));
//         {
//             eckit::AutoClose closer(h);
//             res = h.write(data, (long) sizeof(data));
//             EXPECT(res == (long) sizeof(data));
//             EXPECT(h.position() == Offset(sizeof(data)));
//         }

//         /// @todo: this triggers array create again...
//         h.openForAppend(Length(sizeof(data)));
//         {
//             eckit::AutoClose closer(h);
//             res = h.write(data, (long) sizeof(data));
//             EXPECT(res == (long) sizeof(data));
//             EXPECT(h.position() == Offset(2 * sizeof(data)));
//         }

//         // TODO: get rid of all elsewhere. Make sure here that it doesn't throw
//         h.flush();

//         char read_data[10] = "";



//         eckit::URI u = "daos://pool/cont/" ....
//         fdb5::DaosName n{u};
//         eckti::DataHandle h = n.dataHandle();
//         ....







//         // TODO: don't use std::string if not needed. Use test_pool_1, test_cont_1, ...
//         fdb5::DaosName deserialisedname("pool", "cont", test_oid_gen);
//         fdb5::DaosArray read_array(s, deserialisedname);

//         // TODO: might be better to just have the following
//         fdb5::DaosHandle h3(DaosName("test_pool", "test_container", test_oid));
//         fdb5::DaosHandle h4 = deserialisedname.dataHandle();
//         // TODO: this has been removed now fdb5::DaosHandle h2(std::move(readobj));
//         fdb5::DaosHandle h5(readobj.name());
//         Length t = h2.openForRead();
//         EXPECT(t == Length(2 * sizeof(data)));
//         EXPECT(h2.position() == Offset(0));
//         {
//             eckit::AutoClose closer(h2);
//             for (int i = 0; i < 2; ++i) {
//                 res = h2.read(read_data + i * sizeof(data), (long) sizeof(data));
//                 EXPECT(res == (long) sizeof(data));
//             }
//             EXPECT(h2.position() == Offset(2 * sizeof(data)));
//         }

//         EXPECT(std::memcmp(data, read_data, sizeof(data)) == 0);
//         EXPECT(std::memcmp(data, read_data + sizeof(data), sizeof(data)) == 0);

//         char read_data2[10] = "";

//         std::unique_ptr<eckit::DataHandle> h3(deserialisedname.dataHandle());
//         h3->openForRead();
//         {
//             eckit::AutoClose closer(*h3);
//             for (int i = 0; i < 2; ++i) {
//                 h3->read(read_data2 + i * sizeof(data), (long) sizeof(data));
//             }
//         }

//         EXPECT(std::memcmp(data, read_data2, sizeof(data)) == 0);
//         EXPECT(std::memcmp(data, read_data2 + sizeof(data), sizeof(data)) == 0);

//         EXPECT_THROWS_AS(
//             fdb5::DaosHandle dh_fail(fdb5::DaosName(pool.name(), cont.name(), fdb5::DaosOID{1, 0})), 
//             fdb5::DaosEntityNotFoundException);

//         /// @todo: POOL, CONTAINER AND OBJECT OPENING ARE OPTIONAL FOR DaosHandle::openForRead. Test it
//         /// @todo: CONTAINER AND OBJECT CREATION ARE OPTIONAL FOR DaosHandle::openForWrite. Test it
//         /// @todo: test unopen/uncreated DaosObject::size()

//         // lost ownership of obj, recreate and destroy
//         // fdb5::DaosObject obj_rm{cont, test_oid};
//         // obj_rm.destroy();  // NOTIMP

//     }

//     // small documentation of classes and who owns what etc.

//     // have write and read workflows from point of view of FDB internals, where DAOS is not exposed. 

//     // manually destroying container for test purposes. There's no actual need for 
//     // container destruction or auto destroyer as pool is autodestroyed
//     /// @todo: when enabling this, AutoPoolDestroy fails as a corresponding DaosContainer instance 
//     //   still exists in the DaosPool, but the corresponding container does not exist and 
//     //   dummy_daos destroy fails trying to rename an unexisting directory. The issue of pool
//     //   and container destruction and invalidation needs to be addressed first.
//     //cont.destroy();

//     // test DaosPool::name(), uuid(), label()

//     // test pool destroy without close and ensure closed

//     // test pool create from uuid

//     // test pool declare form uuid

//     // test construct DaosContainer with uuid

//     // test DaosContainer::name()

//     // test DaosObject::name()

//     // test handle seek, skip, canSeek

//     // test handle title()

//     // test handle size and estimate

//     // test DaosName::size()

}

// /// @todo: test a new case where some DAOS operations are carried out with a DaosSession with specific config
// //  overriding (but not rewriting) DaosManager defaults

// CASE( "DaosName and DaosHandle workflows" ) {

//     SECTION("Array write to existing pool and container, with ungenerated OID") {

//         fdb5::DaosOID oid{0, 1, DAOS_OT_ARRY, OC_S1};
//         // fdb5::DaosOID oid{uint32_t{0}, uint64_t{1}, DAOS_OT_ARRY, OC_S1};
//         fdb5::DaosName n{"pool", "cont", oid};
//         std::unique_ptr<eckit::DataHandle> h(n.dataHandle());
//         EXPECT(dynamic_cast<fdb5::DaosArrayHandle*>(h->get())); // dereference rather than get, use reference
//         h->openForWrite();
//         {
//             eckit::AutoClose closer(h);
//             h->write(data, len);
//         }
//         EXPECT_THROWS_AS(oid.asString(), eckit::AssertionFailed); // ungenerated
//         EXPECT(n.OID().asString() == populated oid string);

//         // TODO: improve ambiguity in DaosOID constructors!!!
//         // fdb5::DaosOID oid{fdb5::DaosOID::arrayOID(uint32_t{0}, uint64_t{1}, OC_S1)};

//         fdb5::DaosArrayOID oid{0, 1, OC_S1};
//         // fdb5::DaosArrayOID oid{uint32_t{0}, uint64_t{1}, OC_S1};
//         // fdb5::DaosArrayOID oid{uint32_t{0}, uint64_t{1}};
//         // fdb5::DaosArrayOID oid{0, 1}; // NOT EQUIVALENT!
//         fdb5::DaosArrayName n{"pool", "cont", oid};
//         fdb5::DaosArrayName n{"pool", "cont", {0, 1, OC_S1}};
//         eckit::DataHandle h = n.dataHandle();
//         h.openForWrite();
//         {
//             eckit::AutoClose closer(h);
//             h.write(x, y);
//         }
//         EXPECT_THROWS(oid.asString()); // ungenerated
//         EXPECT(n.OID().asString() == populated oid);

//     }

//     SECTION("Array write to existing pool and container, with automatically generated OID") {

//         fdb5::DaosName n{"pool", "cont"};
//         fdb5::DaosName n2 = n.createArrayName(); // TODO: separate DaosContainerName, DaosArrayName, ...
//         std::unique_ptr<eckit::DataHandle> h(n2.dataHandle());
//         h.openForWrite();
//         {
//             eckit::AutoClose closer(h);
//             h.write(data, len);
//         }
//         EXPECT(n2.OID().asString() == populated oid);     

//     }

//     SECTION("Array write to existing pool and container, with URI") {

//         eckit::URI container{"daos://pool/container"};

//         fdb5::DaosContainerName n{container};
//         fdb5::DaosArrayName n2 = n.createArrayName(); // TODO: separate DaosContainerName, DaosArrayName, ...
//         std::unique_ptr<eckit::DataHandle> h(n2.dataHandle());
//         h.openForWrite();
//         {
//             eckit::AutoClose closer(h);
//             h.write(data, len);
//         }
//         EXPECT(n2.OID().asString() == populated oid);     

//     }

//     SECTION("Array write to existing pool and container, with generated OID") {

//         // TODO: generate these bits from DAOS macros
//         fdb5::DaosArrayOID oid{(uint64_t) 0x0011000000000000, (uint64_t) 1};
//         fdb5::DaosArrayOID oid{"00110000000000000000000000000001"};
//         fdb5::DaosArrayName n{"pool", "cont", oid};
//         fdb5::DaosArrayName n{"pool", "cont", {"00110000000000000000000000000001"}};
//         eckit::DataHandle h = n.dataHandle();
//         h.openForWrite(); // will fail if not exists, or instantiate it and open it otherwise. Or maybe should suppoort creating objcts with pre-generated oids?
//         {
//             eckit::AutoClose closer(h);
//             h.write(x, y);
//         }
//         EXPECT(n.OID().asString() == oid.asString());
//         // TODO: how would operations which require RPC look like before calling dataHandle?

//     }

//     SECTION("Array write to existing pool but non-existing container, with ungenerated OID") {

//         fdb5::DaosArrayOID oid{0, 1, OC_S1};
//         fdb5::DaosName n{"pool", "cont", oid};
//         std::unique_ptr<eckit::DataHandle> h(n.dataHandle());   // should check at least the pool exists before returning
//         h.openForWrite(); // will create cont and object if not exists, and open object
//         {
//             eckit::AutoClose closer(h);
//             h.write(x, y);
//         }
//         EXPECT_THROWS(oid.asString()); // ungenerated
//         EXPECT(n.OID().asString() == populated oid);
//         // TODO: how would operations which require RPC look like before calling dataHandle?

//     }

//     SECTION("Array write to existing pool but non-existing container, with automatically generated OID") {
        
//             n.createArray will create container

//     }

//     SECTION("Array write to existing pool but non-existing container, with generated OID") {

//         will fail as both cont and array are expected to exist. Or if support is added in DaosContainer::createArray to create pre-generated oid, then a container should be created here

//     }

//     SECTION("Array read from existing pool and container, with generated OID") {

//         // TODO: generate these bits from DAOS macros
//         fdb5::DaosArrayOID oid{(uint64_t) 0x0011000000000000, (uint64_t) 1};
//         fdb5::DaosArrayOID oid{"00110000000000000000000000000001"};
//         fdb5::DaosArrayName n{"pool", "cont", oid};
//         fdb5::DaosArrayName n{"pool", "cont", {"00110000000000000000000000000001"}};
//         eckit::DataHandle h = n.dataHandle();
//         h.openForRead(); // will fail if not exists, or instantiate it and open it otherwise
//         {
//             eckit::AutoClose closer(h);
//             h.read(data, h.size());
//         }
//         EXPECT(n.OID().asString() == oid.asString());
//         // TODO: how would operations which require RPC look like before calling dataHandle?

//     }

//     SECTION("Array read from existing pool and container, with generated OID, reading a range") {

//         // TODO: generate these bits from DAOS macros
//         fdb5::DaosArrayOID oid{(uint64_t) 0x0011000000000000, (uint64_t) 1};
//         fdb5::DaosArrayOID oid{"00110000000000000000000000000001"};
//         fdb5::DaosArrayName n{"pool", "cont", oid};
//         fdb5::DaosArrayName n{"pool", "cont", {"00110000000000000000000000000001"}};
//         eckit::DataHandle h = n.dataHandle();
//         h.openForRead(); // will fail if not exists, or instantiate it and open it otherwise
//         {
//             eckit::AutoClose closer(h);
//             EXPECT(h.size() >= 10);
//             h.seek(10);
//             h.read(data, h.size() - 10);
//         }
//         EXPECT(n.OID().asString() == oid.asString());
//         // TODO: how would operations which require RPC look like before calling dataHandle?

//     }

//     SECTION("Array read from existing pool and container, with ungenerated OID") {

//         fdb5::DaosOID oid{(uint32_t) 0, (uint64_t) 1, DAOS_OT_ARRY, OC_S1};
//         fdb5::DaosName n{"pool", "cont", oid};
//         eckit::DataHandle h = n.dataHandle(); // will copy name into handle
//         h.openForRead(); // will call name.OID() which generates oid within DaosName, but not the one in this scope (what if I made it generate the one in this scope?)
//         {
//             eckit::AutoClose closer(h);
//             h.read(x, y);
//         }
//         EXPECT_THROWS(oid.asString()); // ungenerated
//         EXPECT(n.OID().asString() == populated oid);
//         // TODO: how would operations which require RPC look like before calling dataHandle?

//         fdb5::DaosArrayOID oid{(uint32_t) 0, (uint64_t) 1, OC_S1};
//         fdb5::DaosArrayOID oid{(uint32_t) 0, (uint64_t) 1}; // TODO: how to discern from generated OID constructor, only via type of first arg? can be problematic
//         fdb5::DaosArrayName n{"pool", "cont", oid};
//         fdb5::DaosArrayName n{"pool", "cont", {0, 1, OC_S1}};
//         eckit::DataHandle h = n.dataHandle(); // will call name.OID() which generates oid within DaosName, but not the one in this scope
//         h.openForRead(); // will create array via container, instantiate it and open it
//         {
//             eckit::AutoClose closer(h);
//             h.read(x, y);
//         }
//         EXPECT_THROWS(oid.asString()); // ungenerated
//         EXPECT(n.OID().asString() == populated oid);
//         // TODO: how would operations which require RPC look like before calling dataHandle?

//     }

//     SECTION("Array read from existing pool and non-existing container") {
    
//         fail at container open in h.openForRead()

//     }

//     SECTION("KeyValue write to ... ") {

//     }

//     SECTION("KeyValue read from ... ") {

//     }

// }

}  // namespace test
}  // namespace fdb

int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}