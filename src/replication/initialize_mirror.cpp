/**************************************************************************/
/*                                                                        */
/*      Copyright (c) 2005-2017 by ITTIA L.L.C. All rights reserved.      */
/*                                                                        */
/*  This software is copyrighted by and is the sole property of ITTIA     */
/*  L.L.C.  All rights, title, ownership, or other interests in the       */
/*  software remain the property of ITTIA L.L.C.  This software may only  */
/*  be used in accordance with the corresponding license agreement.  Any  */
/*  unauthorized use, duplication, transmission, distribution, or         */
/*  disclosure of this software is expressly forbidden.                   */
/*                                                                        */
/*  This Copyright notice may not be removed or modified without prior    */
/*  written consent of ITTIA L.L.C.                                       */
/*                                                                        */
/*  ITTIA L.L.C. reserves the right to modify this software without       */
/*  notice.                                                               */
/*                                                                        */
/*  info@ittia.com                                                        */
/*  http://www.ittia.com                                                  */
/*                                                                        */
/*                                                                        */
/**************************************************************************/

#include <stdlib.h>

#include "storage/data/environment.h"
#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/transaction.h"
#include "storage/ittiadb/replication_catalog.h"
#include "storage/ittiadb/replication_peer.h"
#include "storage/ittiadb/table.h"
#include "storage/ittiadb/query.h"
#include "storage/data/single_row.h"
#include "storage/data/single_field.h"
#include "storage/data/row_set.h"
#include "storage/types/character_varying.h"
#include "db_iostream.h"

using ::storage::data::Environment;
using ::storage::ittiadb::Connection;
using ::storage::ittiadb::Transaction;
using ::storage::ittiadb::ReplicationCatalog;
using ::storage::ittiadb::ReplicationPeer;
using ::storage::ittiadb::Query;
using ::storage::data::RowSet;
using ::storage::types::CharacterVarying;

static const char * MAIN_FILE_NAME = "initialize_mirror-main.ittiadb";
static const char * MIRROR_FILE_NAME = "initialize_mirror-mirror.ittiadb";

static const db_rep_address_t MAIN_REP_ADDRESS = 1;
static const db_rep_address_t MIRROR_REP_ADDRESS = 2;

static void
initialize_database(Connection& database, db_rep_address_t rep_address)
{
    (void)Query(database,
        "create table t1 ("
        "  id integer primary key,"
        "  data integer"
        ")"
        ).execute();
    (void)Query(database,
        "create table t2 ("
        "  id integer primary key,"
        "  data integer"
        ")"
        ).execute();

    ReplicationCatalog rep_catalog(database);
    (void)rep_catalog.set_config(rep_address);

    if (rep_address == MAIN_REP_ADDRESS) {
        ReplicationCatalog::TableInfo main_info(DB_REP_MODE_OUT);
        (void)rep_catalog.table_set("t1", main_info);
        (void)rep_catalog.table_set("t2", main_info);
    }
    else {
        ReplicationCatalog::TableInfo mirror_info(
                    DB_REP_MODE_IN,
                    DB_REP_RESOLVE_ACCEPT);
        (void)rep_catalog.table_set("t1", mirror_info);
        (void)rep_catalog.table_set("t2", mirror_info);
    }
}

static void
populate_database(Connection& database)
{
    Transaction txn(database);

    (void)txn.begin();
    (void)Query(database,
        "insert into t1 (id)"
        "  select n from $nat(10)"
        ).execute();
    (void)Query(database,
        "insert into t2 (id)"
        "  select n from $nat(10)"
        ).execute();
    (void)txn.commit();
}

static void
modify_database(Connection& database)
{
    Transaction txn(database);

    (void)txn.begin();
    (void)Query(database,
        "update t1 set data = 1"
        "  where id between 4 and 8"
        ).execute();
    (void)Query(database,
        "delete from t2"
        "  where id in (2, 4, 6, 8)"
        ).execute();
    (void)txn.commit();
}

static void
print_database(const char * dbname, Connection& database)
{
    RowSet<> result_t1;
    RowSet<> result_t2;
    Transaction txn(database);
    (void)Query(database, "select * from t1").execute(result_t1);
    (void)Query(database, "select * from t2").execute(result_t2);
    (void)txn.commit();
    std::cout << "# " << dbname << std::endl;
    std::cout << "## t1" << std::endl;
    std::cout << result_t1.columns() << std::endl;
    std::cout << result_t1 << std::endl;
    std::cout << "## t2" << std::endl;
    std::cout << result_t2.columns() << std::endl;
    std::cout << result_t2 << std::endl;
}

int
example_main(int argc, char *argv[])
{
    const char * main_file_name = MAIN_FILE_NAME;
    const char * mirror_file_name = MIRROR_FILE_NAME;
    const char * peer_name = "main";

    if (argc > 1) {
        main_file_name = argv[1];
    }
    if (argc > 2) {
        mirror_file_name = argv[2];
    }

    // Prepare a main database
    Connection main_database(main_file_name);
    if (DB_OK != main_database.open(Connection::CreateAlways))
    {
        std::cerr << "Unable to create database" << main_file_name << ": " << Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    // Initialize main database and populate with sample data
    initialize_database(main_database, MAIN_REP_ADDRESS);
    populate_database(main_database);

    // Set up mirror database
    Connection mirror_database(mirror_file_name);
    if (DB_OK != mirror_database.open(Connection::CreateAlways))
    {
        std::cerr << "Unable to create database" << mirror_file_name << ": " << Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    // Initialize mirror schema, but do not populate with any data
    initialize_database(mirror_database, MIRROR_REP_ADDRESS);

    // Connect mirror to main database
    ReplicationCatalog rep_catalog(mirror_database);
    ReplicationCatalog::PeerDefinition peer_definition(
                DB_REPTYPE_ADHOC,
                MAIN_REP_ADDRESS,
                main_file_name);
    if (DB_OK != rep_catalog.create_peer(peer_name, peer_definition))
    {
        std::cerr << "Unable to create peer: " << Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    ReplicationPeer peer(mirror_database, peer_name);
    if (DB_OK != peer.exchange())
    {
        std::cerr << "Unable to connect to peer: " << Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    // Select a list of tables that were configured to replicate
    RowSet<> table_list;
    (void)Query(main_database,
          "select table_name from tables join rep_tables using (table_id)"
          ).execute(table_list);

    // Transfer data to the mirror database
    for (RowSet<>::const_iterator iter = table_list.begin(); iter != table_list.end(); ++iter) {
        CharacterVarying<DB_MAX_TABLE_NAME> table_name;
        iter->at(0).get(table_name);
        if (DB_OK != peer.snapshot_in(*table_name)) {
            std::cerr << "Error copying table " << *table_name << " from peer: " << Environment::error().description << std::endl;
        }
    }

    // Modify the main database after the mirror was created
    modify_database(main_database);

    while (true) {
        if (DB_OK != peer.exchange())
        {
            std::cerr << "Unable to connect to peer: " << Environment::error() << std::endl;
            return EXIT_FAILURE;
        }
        else {
            std::cout << "Replication exchange complete" << std::endl;
        }

        // Show contents of each database after replication exchange
        print_database("main database", main_database);
        print_database("mirror database", mirror_database);

        // Prompt for changes to the main database
        std::cout << "Enter SQL statements or an empty line to replicate changes to the mirror" << std::endl;
        sql_line_shell(main_database);
    }

    return EXIT_SUCCESS;
}
