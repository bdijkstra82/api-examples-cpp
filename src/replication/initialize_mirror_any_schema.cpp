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

static void
initialize_database(Connection& database)
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
print_database(const char * dbname, Connection& connection)
{
    RowSet<> result_t1;
    RowSet<> result_t2;
    (void)Query(connection, "select * from t1").execute(result_t1);
    (void)Query(connection, "select * from t2").execute(result_t2);
    std::cout << "# " << dbname << std::endl;
    std::cout << "## t1" << std::endl << result_t1 << std::endl;
    std::cout << "## t2" << std::endl << result_t2 << std::endl;
}

void
sql_line_shell(Connection& database)
{
    // Execute a SQL query for each line of input.
    std::cout << "Enter SQL statements or an empty line to quit" << std::endl;

    while (std::cin.good()) {
        std::string statement;
        std::cout << database.uri() <<  "> " << std::flush;
        std::getline(std::cin, statement);

        // Continue until a blank line is entered
        if (statement.empty()) {
            break;
        }

        // Remove trailing semicolon, if present
        if (*(statement.end() - 1) == ';') {
            statement.resize(statement.length() - 1);
        }

        RowSet<> row_set;
        if (DB_OK == Query(database, statement.c_str()).execute(row_set)) {
            std::cout << row_set.columns() << std::endl;
            std::cout << row_set;
        }
        else {
            const ::storage::data::Error error = Environment::error();
            std::cerr << error.name << ": " << error.description << std::endl;
        }
    }
}

int
example_main(int argc, char *argv[])
{
    // Hard-coded database file name.
    const char * database_file_name = "initialize_mirror-main.ittiadb";
    const char * mirror_file_name = "initialize_mirror-mirror.ittiadb";
    const char * peer_name = "main";
    const db_rep_address_t database_peer_address = 1;
    const db_rep_address_t mirror_peer_address = 2;

    if (argc > 1) {
        database_file_name = argv[1];
    }

    // TODO: don't create the main database, especially if it is on the server

    // Prepare a main database

    Connection main_database(database_file_name);
    if (DB_OK != main_database.open(Connection::CreateAlways))
    {
        std::cerr << "Unable to create database" << database_file_name << ": " << Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    initialize_database(main_database);
    populate_database(main_database);

    ReplicationCatalog main_rep_catalog(main_database);
    (void)main_rep_catalog.set_config(database_peer_address);
    (void)main_rep_catalog.table_set("t1", ReplicationCatalog::TableInfo(DB_REP_MODE_OUT));
    (void)main_rep_catalog.table_set("t2", ReplicationCatalog::TableInfo(DB_REP_MODE_OUT));

    // Set up mirror database

    Connection mirror_database(mirror_file_name);
    if (DB_OK != mirror_database.open(Connection::CreateAlways))
    {
        std::cerr << "Unable to create database" << mirror_file_name << ": " << Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    ReplicationCatalog mirror_rep_catalog(mirror_database);
    (void)mirror_rep_catalog.set_config(mirror_peer_address);

    ReplicationCatalog::PeerDefinition peer_definition(
                DB_REPTYPE_ADHOC,
                database_peer_address,
                database_file_name);
    if (DB_OK != mirror_rep_catalog.create_peer(peer_name, peer_definition))
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

    {
        RowSet<> tables;
        (void)Query(main_database,
              "select table_name from tables join rep_tables using (table_id)"
              ).execute(tables);

        // Copy tables and replication settings

        for (RowSet<>::const_iterator iter = tables.begin(); iter != tables.end(); ++iter) {
            CharacterVarying<DB_MAX_TABLE_NAME> table_name;
            iter->at(0).get(table_name);

            ReplicationCatalog::TableInfo rep_info;
            (void)main_rep_catalog.table_query(*table_name, rep_info);

            if (rep_info.is_out())
            {
                db_tabledef_t table_def;
                (void)db_tabledef_init(&table_def, NULL);
                (void)db_describe_table(main_database.handle(), *table_name, &table_def, DB_DESCRIBE_TABLE_FIELDS | DB_DESCRIBE_TABLE_INDEXES);
                (void)db_create_table(mirror_database.handle(), table_def.table_name, &table_def, 0);
                // Copy indexes so that the mirror is always ready to take over
                // the duties of the main database.
                for (int i = 0; i < table_def.nindexes; ++i) {
                    db_indexdef_t &index_def = table_def.indexes[i];
                    (void)db_create_index(mirror_database.handle(), table_def.table_name, index_def.index_name, &index_def);
                }
                (void)db_tabledef_destroy(&table_def);

                // TODO: foreign keys? sequences? identity column sequences must be created first.

                // The mirror will replicate in and accept all conflicting rows
                ReplicationCatalog::TableInfo mirror_info(
                            DB_REP_MODE_IN,
                            DB_REP_RESOLVE_ACCEPT);
                (void)mirror_rep_catalog.table_set(*table_name, mirror_info);
            }
        }

        // Transfer data to the mirror

        for (RowSet<>::const_iterator iter = tables.begin(); iter != tables.end(); ++iter) {
            CharacterVarying<DB_MAX_TABLE_NAME> table_name;
            iter->at(0).get(table_name);
            if (DB_OK != peer.snapshot_in(*table_name)) {
                std::cerr << "Error copying table " << *table_name << " from peer: " << Environment::error().description << std::endl;
            }
        }
    }

    modify_database(main_database);

    if (DB_OK != peer.exchange())
    {
        std::cerr << "Unable to connect to peer: " << Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    print_database("origin database", main_database);
    print_database("mirror database", mirror_database);

    sql_line_shell(mirror_database);

    mirror_database.close();
    main_database.close();

    return EXIT_SUCCESS;
}
