/**************************************************************************/
/*                                                                        */
/*      Copyright (c) 2005-2019 by ITTIA L.L.C. All rights reserved.      */
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
#include <stdio.h>

#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/transaction.h"
#include "storage/ittiadb/replication_catalog.h"
#include "storage/ittiadb/replication_peer.h" 
#include "storage/ittiadb/query.h"
#include "storage/data/single_row.h"
#include "storage/data/single_field.h" 
#include "storage/data/environment.h" 
#include "db_iostream.h"
#include "ittia/os/os_file.h"
#include "example_thread.h"


using ::storage::data::Environment;
using ::storage::ittiadb::Connection;
using ::storage::ittiadb::Transaction;
using ::storage::ittiadb::ReplicationCatalog;
using ::storage::ittiadb::ReplicationPeer;
using ::storage::ittiadb::Query;
using ::storage::data::RowSet;
using ::storage::data::SingleRow;


// Database replication settings for incremental backup
static const char * MAIN_FILE_NAME = "main.ittiadb";
static const char * BACKUP_FILE_NAME = "incremental_backup.ittiadb";
static const char * PEER_NAME = "main";
static const db_rep_address_t MAIN_REP_ADDRESS = 1;
static const db_rep_address_t BACKUP_REP_ADDRESS = 2;

/// Configuration for backup example, shared between threads.
struct example_backup_config_t {
    /// Set to `true` to enable incremental backup.
    bool  is_backup_enabled;
    /// Name of main database file.
    const char * database_name;
    /// Name of backup database file.
    const char * incremental_backup_name;

    /// Next ID value for new database records.
    int insert_transaction_counter = 7;
};

// Example application functions
static void initialize_database(Connection& database);
static void main_database_insertion_task(void *argument);        //:params example_backup_config_t

// Incremental backup functions
static void full_database_backup_task(void * argument);          //:params example_backup_config_t
static void incremental_backup_setup(Connection & main_database, Connection & backup_database);
static void incremental_backup_task(void * argument);            //:params example_backup_config_t
static void recover_from_backup(example_backup_config_t * td);


void
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

    // Set OUT replication mode for all tables to include them in
    // incremental backups.
    ReplicationCatalog::TableInfo main_table_info(DB_REP_MODE_OUT);
    ReplicationCatalog main_database_catalog(database);
    (void)main_database_catalog.table_set("t1", main_table_info);
    (void)main_database_catalog.table_set("t2", main_table_info);
}

void
main_database_insertion_task(void *argument)
{
    example_backup_config_t * bc = static_cast<example_backup_config_t *> (argument);

    Connection main_database(bc->database_name);
    if(DB_OK != main_database.open(Connection::OpenExisting)) {
        std::cout << "Could not open " << main_database.uri() << ": " << Environment::error() << std::endl;
        exit(1);
    }

    // Prepare insert queries. Note: the catalog for t1 and t2 is locked until
    // these query objects are closed or destroyed.
    Query insert_query_t1(
        main_database,
        "INSERT INTO t1 (id,data)"
        "  values(?,?)"
        ), insert_query_t2(
        main_database,
        "INSERT INTO t2 (id,data)"
        "  values(?,?)"
        );
    if (DB_OK != insert_query_t1.prepare() || DB_OK != insert_query_t2.prepare()) {
        std::cout << "Failed preparing parametrized query" << ": " << Environment::error() << std::endl;
        exit(1);
    }
    // Both queries share the same parameter types and values
    SingleRow params(insert_query_t1.parameters());

    Transaction txn(main_database);
    while (bc->is_backup_enabled) {
        // Sleep when transaction is not active
        example_thread::sleep_for_seconds(1);

        txn.begin(Transaction::Shared);
        //Insert 3 values each time to check correctness of a back up.
        for(int i = 0; i < 3; i++) {
            params[0].set(bc->insert_transaction_counter * 3 + i);
            params[1].set(bc->insert_transaction_counter);
            if (0 > insert_query_t1.execute_with(params)||
                0 > insert_query_t2.execute_with(params))
            {
                std::cout << "Insert failed: " << Environment::error() << std::endl;
                continue;
            }
        }
        ++bc->insert_transaction_counter;
        txn.commit();
    }
}

void
full_database_backup_task(void *argument)
{
    std::cout << "Database backup started" << std::endl;
    example_backup_config_t *td = static_cast<example_backup_config_t *> (argument);

    Connection main_database(td->database_name);
    if (DB_OK != main_database.open(Connection::OpenExisting))
    {
        std::cout << "Unable to open main database, " << main_database.uri()<< ": " << Environment::error() << std::endl;
        exit(1);
    }

    // Copy main database to the backup database file
    if (DB_OK != main_database.backup( td->incremental_backup_name, 0))
    {
        std::cout << "Unable to create a backup of the main database, " << main_database.uri()<< ": " << Environment::error() << std::endl;
        exit(1);
    }

    // Configure incremental backup from the main database to the backup database
    Connection backup_database(td->incremental_backup_name);
    if (DB_OK != backup_database.open(Connection::OpenExisting))
    {
        std::cout << "Unable to open backup database for incremental backup setup. " << backup_database.uri() << ": " << Environment::error() << std::endl;
        exit(1);
    }
    std::cout << "Full database backup is complete!!!" << std::endl;

    incremental_backup_setup(main_database, backup_database);
    std::cout << "Database is configured for incremental backup" << std::endl;
}

// Configure incremental backup using ITTIA DB SQL replication
void
incremental_backup_setup(Connection& main_database, Connection& backup_database)
{
    ReplicationCatalog main_database_catalog(main_database);
    ReplicationCatalog backup_catalog(backup_database);

    // Assign each database file a unique replication address to enable replication.
    (void)main_database_catalog.set_config(MAIN_REP_ADDRESS);
    (void)backup_catalog.set_config(BACKUP_REP_ADDRESS);

    // Set IN replication mode for all tables in the backup database.
    ReplicationCatalog::TableInfo backup_table_info(DB_REP_MODE_IN);
    (void)backup_catalog.table_set("t1", backup_table_info);
    (void)backup_catalog.table_set("t2", backup_table_info);

    // Note: assume the main database is already configured to use OUT
    // replication mode. Do not attempt to set replication mode here, since
    // the main database catalog may be locked by another thread.

    // Add an ad hoc replication peer to the backup database referencing the main database.
    ReplicationCatalog::PeerDefinition backup_definition(
        DB_REPTYPE_ADHOC,
        MAIN_REP_ADDRESS,
        main_database.uri());
    if (DB_OK != backup_catalog.create_peer( PEER_NAME, backup_definition))
    {
        std::cout << "Unable to create incremental backup: " << Environment::error() << std::endl;
        exit(1);
    }
}

// Perform incremental backup in a continuous loop.
// Use ITTIA DB SQL ad hoc database replication. Note: with this approach, the backup is run every
// 100 milliseconds, so some data loss may occur if the main database is damaged or destroyed.
// Use synchronous replication instead if no data loss is acceptable.
//
// Database back up is intentionally not synchronized with database updates to demonstrate the
// possibility of losing some data; this thread finishes before main thread stops updating.
void
incremental_backup_task(void *argument)
{
    example_backup_config_t * td = static_cast<example_backup_config_t *> (argument);
    std::cout<<"Incremental backup task started"<<std::endl;
    Connection backup_database(td->incremental_backup_name);
    if(DB_OK != backup_database.open(Connection::OpenExisting)) {
        std::cout << "Could not open " << backup_database.uri() << ": " << Environment::error() << std::endl;
        return;
    }

    ReplicationPeer backup_peer(backup_database, PEER_NAME);
    while(td->is_backup_enabled)
    {
        example_thread::sleep_for_milliseconds(100);
        if (DB_OK != backup_peer.exchange())
        {
            std::cout << "Unable to connect and update backup: " << Environment::error() << std::endl;
        }
    }
}

// Recover main database from backup.
//
// Note: the recovery procedure may need to be customized for each application.
void
recover_from_backup(example_backup_config_t * td)
{
    std::cout<<"Restoring from back up."<<std::endl;
    Connection backup_database(td->incremental_backup_name);

    if (DB_OK != backup_database.open(Connection::OpenExisting))
    {
        std::cout << "Unable to open backup database" << backup_database.uri() << ": " << Environment::error() << std::endl;
        exit(1);
    }

    // A clean version of the backup database is needed.
    // Remove peer referencing the original main database, which no longer exists.
    ReplicationCatalog backup_database_catalog(backup_database);
    if (DB_OK != backup_database_catalog.drop_peer(PEER_NAME)) {
        std::cout << "Failed to reset outdated backup settings" << ": " << Environment::error() << std::endl;
        exit(1);
    }

    if (backup_database.backup(td->database_name, 0) != DB_OK) {
        std::cout << "Failed to restore database from backup," << backup_database.uri() << ": " << Environment::error() << std::endl;
        exit(1);
    }

    // The newly created main database must exist after backup function call.
    Connection main_recovered(td->database_name);
    main_recovered.open(Connection::OpenExisting);
    std::cout<<"Main database recovered from the most recent incremental backup."<<std::endl;

    // Set new configurations for incremental backup.
    incremental_backup_setup(main_recovered, backup_database);

    // Set main database to update incremental backup.
    ReplicationCatalog::TableInfo main_table_info(DB_REP_MODE_OUT);
    ReplicationCatalog main_database_catalog(main_recovered);
    (void)main_database_catalog.table_set("t1", main_table_info);
    (void)main_database_catalog.table_set("t2", main_table_info);
}


int
example_main(int argc, char *argv[])
{
    struct example_backup_config_t td;
    td.is_backup_enabled = true;
    td.database_name = MAIN_FILE_NAME;
    td.incremental_backup_name = BACKUP_FILE_NAME;
    if (argc > 1) {
        td.database_name = argv[1];
    }

    if (argc > 2) {
        td.incremental_backup_name = argv[2];
    }

    // Create a main database
    Connection main_database(td.database_name);
    if (DB_OK != main_database.open(Connection::CreateAlways))
    {
        std::cout << "Unable to create database" << td.database_name << ": " << Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    // Initialize main database file and schema.
    initialize_database(main_database);
    main_database.close();

    // Continuously insert into the main database in a background thread.
    example_thread main_database_insert_thread(main_database_insertion_task, &td);

    // Perform a full backup and configure incremental backup in a background thread.
    example_thread backup_thread(full_database_backup_task, &td);
    backup_thread.join();

    // Begin incremental backup in a background thread.
    example_thread incremental_backup_thread(incremental_backup_task, &td);

    // Continue adding new records with incremental backup for 4 seconds.
    example_thread::sleep_for_seconds(4);

    // Close all connections and wait for all background threads to finish.
    td.is_backup_enabled = false;
    incremental_backup_thread.join();
    main_database_insert_thread.join();

    // Simulate hardware failure by removing the main database.
    main_database.remove();

    // Recover the main database from the backup file.
    recover_from_backup(&td);
    std::cout << "Back to normal after failure." << std::endl;
    td.is_backup_enabled = true;

    // Start tasks to insert new data and resume incremental backup.
    example_thread main_database_recovered_insert_thread(main_database_insertion_task, &td);
    example_thread incremental_backup_recovered(incremental_backup_task, &td);

    // Continue adding new records with incremental backup for 5 seconds.
    example_thread::sleep_for_seconds(5);

    // Close all connections and wait for all background threads to finish.
    td.is_backup_enabled = false;
    main_database_recovered_insert_thread.join();
    incremental_backup_recovered.join();

    return 0;
}
