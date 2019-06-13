/**************************************************************************/
/*                                                                        */
/*      Copyright (c) 2005-2016 by ITTIA L.L.C. All rights reserved.      */
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

/** @file bulk_import.cpp
 *
 * Command-line example program demonstrating the ITTIA DB Storage API.
 */

#include <stdlib.h>

#include "storage/data/environment.h"
#include "storage/ittiadb/transaction.h"
#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/table.h"
#include "storage/ittiadb/query.h"
#include "storage/data/single_row.h"
#include "db_iostream.h"

static const char* EXAMPLE_DATABASE = "bulk_import.ittiadb";

static int
create_database_schema(storage::ittiadb::Connection & db,
    storage::ittiadb::Connection::FileStorageConfig& config)
{
    int rc = EXIT_SUCCESS;
    // Create a new database file
    db_result_t db_rc = db.open(storage::ittiadb::Connection::CreateAlways, config);
    rc = (DB_OK == db_rc) ? EXIT_SUCCESS: EXIT_FAILURE;

    if (EXIT_FAILURE != rc)
    {
        //Create schema
        int64_t rows_affected;
        rows_affected = storage::ittiadb::Query(db, "CREATE TABLE tablename(" 
                                                      " ansi_field ansistr(10)," 
                                                      " int64_field integer,"
                                                      " float64_field float,"
                                                      " utf8_field utf8str(10),"
                                                      " CONSTRAINT pkname PRIMARY KEY(int64_field) )"
                                                      ).execute();
        // Some functions return a useful value or -1 for failure.
        if (rows_affected < 0) {
            db.close();
            rc = EXIT_FAILURE;
        }
    }
    return rc;
}

static db_result_t
import_data(storage::ittiadb::Table& table)
{
    db_result_t db_rc = DB_OK;
    storage::data::SingleRow row(table.columns());
    row["ansi_field"].set("ansi_str1");
    row["int64_field"].set(1);
    row["float64_field"].set(1.231);
    row["utf8_field"].set("utf8-1");
    //insert 100 rows
    for (int i = 0; i < 100 && DB_OK == db_rc; ++i) {
        row["int64_field"].set(i);
        row["float64_field"].set(i * i / 1000.0);
        db_rc = table.insert(row);
    }
    return db_rc;
}

int
example_main(int argc, char *argv[])
{
    db_result_t rc;

    const char * database_uri = EXAMPLE_DATABASE;
    if (argc > 1) {
        database_uri = argv[1];
    }

    storage::ittiadb::Connection database(database_uri);

    // Init storage configuration with default settings
    storage::ittiadb::Connection::FileStorageConfig file_storage_config;
    file_storage_config.file_mode &= ~DB_NOLOGGING;
    // Create database & schema with logging disabled
    if (EXIT_SUCCESS != create_database_schema(database, file_storage_config))
    {
        std::cerr << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    //Import bulk data
    storage::ittiadb::Transaction txn(database);
    storage::ittiadb::Table table(database, "tablename");
    rc = table.open();
    rc = (DB_OK == rc) ? txn.begin():rc;
    rc = (DB_OK == rc) ? import_data(table):rc;
    rc = (DB_OK == rc) ? txn.commit() : rc;

    if (DB_OK != rc)
    {
        std::cerr << "Error importing data in bulk : "
            << storage::data::Environment::error() << std::endl;
        // Close database
        database.close();

        // Recreate database with Logging enabled
        storage::ittiadb::Connection::FileStorageConfig file_storage_config;
        
        // Create database & schema with logging disabled
        if (EXIT_SUCCESS != create_database_schema(database, file_storage_config))
        {
            std::cerr << storage::data::Environment::error() << std::endl;
            return EXIT_FAILURE;
        }
        // Bulk import again
        rc = import_data(table);
        rc = (DB_OK == rc) ? txn.commit() : rc;
        rc = (DB_OK == rc) ? table.close() : rc;
        if (DB_OK == rc)
        {
            std::cerr << "Error in importing data: " << storage::data::Environment::error() << std::endl;
            return EXIT_FAILURE;
        }
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return EXIT_SUCCESS;
}
