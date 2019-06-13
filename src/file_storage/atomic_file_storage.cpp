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

/** @file atomic_file_storage.cpp
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

static const char* EXAMPLE_DATABASE = "atomic_file_storage.ittiadb";

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
    // Create database with custom page size;
    file_storage_config.page_size = DB_DEF_PAGE_SIZE * 2;
    // Create a new database file
    rc = database.open(storage::ittiadb::Connection::CreateAlways, file_storage_config);
    if (DB_OK != rc)
    {
        std::cerr << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    //Create schema
    int64_t rows_affected;
    rows_affected = storage::ittiadb::Query(database, "CREATE TABLE tablename(" 
                                                      " ansi_field ansistr(10)," 
                                                      " int64_field integer,"
                                                      " float64_field float,"
                                                      " utf8_field utf8str(10),"
                                                      " CONSTRAINT pkname PRIMARY KEY(int64_field) )"
                                                      ).execute();
    // Some functions return a useful value or -1 for failure.
    if (rows_affected < 0) {
        std::cerr << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    // close database
    database.close();
    rc = database.open(storage::ittiadb::Connection::OpenExisting);
    if (DB_OK != rc)
    {
        std::cerr << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    
    //Start transaction
    storage::ittiadb::Transaction txn(database);
    storage::ittiadb::Table table(database, "tablename");
    table.open();
    rc = txn.begin();
    if (DB_OK != rc)
    {
        std::cerr << "Error beginning transaction: " 
            << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    
    //Add first row
    storage::data::SingleRow row(table.columns());
    row["ansi_field"].set("ansi_str1");
    row["int64_field"].set(1);
    row["float64_field"].set(1.231);
    row["utf8_field"].set("utf8-1");
    rc = table.insert(row);
    if (DB_OK != rc)
    {
        std::cerr << "Error inserting a row: [" << row<<"] " 
            << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    //Add second row
    row["ansi_field"].set("ansi_str2");
    row["int64_field"].set(2);
    row["float64_field"].set(2.231);
    row["utf8_field"].set("utf8-2");
    rc = table.insert(row);
    if (DB_OK != rc)
    {
        std::cerr << "Error inserting a row: [" << row << "] "
            << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    //Add third row
    row["ansi_field"].set("ansi_str3");
    row["int64_field"].set(3);
    row["float64_field"].set(3.231);
    row["utf8_field"].set("\0xD0\0xA3\0xD0\0xA2\0xD0\0xA4-8");
    rc = table.insert(row);
    if (DB_OK != rc)
    {
        std::cerr << "Error inserting a row: [" << row << "] "
            << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    // Commit after inserting 3 records; don't commit the last 2 rows.
    rc = txn.commit();
    rc = (DB_OK == rc)? txn.begin():rc;
    if (DB_OK != rc)
    {
        std::cerr << "Error commiting & begin transaction: " 
            << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    //Add fourth row
    row["ansi_field"].set("ansi_str4");
    row["int64_field"].set(4);
    row["float64_field"].set(4.231);
    row["utf8_field"].set("utf8-4");
    rc = table.insert(row);
    if (DB_OK != rc)
    {
        std::cerr << "Error inserting a row: [" << row << "] "
            << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    //Add fifth row
    row["ansi_field"].set("ansi_str5");
    row["int64_field"].set(5);
    row["float64_field"].set(5.231);
    row["utf8_field"].set("utf8");
    rc = table.insert(row);
    if (DB_OK != rc)
    {
        std::cerr << "Error inserting a row: [" << row << "] "
            << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    // Close database with last two rows left uncommitted.
    table.close(); 
    database.close();
    
    // Reopen DB again & check only commited records are inside
    rc = database.open(storage::ittiadb::Connection::OpenExisting);
    if (DB_OK != rc)
    {
        std::cerr << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    storage::ittiadb::Table table_readback(database, "tablename");
    table_readback.open("pkname");
    storage::data::RowSet<> result_set(table_readback.columns());
    rc = table_readback.fetch(result_set);
    if (DB_OK != rc)
    {
        std::cerr << "Table fetch failed "<< storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    if (result_set.size() != 3)
    {
        std::cerr << " Unexpected records count in table after reopen: "
            <<result_set.size()<<" but 3 expected. " << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return EXIT_SUCCESS;
}

