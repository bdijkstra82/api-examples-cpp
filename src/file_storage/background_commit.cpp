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

/** @file background_commit.c
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

static const char* EXAMPLE_DATABASE = "background_commit.ittidb";

typedef struct {
    int lazy_tx;
    int forced_tx;
} trans_stat_t;

int
example_main(int argc, char *argv[])
{
    int rc = EXIT_FAILURE;
    db_result_t db_rc;

    const char * database_uri = EXAMPLE_DATABASE;
    if (argc > 1) {
        database_uri = argv[1];
    }

    storage::ittiadb::Connection database(database_uri);
    
    // Create a new database file
    db_rc = database.open(storage::ittiadb::Connection::CreateAlways);
    if (DB_OK != db_rc)
    {
        std::cerr << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    //Create database schema
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
        std::cerr << "Database schema creation failed: " 
            << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }

    // Start transactions generation. Part of transactions commit with 
    // DB_LAZY_COMPLETION flag to make them to be deferred
    storage::ittiadb::Transaction txn(database);
    storage::ittiadb::Table table(database, "tablename");
    db_rc = table.open(storage::ittiadb::Table::Exclusive);
    storage::data::SingleRow row(table.columns());
    row["ansi_field"].set("ansi_str1");
    row["utf8_field"].set("utf8-1");
    trans_stat_t stat = {0,0};
    //insert 100 rows
    for (int i = 0; i < 100 && DB_OK == db_rc; ++i) {
        db_rc = txn.begin();
        int do_lazy_commit = i % 5;
        
        row["int64_field"].set(i+1);
        row["float64_field"].set(do_lazy_commit? 50.0:16.0);
        
        db_rc = (DB_OK == db_rc) ? table.insert(row) : db_rc;
        if (DB_OK == db_rc)
        {
            if (do_lazy_commit) {
                stat.lazy_tx++;
                db_rc = txn.commit(storage::ittiadb::Transaction::LazyCompletion);
            }
            else {
                stat.forced_tx++;
                db_rc = txn.commit(storage::ittiadb::Transaction::ForcedCompletion);
            }
        }

        if (i % 30) {
            //what storage API?
            db_rc = (DB_OK == db_rc) ? 
                db_flush_tx(database.handle(), DB_FLUSH_JOURNAL):db_rc;
        }
    }
    if (DB_OK != db_rc) {
        std::cerr << " Error inserting or commiting " 
            << storage::data::Environment::error()<<std::endl;
        return EXIT_FAILURE;
    }
    table.close();
    // Close database
    database.close();

    // Reopen DB and check if all transactions are really there
    db_rc = database.open(storage::ittiadb::Connection::OpenExisting);
    if (DB_OK != db_rc)
    {
        std::cerr << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    storage::ittiadb::Table table_readback(database, "tablename");
    table_readback.open();
    storage::data::RowSet<> result_set(table_readback.columns());
    db_rc = table_readback.fetch(result_set);
    if (DB_OK != db_rc)
    {
        std::cerr << storage::data::Environment::error() << std::endl;
        return EXIT_FAILURE;
    }
    table_readback.close();

    trans_stat_t readback_stat = { 0, 0 };
    int counter = 0;
    int f1 =0;
    float f2 =0.0f;
    for (storage::data::RowSet<>::const_iterator iter = result_set.begin(); 
        iter != result_set.end(); ++iter)
    {   
        (*iter).at("int64_field").get(f1);
        (*iter).at("float64_field").get(f2);
        if (f1 != ++counter)
        {
            break;
        }
        readback_stat.lazy_tx +=  f2 == 50 ? 1 : 0;
        readback_stat.forced_tx += f2 == 16 ? 1 : 0;
    }

    if (DB_OK != db_rc) {
        std::cerr <<"Error to scan table of backup db "
                  << storage::data::Environment::error()<<std::endl;
    }
    else if (f1 != counter) {
        std::cerr << "Pkey field values sequence violation detected in backup db: (" 
                  << f1 << "," << counter << ")" << std::endl;         
    }
    else if (readback_stat.lazy_tx != stat.lazy_tx) {
        std::cerr << "Unexpected count of records which was commited in lazy-commit mode: " 
            << readback_stat.lazy_tx << ", but expected" << stat.lazy_tx<<std::endl;
    }
    else if (readback_stat.forced_tx != stat.forced_tx) {
        std::cerr << "Unexpected count of records which was commited in force-commit mode: "
            << readback_stat.forced_tx << ", but expected" << stat.forced_tx << std::endl;
    }
    else {
        std::cout<< counter << " records is inside " << std::endl;
        rc = EXIT_SUCCESS;
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return rc;
}
