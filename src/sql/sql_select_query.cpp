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

/// @file sql_select_query.cpp
///
/// Command-line example program demonstrating the ITTIA DB storage API.
///

#include <stdlib.h>
#include "storage/data/environment.h"
#include "storage/ittiadb/transaction.h"
#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/table.h"
#include "storage/ittiadb/query.h"
#include "storage/data/single_row.h"
#include "storage/data/single_field.h"
#include "db_iostream.h"

static const char* EXAMPLE_DATABASE = "sql_select_query.ittiadb";

/// Inserts data row to storage table 
static int
ins_data(storage::ittiadb::Connection& db)
{
    int rc = DB_NOERROR;

    storage::ittiadb::Query create_table(db, "create table storage ("
        "  id sint32 not null primary key,"
        "  data sint32"
        ")");

    storage::ittiadb::Query insert_to_table(db, "insert into storage (id, data)"
        "  select N, 100 - N from $nat(100)");
    // Prepare SQL Query
    if (DB_OK != create_table.prepare()
        && DB_OK != insert_to_table.prepare())
    {
        GET_ECODE(rc, DB_FAIL, "prepare SQL failed ");
    }

    // Execute Create storage table query
    if (DB_NOERROR == rc)
    {
        const int64_t rows_affected = create_table.execute();
        GET_ECODE(rc, rows_affected, "Failed to execute create_table ");
    }

    //Execute insert to created storage table
    if (DB_NOERROR == rc) {
        const int64_t rows_affected = insert_to_table.execute();
        GET_ECODE(rc, rows_affected, "Failed to insert into create_table ");
    }

    std::cout << "creating & inserting data into \"storage\" table" << std::endl;
    return rc;
}

static void
process_record( int32_t id, int32_t data )
{
    // Stub function: process a record from the database.
}

/// Process Queries to fetch all rows of table and first 30 rows of table
static int
select_query(storage::ittiadb::Connection& db)
{
    int rc = DB_NOERROR;
    
    storage::ittiadb::Query select_all_row_query(db, "select id, data from storage");
    if (DB_OK != select_all_row_query.prepare()) {
        GET_ECODE(rc, DB_FAIL, "Preparing select_all_row_query ");
    }

    storage::data::RowSet<> result_set(select_all_row_query.columns());
    if (DB_OK != select_all_row_query.execute(result_set)) {
        GET_ECODE(rc, DB_FAIL, "Executing select_all_row_query ");
    }

    storage::data::RowSet<>::const_iterator itr = result_set.begin();
    for (; itr != result_set.end() && (DB_NOERROR == rc); ++itr)
    {
        const int32_t id = itr->at(0).to<int32_t>();
        const int32_t data = itr->at(1).to<int32_t>();
        process_record(id, data);
    }
    std::cout << result_set.size() << " rows of \"storage\" table processed" << std::endl;

    if (DB_NOERROR == rc)
    {
        // Select only the first 30 rows in the table.
        storage::ittiadb::Query select_first_30_query(db,
            "select id, data"
            "  from storage"
            "  order by id"
            "  offset 0 rows"
            "  fetch first 30 rows only");

        if (DB_OK != select_first_30_query.prepare()) {
            GET_ECODE(rc, DB_FAIL, "Preparaing select_first_30_query ");
        }

        storage::data::RowSet<> result_set(select_all_row_query.columns());
        if (DB_OK != select_first_30_query.execute(result_set)) {
            GET_ECODE(rc, DB_FAIL, "Executing select_first_30_query ");
        }

        storage::data::RowSet<>::const_iterator itr = result_set.begin();
        for (; itr != result_set.end() && (DB_NOERROR == rc); ++itr)
        {
            const int32_t id = itr->at("id").to<int32_t>();
            const int32_t data = itr->at("data").to<int32_t>();
            process_record(id, data);

        }
        std::cout << result_set.size() << " rows of \"storage\" table processed" << std::endl;
    }

    return rc;
}

int
example_main(int argc, char *argv[])
{
    int rc = DB_NOERROR;

    const char * database_uri = EXAMPLE_DATABASE;
    if (argc > 1) {
        database_uri = argv[1];
    }

    storage::ittiadb::Connection database(database_uri);

    // Create a new database file
    if (DB_OK != database.open(storage::ittiadb::Connection::CreateAlways)) {
        GET_ECODE(rc, DB_FAIL, "Opening database ");
    }
    if (DB_NOERROR == rc) {
        rc = ins_data(database);
    }

    if (DB_NOERROR == rc) {
        if (DB_OK != storage::ittiadb::Transaction(database).commit()) {
            GET_ECODE(rc, DB_FAIL, "Commiting Transaction ");
        }
    }

    if (DB_NOERROR == rc) {
        rc = select_query(database);
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return (DB_NOERROR != rc) ? EXIT_FAILURE : EXIT_SUCCESS;
}
