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

static const char* EXAMPLE_DATABASE = "sql_parameters.ittiadb";

static int
ins_data(storage::ittiadb::Connection& db)
{
    int rc = DB_NOERROR;
    
    storage::ittiadb::Query create_table(
        db,
        "create table storage ("
        "  id sint32 not null primary key,"
        "  data sint32"
        ")"
    );

    storage::ittiadb::Query insert_to_table(
        db,
        "insert into storage (id, data)"
        "  select N, 100 - N from $nat(100)"
    );

    // Prepare SQL Query
    if ( ( DB_OK != create_table.prepare()) 
        && ( DB_OK != insert_to_table.prepare() ) )
    {
        GET_ECODE(rc, DB_FAIL, "prepare SQL failed");
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
    /* Stub function: process a record from the database. */
}

static int
select_parametrized_query(storage::ittiadb::Connection& db)
{
    int rc = DB_NOERROR;
    int32_t id_param;
    int32_t data_param;
    storage::ittiadb::Query select_query(
        db,
        "select id, data"
        "  from storage"
        "  where id > ? and data > ?"
    );

    if (DB_OK != select_query.prepare()) {
        GET_ECODE(rc, DB_FAIL, "preparing parametrized query");
    }
    

    if (DB_NOERROR == rc)
    {
        id_param = 98;
        data_param = 0;
        storage::data::SingleRow params_row(select_query.parameters());
        params_row[0].set(id_param);
        params_row[1].set(data_param);


        storage::data::RowSet<> result_set(select_query.columns());
        if (DB_OK != select_query.execute_with(params_row, result_set)) {
            GET_ECODE(rc, DB_FAIL, "executing parameterize query");
        }
        storage::data::RowSet<>::const_iterator itr = result_set.begin();
        for (; itr != result_set.end() && DB_NOERROR == rc; ++itr)
        {
            const int32_t id = itr->at(0).to<int32_t>();
            const int32_t data = itr->at(1).to<int32_t>();
            process_record(id, data);
        }
        std::cout << result_set.size() << " rows of 'storage' table processed with (id, data) = ("
            << params_row << ")" << std::endl;
    }

    if (DB_NOERROR == rc)
    {
        // Reexecute with changed parameters
        id_param = 97;
        data_param = 1;

        storage::data::RowSet<> result_set(select_query.columns());
        storage::data::SingleRow params_row(select_query.parameters());
        params_row[0].set(id_param);
        params_row[1].set(data_param);

        if (DB_OK != select_query.execute_with(params_row, result_set)) {
            GET_ECODE(rc, DB_FAIL, "executing parameterize query");
        }
        storage::data::RowSet<>::iterator itr = result_set.begin();
        for (; itr != result_set.end() && DB_NOERROR == rc; ++itr)
        {
            const int32_t id = itr->at("id").to<int32_t>();
            const int32_t data = itr->at("data").to<int32_t>();
            process_record(id, data);
        }
        std::cout << result_set.size() << " rows of 'storage' table processed with (id, data) = ("
            << params_row << ")" << std::endl;
    }

    if (DB_NOERROR == rc)
    {
        // Reexecute with changed parameters
        id_param = 96;
        data_param = 1;

        storage::data::RowSet<> result_set(select_query.columns());
        storage::data::SingleRow params_row(select_query.parameters());
        params_row[0].set(id_param);
        params_row[1].set(data_param);

        if (DB_OK != select_query.execute_with(params_row, result_set)) {
            GET_ECODE(rc, DB_FAIL, "executing parameterize query");
        }
        storage::data::RowSet<>::iterator itr = result_set.begin();
        for (; itr != result_set.end() && DB_NOERROR == rc; ++itr) {
            const int32_t id = itr->at("id").to<int32_t>();
            const int32_t data = itr->at("data").to<int32_t>();
            process_record(id, data);
        }
        std::cout << result_set.size() << " rows of 'storage' table processed with (id, data) = ("
            << params_row << ")" << std::endl;
    }

    // Parameterize FETCH FIRST and OFFSET clauses to read the entire table in pages.
    storage::ittiadb::Query fetch_first_offset_query(
        db,
        "select id, data"
        "  from storage"
        "  order by id"
        "  offset ? rows"
        "  fetch first ? rows only"
    );

    if (DB_OK != fetch_first_offset_query.prepare()) {
        GET_ECODE(rc, DB_FAIL, "Preparing fetch_first_offset_query ");
    }
    storage::data::SingleRow fetch_first_param(fetch_first_offset_query.parameters());
    storage::data::RowSet<> fetch_first_result_set(fetch_first_offset_query.columns());
    int32_t offset_param = 0;
    int32_t ffirst_param = 10;

    do {
        // Execute prepared cursor
        fetch_first_param[0].set(offset_param);
        fetch_first_param[1].set(ffirst_param);
        //clear the result set to repopulate again.
        fetch_first_result_set.clear();
        if (DB_OK != fetch_first_offset_query.execute_with(fetch_first_param, fetch_first_result_set) ) {
            GET_ECODE(rc, DB_FAIL, "Executing fetch_first_offset_query ");
        }

        storage::data::RowSet<>::const_iterator itr = fetch_first_result_set.begin();
        for (; itr != fetch_first_result_set.end() && (DB_NOERROR == rc); ++itr)
        {
            const int32_t id = itr->at("id").to<int32_t>();
            const int32_t data = itr->at("data").to<int32_t>();
            process_record(id, data);
        }
        std::cout << fetch_first_result_set.size() 
            <<  " rows of \"storage\" table processed with (offset, ffirst) = ("
            << fetch_first_param << ")" << std::endl;
        offset_param += ffirst_param;

    } while ((fetch_first_result_set.size() > 0) && (DB_NOERROR == rc));

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
    if (DB_OK != database.open(storage::ittiadb::Connection::CreateAlways))
    {
        GET_ECODE(rc, DB_FAIL, "Opening database ");
    }

    if( DB_NOERROR == rc ) {
        rc = ins_data( database );
    }

    if( DB_NOERROR == rc && DB_OK != storage::ittiadb::Transaction(database).commit()) {
        GET_ECODE(rc, DB_FAIL, "Commiting Transaction ");
    }

    if (DB_NOERROR == rc) {
        rc = select_parametrized_query(database);
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return (DB_NOERROR != rc) ? EXIT_FAILURE : EXIT_SUCCESS;
}
