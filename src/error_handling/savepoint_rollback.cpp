/**************************************************************************/
/*                                                                        */
/*      Copyright (c) 2005-2015 by ITTIA L.L.C. All rights reserved.      */
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

/// @file savepoint_rollback.cpp
///
///Command-line example program demonstrating the ITTIA DB Storage API.
///

#include <stdlib.h>
#include "storage/data/environment.h"
#include "storage/ittiadb/nested_transaction.h"
#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/table.h"
#include "storage/ittiadb/query.h"
#include "storage/data/single_row.h"
#include "storage/data/single_field.h"
#include "db_iostream.h"

static const char* EXAMPLE_DATABASE = "savepoint_rollback.ittiadb";

/// Declarations to use while populating table

//Populate table using string-type only relative bounds fields. So declare appropriate structure. See
//ittiadb/manuals/users-guide/api-c-database-access.html#relative-bound-fields
typedef struct
{
    int32_t   sensorid;
    int64_t   packetid;
    db_ansi_t   timepoint[20];
    db_ansi_t   temperature[10];

} storage_t;

static int
do_transaction_with_savepoint(storage::ittiadb::Connection& db)
{
    int rc = DB_NOERROR;    
    int good_readings = 0;
    int bad_readings = 0;
    int i = 0;
    static storage_t readings[9] = {
        // Good&Bad readings
        { 1, 2, "2015-07-29 01:05:01", "1.9" },
        { 2, 3, "2015-07-29 01:01:01", "11.1" },
        { 1, 1, "2015-07-32 01:01:01", "-1.1" },    //< Invalid date
        { 2, 3, "2015-07-29 01:10:01", "13.5" },
        { 3, 3, "2015-07-30 01:01:01", "1.6" },
        { 3, 1, "2015-02-30 01:20:01", "-f5.0" },   //< Invalid temperature
        { 3, 3, "2015-07-30 01:05:01", "2.7" },
        { 3, 3, "2015-07-30 01:10:01", "3.8" },
        { 3, 3, "2015-07-30 01:10:01", "3.8" },     //< Pkey violaton

    };

    storage::ittiadb::Table table(db,"readings");
    if (DB_OK != table.open(storage::ittiadb::Table::Exclusive)) {
        GET_ECODE(rc, DB_FAIL, "Opening 'reading' table ");
    }
    storage::data::SingleRow row_to_insert(table.columns());
    storage::ittiadb::Transaction tx(db);
    if (DB_OK != tx.begin()) {
        GET_ECODE(rc, DB_FAIL, "Beginning nested transaction ");
    }
    if (DB_NOERROR == rc)
    {
        for (i = 0; (i < DB_ARRAY_DIM(readings)); ++i)
        {

            storage::ittiadb::NestedTransaction ntx(db);
            if (DB_OK == ntx.begin_or_set()) {
                row_to_insert["sensorid"].set(readings[i].sensorid);
                row_to_insert["packet"].set(readings[i].packetid);
                row_to_insert["timepoint"].set(readings[i].timepoint);
                row_to_insert["temperature"].set(readings[i].temperature);
                
                if (DB_OK == table.insert(row_to_insert))
                {
                    good_readings++;
                    ntx.commit_or_release();
                    continue;
                }
                else
                {
                    // If insert failed, do rollback to sp1, skip reading & try with next one
                    bad_readings++;
                    continue;
                }
            }
            else
            {
                GET_ECODE(rc, DB_FAIL, "Transaction begin failed ");
            }
        }
        if (tx.isActive() && DB_OK == tx.commit())
        {
            std::cerr << "Count of readings (overall/good/bad): ("
                << DB_ARRAY_DIM(readings) << "/" << good_readings << "/" << bad_readings
                << "). Overall - good - bad: " << (DB_ARRAY_DIM(readings) - good_readings - bad_readings)
                << std::endl;
        }
    }

    return DB_ARRAY_DIM( readings ) == good_readings + bad_readings ? DB_NOERROR : DB_FAILURE;
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

    // Create schema
    if (DB_NOERROR == rc)
    {
    // Table "readings" contains readings of several temperature sensors.
    // Assume that readings is arrived from some source in packets.
    // Every packet contains several readings from several sensors.
    // Packet number is monotonically increasing integer.
    //
    // Every sensor has it's unique id.
    // Reading's values is 'temperature' and 'timepoint'. 
    //   - If one of those values is wrong whole packet should be ignored.
    //
    // Primary key for readings table is(sensorid, timepoint).
        const int64_t rows_affected = storage::ittiadb::Query(database, "CREATE TABLE readings(" 
                                                      " sensorid sint32 NOT NULL," 
                                                      " packet sint64 NOT NULL,"
                                                      " timepoint timestamp NOT NULL,"
                                                      " temperature float64 NULL,"
                                                      " CONSTRAINT pkname PRIMARY KEY(sensorid,timepoint) )"
                                                      ).execute();
        GET_ECODE(rc, rows_affected, "Creating 'readings' Table ");
    }

    // Start transactions with savepoints
    if (DB_NOERROR == rc)
    {
        rc = do_transaction_with_savepoint(database);
    }
    else
    {
        database.close();
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return (DB_NOERROR != rc) ? EXIT_FAILURE : EXIT_SUCCESS;
}
