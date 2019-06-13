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

///  @file transaction_rollback.cpp
/// 
/// Command-line example program demonstrating the ITTIA DB Storage API.

#include <stdlib.h>

#include "storage/data/environment.h"
#include "storage/ittiadb/nested_transaction.h"
#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/table.h"
#include "storage/ittiadb/query.h"
#include "storage/data/single_row.h"
#include "storage/data/single_field.h"
#include "storage/types/date_and_time.h"
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

/// Load packets of readings in DB table. Each packet in personal transaction
static int
load_readings(storage::ittiadb::Connection& db)
{
    int rc = DB_NOERROR;
    int i;
    int64_t current_packet = 0;
    int is_wrong_packet = 0;
    int in_tx = 0;
    int good_packets = 0;

    static storage_t readings[] = {
        // sensorid, packetid, timestamp, temperature
        // Invalid packet
        { 1, 1, "2015-07-32 01:01:01", "-1.1" },
        { 1, 1, "2015-07-29 01:05:01", "-1.2f" },
        { 2, 1, "2015-07-29 01:01:01", "-1.3" },
        { 2, 1, "2015-07-29 25:05:01", "-2.4" },
        { 2, 1, "2015-07-29 01:10:01", "-3.5" },
        { 3, 1, "2015-07-29 01:01:01", "-1.6" },
        { 3, 1, "2015-07-29 01:05:01", "-2.7" },
        { 3, 1, "2015-07-29 01:10:01", "-3.8" },
        { 3, 1, "2015-07-29 01:15:01", "-4.9" },
        { 3, 1, "2015-02-30 01:20:01", "-5.0" },

        // Good packet
        { 1, 2, "2015-07-29 01:01:01", "-1.1" },
        { 1, 2, "2015-07-29 01:05:01", "1.9" },
        { 2, 3, "2015-07-29 01:01:01", "11.1" },
        { 2, 3, "2015-07-29 01:10:01", "13.5" },
        { 3, 3, "2015-07-30 01:01:01", "1.6" },
        { 3, 3, "2015-07-30 01:05:01", "2.7" },
        { 3, 3, "2015-07-30 01:10:01", "3.8" },

    };

    storage::ittiadb::Table table(db, "readings");
    if (DB_OK != table.open(storage::ittiadb::Table::Exclusive)) {
        GET_ECODE(rc, DB_FAIL, "Opening 'reading' table ");
    }
    storage::data::SingleRow row_to_insert(table.columns());
    storage::ittiadb::Transaction tx(db);

    for( i = 0; i < DB_ARRAY_DIM(readings) && DB_NOERROR == rc; ++i ) {
        // Start a new transaction when a new packet is encountered 
        if( current_packet != readings[i].packetid ) {
            // Commit only if the last packet contained accurate data
            if( is_wrong_packet ) {
                // Previous packet had bad data, so roll back its changes 
                if ( DB_OK != tx.rollback() ) {
                    GET_ECODE(rc, DB_FAIL, "unable to roll back packet data");
                }
            }
            else if ( in_tx ) {
                // Commit previous packet's changes
                if ( DB_OK != tx.commit() ) 
                {
                    GET_ECODE(rc, DB_FAIL, "unable to commit packet data");
                }
                good_packets++;
            }
            else {
                std::cerr << "unable to commit packet data"<<std::endl;
            }
            is_wrong_packet = 0;
            current_packet = readings[i].packetid;
            if(DB_OK != tx.begin())
            {
                GET_ECODE(rc, DB_FAIL, "Transaction Begin Failed ");
            }
            in_tx = 1;
        }
        // Try to insert reading
        if( DB_OK != row_to_insert["sensorid"].set(readings[i].sensorid) ||
            DB_OK != row_to_insert["packet"].set(readings[i].packetid) ||
            DB_OK != row_to_insert["timepoint"].set(readings[i].timepoint) ||
            DB_OK != row_to_insert["temperature"].set(readings[i].temperature) ||
            DB_OK != table.insert(row_to_insert) )
        {        
        
            // Some error occured on insertion
            int ecode = DB_NOERROR;
            GET_ECODE(ecode, DB_FAIL);
            is_wrong_packet = 1;
            // See the kind of error
            if (DB_EINVALIDNUMBER == ecode
                || DB_ECONVERT == ecode
                || DB_EINVTYPE == ecode
                || DB_EDUPLICATE == ecode
                )
            {
                // It seems data conversion error. Continue loading loop
                std::cerr << "Couldn't insert reading. Skip packet" << readings[i].packetid <<std::endl;
            }
            else if (DB_NOERROR != ecode){
                // Some sort of system error occured. Break loading loop
                std::cerr << "Unhandled error while insert reading. Break loading row: " << row_to_insert << std::endl;
                break;
            }
        }
    }
    // If last good packet's changes is uncommited, commit them now
    if( in_tx && !is_wrong_packet ) {
        if( DB_OK == tx.commit( ))  {
            good_packets++;
        }
        else {
            GET_ECODE(rc,DB_FAIL, "Couldn't commit packet data" );
        }
    }

    return good_packets > 0 ? DB_NOERROR : DB_FAILURE;
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

    if (DB_NOERROR == rc)
    {
        // Start transactions generation. Part of transactions commit with 
        // DB_LAZY_COMPLETION flag to make them to be deffered
        rc = load_readings(database);
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return (DB_NOERROR != rc) ? EXIT_FAILURE : EXIT_SUCCESS;
}
