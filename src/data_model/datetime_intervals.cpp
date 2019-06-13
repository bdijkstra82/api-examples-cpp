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

/// @file datetime_intervals.cpp
///
///Command-line example program demonstrating the ITTIA DB Storage API.
///

#include <stdlib.h>

#include "storage/data/environment.h"
#include "storage/ittiadb/transaction.h"
#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/table.h"
#include "storage/ittiadb/query.h"
#include "storage/data/single_row.h"
#include "storage/data/single_field.h"
#include "storage/types/date_and_time.h"
#include "db_iostream.h"

static const char* EXAMPLE_DATABASE = "datetime_intervals.ittiadb";

static int
datetime_example(storage::ittiadb::Connection& db)
{
    int rc = DB_NOERROR;
    int64_t rows_affected = -1;
    // Create 'storage' table
    rows_affected = storage::ittiadb::Query(db, "CREATE TABLE storage("
        " d  date NOT NULL, "
        " t  time NOT NULL, "
        " dt timestamp NOT NULL )").execute();
    if (rows_affected < 0)
    {
        GET_ECODE(rc, DB_FAIL, "'storage' table schema creation failed ");
    }

    //Insert data using standard 
    if (DB_NOERROR == rc) {
        storage::ittiadb::Query insert_using_standard(db, "INSERT INTO storage(d, t, dt)"
            "VALUES(?,?,?)");
        if (DB_OK != insert_using_standard.prepare())
        {
            GET_ECODE(rc, DB_FAIL, " 'insert using standard format' query prepare failed ");
        }


        storage::data::SingleRow params(insert_using_standard.parameters());
        if (DB_NOERROR == rc &&
            DB_OK == params[0].set(storage::types::Date(2015, 01, 01)) &&
            DB_OK == params[1].set(storage::types::Time(13, 59, 48)) &&
            DB_OK == params[2].set(storage::types::Timestamp(2015, 01, 01,13, 59, 59, 123456))) {
            if (0 > insert_using_standard.execute_with(params)) {
                GET_ECODE(rc, DB_FAIL, "'insert using standard format' query execution failed");
            }
        }
        else {
            GET_ECODE(rc, DB_FAIL, "Query Params initialization failed");
        }

    }

    //Insert custom date/time formats
    if (DB_NOERROR == rc) {
        if (0 > storage::ittiadb::Query(db, "INSERT INTO storage (d, t, dt)"
            "  VALUES( date_parse('15 31 3', 'YY DD M'),"
            "          '07:09:04',"
            "          date_parse('Fri Dec 13, 2001 at 5PM', 'DDD MMM D\",\" YYYY \"at\" htt', '')  )"
            ).execute()) {
            GET_ECODE(rc, DB_FAIL, "Custom Query Params execution failed");
        }
    }

    // Extracting hours, minutes, and seconds from a time column.
    if (DB_NOERROR == rc) {
        storage::data::SingleRow result_row;
        storage::ittiadb::Query select_query(db,
            "SELECT extract( hour from t),"
            "       extract( minute from t ),"
            "       extract( second from t ), t "
            "  FROM storage FETCH FIRST ROW ONLY");
        if (DB_OK != select_query.prepare() || (0 > select_query.execute(result_row)))
        {
            GET_ECODE(rc, DB_FAIL, "Extracting hours, minutes, and seconds from a time column.");
        }
        else {
            const storage::types::Time& time = result_row[3].to<storage::types::Time>();            
            std::cout << "Time: " << time.hour << ":" << time.minute << ":"<<time.second
                    <<". Extracted (hr:mm:ss) : ( "
                    << result_row[0] << ":" << result_row[1] << ":" << result_row[2] << ")" << std::endl;
        }
    }

    // Extracting years, months, and days from a date column
    if (DB_NOERROR == rc) {
        storage::data::SingleRow result_row;
        storage::ittiadb::Query select_query(db,
            "SELECT extract( year from d ),"
            "       extract( month from d ),"
            "       extract( day from d ), d "
            "  FROM storage FETCH FIRST ROW ONLY");
        if (DB_OK != select_query.prepare() || (0 > select_query.execute(result_row)))
        {
            GET_ECODE(rc, DB_FAIL, "Extracting hours, minutes, and seconds from a time column.");
        }
        else {
            const storage::types::Date& date = result_row[3].to<storage::types::Date>();

            std::cout << "Date: " << date.year << "-" << date.month << "-" << date.day
                << ". Extracted (year,month,seconds) : (" 
                << result_row[0] << "," << result_row[1] << "," << result_row[2] << ")" << std::endl;
        }
    }

    // Converting date and time columns to UNIX timestamp.
    if (DB_NOERROR == rc){
        storage::data::SingleRow result_row;
        storage::types::Date DT;
        storage::types::Time TM;
        storage::types::IntervalDayToSecond d1;
        storage::types::IntervalDayToSecond d2;

        if (0> storage::ittiadb::Query(db,
            "SELECT d, t, "
            "( d - UNIX_EPOCH ) second ,"
            "( t - time '00:00:00' ) second "
            "  FROM storage").execute(result_row)){ //?? calling fetch first row doesn't work
            GET_ECODE(rc, DB_FAIL, "Query Converting date and time columns to UNIX timestamp ");
        }
        else {
                
                (void)result_row[0].get(DT);
                (void)result_row[1].get(TM);
                (void)result_row[2].get(d1);
                (void)result_row[3].get(d2);
                std::cout 
                    << "Date,Time : " << DT.year << "-" << DT.month << "-" << DT.day
                    << "," << TM.hour << ":" << TM.minute << ":" << TM.second
                    << " ->Unixtime: " << d1.to_seconds<int64_t>() << "," << d2.to_seconds<int64_t>()
                    << std::endl;

        }


    // Converting date and time columns from UNIX timestamp.
        storage::ittiadb::Query sql_query(db, "SELECT UNIX_EPOCH +  cast( ? as interval second )  ");
        if (DB_OK == sql_query.prepare()) {
            storage::data::SingleRow params(sql_query.parameters());            
            params[0].set(d1);
            storage::data::SingleRow result_row;
            sql_query.execute_with(params, result_row);
            const storage::types::Date& date= result_row[0].to<storage::types::Date>();
            
            std::cout << "Date: " << DT.year << "-" << DT.month << "-" << DT.day
                << "-> Unixtime: " << d1.to_seconds<int64_t>() << "-> Date again : "
                << date.year << "-" << date.month << "-" << date.day << std::endl;
        }
        else {
            GET_ECODE(rc, DB_FAIL, "while converting unixtime to Date type ");
        }
        
    }
    //Adding months, years, or days to a date column.
    //Adding hours, minutes, or seconds to a time column.
    if (DB_NOERROR == rc) {
        storage::data::RowSet<> result_set;
        storage::ittiadb::Query select_query(db,
            "select d, d + cast( 1 as interval day ) + interval '1' month + interval '10' year, "
            "t, t + cast( 1 as interval hour ) + interval '1' minute + interval '10' second "
            "from storage ");
        if (DB_OK != select_query.prepare() || (0 > select_query.execute(result_set)))
        {
            GET_ECODE(rc, DB_FAIL, "while hour, minute, second components extracting from time-type column ");
        }
        else {
            storage::data::Row r = result_set.front();
            std::cout << "Date " << r[0] << " +1 day + 1 month + 10 years is : " << r[1] << std::endl
                << "Time " << r[2] << " + 1 hour + 1 minute + 10 seconds is: " << r[3] << std::endl;
        }
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

    // Create a new file storage database with default parameters.
    storage::ittiadb::Connection database(database_uri);
    if (DB_OK != database.open(storage::ittiadb::Connection::CreateAlways)) {
        GET_ECODE(rc, DB_FAIL, "Opening database Failed");
    }

    if( DB_NOERROR == rc ) {
        rc = datetime_example( database );
    }

    if( DB_NOERROR == rc ) {
        storage::ittiadb::Transaction(database).commit();
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return (DB_NOERROR != rc) ? EXIT_FAILURE : EXIT_SUCCESS;
}
