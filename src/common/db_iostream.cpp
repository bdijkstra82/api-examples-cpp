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

#include "db_iostream.h"

#include <string>

#include "storage/ittiadb/query.h"
#include "storage/ittiadb/transaction.h"

using ::storage::data::Environment;
using ::storage::ittiadb::Connection;
using ::storage::ittiadb::Query;
using ::storage::ittiadb::Transaction;
using ::storage::data::RowSet;

/// Execute a SQL query for each line of console input
void
sql_line_shell(Connection& database, std::istream& in, std::ostream& out, std::ostream& err)
{
    while (in.good()) {
        std::string statement;
        out << database.uri() <<  "> " << std::flush;
        std::getline(in, statement);

        // Continue until a blank line is entered
        if (statement.empty()) {
            (void)Transaction(database).commit();
            break;
        }

        // Remove trailing semicolon, if present
        if (*(statement.end() - 1) == ';') {
            statement.resize(statement.length() - 1);
        }

        // Execute the SQL statement
        RowSet<> row_set;
        int64_t modified_rows;
        if (DB_OK == Query(database, statement.c_str()).execute(row_set, modified_rows))
        {
            if (!row_set.columns().empty()) {
                out << row_set.columns() << std::endl;
                out << row_set;
            }
            else if (modified_rows >= 0) {
                out << modified_rows << " rows modified" << std::endl;
            }
        }
        else {
            const ::storage::data::Error error = Environment::error();
            err << error.name << ": " << error.description << std::endl;
        }
    }
}

