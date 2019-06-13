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
#ifndef DB_IOSTREAM_H
#define DB_IOSTREAM_H

#include <iostream>

#include "storage/data/environment.h"
#include "storage/data/row_set.h"
#include "storage/ittiadb/connection.h"

//Output database error message
inline std::ostream& operator<<(std::ostream& os, const storage::data::Error& error)
{
    os << " error " << error.name << " : " << error.description;
    return os;
}

/// Output the names of the fields in a row definition.
inline std::ostream& operator<<(std::ostream& os, const storage::data::RowDefinition& fields)
{
    for (storage::data::RowDefinition::const_iterator iter = fields.begin();
        iter != fields.end();
        ++iter)
    {
        if (iter != fields.begin())
            os << ",";

        os << iter->name();
    }
    return os;
}

/// Output a generic result field by converting it to a string.
inline std::ostream& operator<<(std::ostream& os, const storage::data::Field& field)
{
    if (!field.exists())
        os << "n/a";
    else if (field.is_null())
        os << "null";
    else
        os << field.to<std::string>();
    return os;
}

/// Output a generic result row by converting each field to a string.
inline std::ostream& operator<<(std::ostream& os, const storage::data::Row& row)
{
    for (storage::data::Row::const_iterator iter = row.begin();
        iter != row.end();
        ++iter)
    {
        if (iter != row.begin())
            os << ",";
        os << *iter;
    }
    return os;
}

/// Output each result row on a new line.
inline std::ostream& operator<<(std::ostream& os, const storage::data::RowSet<>& result_set)
{
    for (storage::data::RowSet<>::const_iterator iter = result_set.begin();
        iter != result_set.end();
        ++iter)
    {
        os << *iter << std::endl;
    }
    return os;
}

inline const int& GET_ECODE(int& rc, const db_result_t& dbrc, const char* error_msg=NULL)
{
    rc = DB_NOERROR;
    const storage::data::Error& error = storage::data::Environment::error();
    if (DB_OK != dbrc)
    { 
        if (NULL != error_msg)
        {
            std::cerr << error_msg << " :: " << error << std::endl;
        }
        rc = error.code;
    }   
    return rc;
}

inline const int& GET_ECODE(int& rc, const int64_t& affected_rows, const char* error_msg)
{
    rc = DB_NOERROR;
    const storage::data::Error& error = storage::data::Environment::error();
    if (affected_rows < 0)
    {
        std::cerr << error_msg << " :: " << error << std::endl;
        rc = error.code;
    }
    return rc;
}

void sql_line_shell(::storage::ittiadb::Connection& database, std::istream& in = std::cin, std::ostream& out = std::cout, std::ostream& err = std::cerr);

#endif //DB_IOSTREAM_H
