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

/// @file unicode_character_strings.cpp
///
///Command-line example program demonstrating the ITTIA DB Storage API.

#include <stdlib.h>

#include "storage/data/environment.h"
#include "storage/ittiadb/transaction.h"
#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/table.h"
#include "storage/ittiadb/query.h"
#include "storage/data/single_row.h"
#include "storage/data/single_field.h"
#include "db_iostream.h"

static const char* EXAMPLE_DATABASE = "unicode_character_strings.ittiadb";

static const int32_t MAX_TEXT_LEN = 50;

typedef struct {
    uint32_t    id;
    char        en[MAX_TEXT_LEN + 1];
    char        jp[MAX_TEXT_LEN + 1];
    char        fr[MAX_TEXT_LEN + 1];
    char        es[MAX_TEXT_LEN + 1];
    char        ru[MAX_TEXT_LEN + 1];
} storage_t;

static storage_t texts[] = {
    { 1, "Folio",       "フォリオ",         {0},                "Folio",            "Фолио"         },
    { 2, "General",     "一般",             {0},                "General",          "Основные"      },
    { 3, "Generic",     "汎用",             "Générique",        "Genérico",         "Общее"         },
    { 4, "Grayscale",   "グレースケール",   "Niveaux de gris",  "Escale de grises", "Оттенки серого"},
    { 5, "Light",       "薄い",             "Clair",            "Ligero",           "Светлый"       },
    { 6, "Medium",      "紙質",            "Moyen",            "Media",            "Средний"       },
    { 7, "No",          "いいえ",           "Non",              "No",               "Нет"           },
    { 8, "No Content",  "中身がありません", "Aucun contenu",    "No hay contenido", "Нет контента"  },
};

static int
load_data( storage::ittiadb::Connection& db )
{
    int ecode = DB_NOERROR;    
    int i;
    storage::ittiadb::Table table(db,"storage");
    if (DB_OK != table.open(storage::ittiadb::Table::Exclusive)) {
        GET_ECODE(ecode, DB_FAIL, "FAiled to open Storage table ");
    }
    else {
        
        for (i = 0; i < DB_ARRAY_DIM(texts) && DB_NOERROR == ecode; ++i) {
            // Prepare & populate single Row for insert
            storage::data::SingleRow row_to_insert(table.columns());
            if (DB_OK == row_to_insert["id"].set(texts[i].id) &&
                DB_OK == row_to_insert["en"].set(texts[i].en) &&
                DB_OK == row_to_insert["jp"].set(texts[i].jp) &&
                DB_OK == row_to_insert["fr"].set(texts[i].fr) &&
                DB_OK == row_to_insert["es"].set(texts[i].es) &&
                DB_OK == row_to_insert["ru"].set(texts[i].ru) ) {

                if (DB_OK != table.insert(row_to_insert))
                {
                    GET_ECODE(ecode, DB_FAIL, "Failed to insert row ");
                }
            }
            else {
                GET_ECODE(ecode, DB_FAIL, "Failed to prepare row for insert ");
            }
        }
        
        if (DB_OK != storage::ittiadb::Transaction(db).commit()) {
            GET_ECODE(ecode, DB_FAIL, "Failed to commit transaction ");
        }
    }
    if (DB_NOERROR != ecode)
    {
        std::cerr << "Couldn't populate 'storage' table" << std::endl;
    }
    return ecode;
}

static int
find_text(storage::ittiadb::Connection& db, const char * textid, const char * locale)
{
    int ecode = DB_NOERROR;

    storage::ittiadb::Table table(db, "storage");
    if (DB_OK != table.open("pkey_storage", storage::ittiadb::Table::Exclusive))
    {
        GET_ECODE(ecode, DB_FAIL, "Failed to Open table with pkey_storage ");
    }
    else {

        char en[MAX_TEXT_LEN + 1] = { 0 };
        char tr[MAX_TEXT_LEN + 1] = { 0 };
        storage::data::SingleRow key(table.columns());
        storage::data::SingleRow data(table.columns());
        key["id"].set(textid);
        if (DB_OK != table.search_by_index(DB_SEEK_FIRST_EQUAL, key, 1, data)){
            GET_ECODE(ecode, DB_FAIL, "Search by index ");
            std::cerr << "Couldn't find translated text for messageid " << textid << std::endl;
        }
        else
        {
            data["en"].get(en);
            data[locale].get(tr);
            std::cout << "Message. Id: " << textid << ", en: [" << en << "], "
                << locale << ": [" << tr << "]" << std::endl;
        }
    }
    return ecode;
}

int
example_main(int argc, char *argv[])
{
    int rc = DB_NOERROR;
    if ( 2 < argc && 0 == strncmp( "-h", argv[1], 2 ) ) {
        std::cout <<
            "Usage:" << std::endl
            << "    " << argv[0] << " <database> <messageid> <locale>" << std::endl
            << "Parameters:" << std::endl
            << "    database  - File name or database URL" << std::endl
            << "    messageid - Integer in range [1-" << DB_ARRAY_DIM(texts) << "]. '1' by default" << std::endl
            << "    locale    - One of en, jp, fr, es, ru. 'ru' by default" << std::endl;
    }

    const char * database_uri = EXAMPLE_DATABASE;
    if (argc > 1) {
        database_uri = argv[1];
    }

    storage::ittiadb::Connection database(database_uri);
    
    if (DB_OK != database.open(storage::ittiadb::Connection::CreateAlways))
    {
        GET_ECODE(rc, DB_FAIL, "Database open failed ");
        database.close();
        return rc;
    }

    storage::ittiadb::Query create_table(database, "create table storage ("
        "  id sint32 NOT NULL,"
        "  en utf8str(50),"
        "  jp utf8str(50),"
        "  fr utf8str(50),"
        "  es utf8str(50),"
        "  ru utf8str(50),"
        "  CONSTRAINT pkey_storage PRIMARY KEY(id)"
        ")");
    int64_t rows_affected = -1;
    rows_affected = create_table.execute();

    GET_ECODE(rc, rows_affected, "Schema creation failed ");

    if( DB_NOERROR == rc ) {
        rc = load_data( database );
        if( DB_NOERROR == rc ) {
            rc = find_text(database, argc > 2 ? argv[2] : "1", argc > 3 ? argv[3] : "ru");
        }
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return (DB_NOERROR != rc) ? EXIT_FAILURE : EXIT_SUCCESS;
}
