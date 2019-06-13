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

/// @file storage_encryption.cpp
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

#include "db_iostream.h"

static const char * EXAMPLE_DATABASE = "storage_encryption.ittiadb";
static const char* EXAMPLE_BACKUP_DATABASE = "storage_encryption-backup.ittiadb";

using ::storage::data::Environment;
using ::storage::ittiadb::Connection;

int
example_main(int argc, char *argv[])
{
    int rc = DB_NOERROR;
    static const char encryption_key[(256 / 8) +1] = "32byte_256bit_length_encrypt_key";

    const char * database_uri = EXAMPLE_DATABASE;
    if (argc > 1) {
        database_uri = argv[1];
    }

    const char * backup_database_uri = EXAMPLE_BACKUP_DATABASE;
    if (argc > 2) {
        backup_database_uri = argv[2];
    }

    Connection::AuthInfo auth_info;
    Connection::FileStorageConfig storage_cfg;

    /* Set AES-256 ecryption parameters */
    storage_cfg.auth_info = &auth_info;
    auth_info.set_cipher(DB_CIPHER_AES256_CTR, &encryption_key);

    // Create encrypted database file
    Connection database(database_uri);
    
    if (DB_OK != database.open(Connection::CreateAlways, storage_cfg))
    {
        (void)GET_ECODE(rc, DB_FAIL, "unable to create database ");
    }

    std::cout << "Enter SQL statements or an empty line to continue" << std::endl;
    sql_line_shell(database);

    // Close all connections to the database to forget the encryption key
    database.close();

    // Try to open database with the wrong encryption key 
    if (DB_NOERROR == rc)
    {
        static const char wrong_key[(256 / 8) + 1] = "32byte_256bit_wrongx_encrypt_key";

        auth_info.set_cipher(DB_CIPHER_AES256_CTR, &wrong_key);

        if (DB_OK != database.open(Connection::OpenExisting, storage_cfg)) {
            if (!Environment::is_error(DB_ESTORAGE)) {
                std::cout << "AES-encrypted DB has erroneously opened with intentionally-wrong encryption key "
                    << Environment::error() << std::endl;
            }
        }
    }

    // Open the database file with the correct encryption key. 
    auth_info.set_cipher(DB_CIPHER_AES256_CTR, &encryption_key);

    rc = DB_NOERROR;
    if (DB_OK != database.open(storage::ittiadb::Connection::OpenExisting, storage_cfg)) {
        (void)GET_ECODE(rc, DB_FAIL, "unable to open database ");
    }

    // Use backup to make a copy of the database with a different encryption key
    if (DB_NOERROR == rc)
    {
        static const char backup_key[(256 / 8) + 1] = "32byte_256bit_backup_encrypt_key";

        if (DB_OK != database.backup(backup_database_uri, DB_UTF8_NAME, DB_CIPHER_AES256_CTR, backup_key) ) {
            (void)GET_ECODE(rc, DB_FAIL, "unable to backup database");
        }

        // Close original database
        database.close();

        /* Try to open backup database */
        auth_info.set_cipher(DB_CIPHER_AES256_CTR, &backup_key);
        Connection backup_database(backup_database_uri);

        if (DB_NOERROR == rc &&
            DB_OK != backup_database.open(Connection::OpenExisting, storage_cfg))
        {            
            (void)GET_ECODE(rc, DB_FAIL, "unable to open backup database");
        }

        std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
        sql_line_shell(backup_database);

        backup_database.close();
    }

    return (DB_NOERROR != rc) ? EXIT_FAILURE : EXIT_SUCCESS;
}
