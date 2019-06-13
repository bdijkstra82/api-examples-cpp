/**************************************************************************/
/*                                                                        */
/*      Copyright (c) 2005-2019 by ITTIA L.L.C. All rights reserved.      */
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

//---------------------------------------------------------------------------
// phonebook_sql - ITTIA DB SQL electronic phonebook example application.
//---------------------------------------------------------------------------
// Copyright (C) 2005-2007 ITTIA. All rights reserved.
//---------------------------------------------------------------------------
/** @file phonebook_sql.cpp
 *
 * Console example program demonstrating the ITTIA DB SQL C++ Storage API.
 * This example use SQL stamements.
 */

#include "phonebook_cpp.h"

#include <iostream>

#include <storage/data/environment.h>
#include <storage/data/row_set.h>
#include <storage/data/single_row.h>
#include <storage/data/single_field.h>
#include <storage/ittiadb/query.h>
#include <storage/ittiadb/table.h>
#include <storage/ittiadb/sequence.h>
#include <storage/ittiadb/transaction.h>

using ::storage::data::Environment;
using ::storage::data::Error;
using ::storage::data::RowSet;
using ::storage::data::SingleRow;
using ::storage::data::SingleField;
using ::storage::data::Field;
using ::storage::ittiadb::Connection;
using ::storage::ittiadb::Query;
using ::storage::ittiadb::Table;
using ::storage::ittiadb::Sequence;
using ::storage::ittiadb::Transaction;

#ifdef __embedded_cplusplus
#define cerr cout
#endif


/**
 * Helper function to print error messages.
 */
int print_error(Error info = Environment::error())
{
    if (DB_FAILED(info.code)) {
        std::cerr << "ERROR " << info.name << ": " << info.description << std::endl;
    }
    return info.code;
}

/**
 * Create database tables, assuming an empty database has been created.
 *
 * @return database error code
 *
 * Demonstrates:
 * - DB_SUCCESS macro
 * - DB_NOERROR status code
 */
int PhoneBook::create_tables(bool with_picture)
{
    if (DB_SUCCESS(create_table_contact(with_picture)) &&
        DB_SUCCESS(create_table_phone_number())) {
        // Success
        return DB_NOERROR;
    } else {
        // Table error
        return DB_ETABLE;
    }
}

/**
 * Create the table "contact", which lists contacts in the phone book.
 *
 * Demonstrates:
 * - defining table schema: fields and indexes
 * - unique and non-unique indexes
 * - add_datatype() functions return FieldDesc so they can be chained together
 * - add_index() functions return IndexDesc so they can be chained together
 */
int PhoneBook::create_table_contact(bool with_picture)
{
    int64_t result;

    //-------------------------------------------------------------------
    // Create the CONTACT table
    //   uint64         id
    //   nvarchar       name(50)
    //   uint64         ring_id
    //   varchar        picture_name(50)
    //   blob           picture
    //-------------------------------------------------------------------
    if (with_picture) {
        result = Query(db,
            "create table contact ("
            "  id uint64 not null,"
            "  name utf16str(50) not null,"
            "  ring_id uint64,"
            "  picture_name varchar(50),"
            "  picture blob,"
            "  constraint by_id primary key (id)"
            ")").execute();
    }
    else {
        result = Query(db,
            "create table contact ("
            "  id uint64 not null,"
            "  name utf16str(50) not null,"
            "  ring_id uint64,"
            "  picture_name varchar(50),"
            "  constraint by_id primary key (id)"
            ")").execute();
    }

    if (result == 0) {
        result = Query(db, "create index by_name on contact(name)").execute();
    }

    return result == 0 ? DB_NOERROR : print_error();
}

/**
 * Create the table "phone_number", which lists all known telephone numbers.
 */
int PhoneBook::create_table_phone_number()
{
    int64_t result;

    //-------------------------------------------------------------------
    // Create the phone_number table
    //   uint64         id
    //   nvarchar       name(50)
    //   uint64         ring_id
    //   varchar        picture_name(50)
    //   blob           picture
    //-------------------------------------------------------------------
    result = Query(db,
        "create table phone_number ("
        "  contact_id uint64 not null,"
        "  number ansistr(50) not null,"
        "  type uint64 not null,"
        "  speed_dial sint64,"
        "  constraint contact_ref foreign key (contact_id) references contact(id)"
        ")").execute();

    if (result == 0) {
        result = Query(db,
            "create index by_contact_id on phone_number(contact_id)"
            ).execute();
    }

    return result == 0 ? DB_NOERROR : print_error();
}

/**
 * Create sequences. Sequences are used to generate unique identifiers.
 *
 * Demonstrates:
 * - defining sequences
 */
int PhoneBook::create_sequences()
{
    Query q(db, "create sequence contact_id start with 1");
    //-------------------------------------------------------------------
    // Create CONTACT_ID sequence
    //-------------------------------------------------------------------
    return q.execute() == 0 ? DB_NOERROR : print_error();
}

/**
 * Open the database if it exists.
 *
 * @return database error code
 *
 * Demonstrates:
 * - opening a database
 * - the DB_FAILED macro
 */
int PhoneBook::open_database(int file_mode, const char* database_name)
{
    db_result_t result;
    if (file_mode == DB_FILE_STORAGE) {
        Connection::FileStorageConfig config;
        result = db.open(database_name, Connection::OpenExisting, config);
    }
    else if (file_mode == DB_MEMORY_STORAGE) {
        Connection::MemoryStorageConfig config;
        result = db.open(database_name, Connection::OpenExisting, config);
    }
    else {
        return DB_FAILURE;
    }

    if (result != DB_OK) {
        std::cerr << "Unable to open database: [" << database_name << "]." << std::endl;
        return print_error();
    }

    return DB_NOERROR;
}

/**
 * Create an empty database.
 *
 * Demonstrates:
 * - creation of an empty database
 * - StorageMode parameter
 */
int PhoneBook::create_database(int file_mode, const char* database_name)
{
    db_result_t result;
    if (file_mode == DB_FILE_STORAGE) {
        Connection::FileStorageConfig config;
        result = db.open(database_name, Connection::CreateNew, config);
    }
    else if (file_mode == DB_MEMORY_STORAGE) {
        Connection::MemoryStorageConfig config;
        config.memory_storage_size = MEMORY_STORAGE_SIZE;
        std::cout << "Creating " << config.memory_storage_size << " byte memory storage." << std::endl;
        result = db.open(database_name, Connection::CreateNew, config);
    }
    else {
        return DB_FAILURE;
    }

    //-------------------------------------------------------------------
    // Create a new empty database, overwriting existing files
    //-------------------------------------------------------------------
    if (result != DB_OK) {
        std::cerr << "Error creating new database: [" << database_name << "]." << std::endl;
        return print_error();
    }

    int rc;
    rc = create_tables(file_mode != DB_MEMORY_STORAGE);
    if (DB_FAILED(rc)) {
        return rc;
    }
    rc = create_sequences();
    if (DB_FAILED(rc)) {
        std::cerr << "Error creating sequences" << std::endl;
        return rc;
    }
    return DB_NOERROR;
}

/**
 * Close the database.
 *
 * @return database error code
 */
int PhoneBook::close_database()
{
    return db.isOpen() && DB_OK == db.close() ? DB_NOERROR : print_error();
}

/**
 * Insert a contact into the database.
 *
 * Demonstrates:
 * - use of a sequence
 * - opening of a table
 * - insert mode
 * - assigning data to a row
 * - posting data to the database
 * - closing a table
 * - inserting data into a BLOB field
 */
uint32_t PhoneBook::insert_contact(const char *name, uint32_t ring_id, const char *picture_name)
{
    Query q(db,
            "insert into contact (id, name, ring_id, picture_name) "
            "  values ($<integer>0, $<nvarchar>1, $<integer>2, $<varchar>3) "
            );
    Sequence id_sequence(db, "contact_id");
    db_seqvalue_t id;

    id_sequence.open();
    id_sequence.get_next_value(id);

    q.prepare();
    SingleRow params(q.parameters());
    params.at(0).set(id);
    params.at(1).set(name);
    params.at(2).set(ring_id);
    params.at(3).set(picture_name);

    if (1 != q.execute_with(params)) {
        //---------------------------------------------------------------
        // Error returned from execute
        //---------------------------------------------------------------
        print_error();
        id.int64 = 0;
    }

    return id.int64;
}

/**
 * Insert a phone entry into the database.
 */
void PhoneBook::insert_phone_number(uint32_t contact_id, const char *number, PhoneNumberType type, int32_t speed_dial)
{
    Query q(db,
        "insert into phone_number (contact_id,number,type,speed_dial) "
        "  values ($<integer>0, $<varchar>1, $<integer>2, $<integer>3) ");

    q.prepare();
    SingleRow params(q.parameters());
    params.at(0).set(contact_id);
    params.at(1).set(number);
    params.at(2).set<int32_t>(type);
    params.at(3).set(speed_dial);

    if (1 != q.execute_with(params)) {
        print_error();
    }
}

/**
 * Update an existing contact's name.
 *
 * Demonstrates:
 * - searching for existence of a record using an index
 * - edit mode
 */
void PhoneBook::update_contact_name(uint32_t id, const char *newname)
{
    Query q(db,
        "update contact "
        "  set name = $<nvarchar>1 "
        "  where id = $<integer>0 ");

    q.prepare();
    SingleRow params(q.parameters());
    params.at(0).set(id);
    params.at(1).set(newname);

    if (1 != q.execute_with(params)) {
        print_error();
    }
}

/**
 * Remove contact record from the database.
 *
 * Demonstrates:
 * - deleting a record
 * - parent/child removal
 */
void PhoneBook::remove_contact(uint32_t id)
{
    Query q(db,
        "delete from phone_number"
        "  where contact_id = $<integer>0");

    //---------------------------------------------------------------
    // Remove the corresponding recordss from the phone_number table.
    //---------------------------------------------------------------
    q.prepare();
    SingleRow params(q.parameters());
    params.at(0).set(id);

    if (0 > q.execute_with(params)) {
        print_error();
    }
    else {
        Query q2(db,
            "delete from contact"
            "  where id = $<integer>0");

        // Re-use parameters from the first query
        if (1 > q2.execute_with(params)) {
            print_error();
        }
    }
}

/**
 * Briefly list all contacts in the database.
 */
void PhoneBook::list_contacts_brief()
{
    Query q(db,
          "select id, name "
          "  from contact "
          "  order by name ");

    RowSet<> row_set;
    if (DB_OK == q.execute(row_set)) {
        for (RowSet<>::const_iterator iter = row_set.begin(); iter != row_set.end(); ++iter) {
            uint32_t id;
            std::string name;
            iter->at("id").get(id);
            iter->at("name").get(name);

            std::cout << id << '\t';
            std::cout << name << std::endl;
        }
    }
    return;
}

/**
 * List all contacts in the database with full phone numbers
 *
 * Demonstrates:
 * - parent/child relationships
 */
void PhoneBook::list_contacts(int sort)
{
    const char *cmd;
    uint64_t prev_id = 0;

    const char* query_by_name =
        "select A.id, A.name, A.ring_id, A.picture_name, B.number, B.type, B.speed_dial"
        "  from contact A, phone_number B"
        "  where A.id = B.contact_id"
        "  order by A.name, B.type";
    const char* query_by_id =
        "select A.id, A.name, A.ring_id, A.picture_name, B.number, B.type, B.speed_dial"
        "  from contact A, phone_number B"
        "  where A.id = B.contact_id"
        "  order by A.id, B.type";
    const char* query_by_ring_id_name =
        "select A.id, A.name, A.ring_id, A.picture_name, B.number, B.type, B.speed_dial"
        "  from contact A, phone_number B"
        "  where A.id = B.contact_id"
        "  order by A.ring_id, A.name, B.type";

    /* Choose the query for the selected sort order. */
    switch (sort) {
        case 0:
            cmd = query_by_id;
            break;
        case 1:
            cmd = query_by_name;
            break;
        case 2:
            cmd = query_by_ring_id_name;
            break;
        default:
            return;
    }

    Query q(db, cmd);

    RowSet<> row_set;
    if (DB_OK == q.execute(row_set)) {

        for (RowSet<>::const_iterator iter = row_set.begin(); iter != row_set.end(); ++iter) {
            uint32_t id;
            std::string name;
            std::string number;
            uint64_t type;
            int64_t speed_dial;

            iter->at("id").get(id);
            iter->at("name").get(name);
            const Field& ring_id_field = iter->at("ring_id");
            const Field& picture_name_field = iter->at("picture_name");
            iter->at("number").get(number);
            iter->at("type").get(type);
            iter->at("speed_dial").get(speed_dial);

            if  ( (uint64_t)id != prev_id ) {
                prev_id = id;
                std::cout << "Id: " << id << std::endl;
                std::cout << "Name: " << name << std::endl;

                if (!ring_id_field.is_null()) {
                    std::cout << "Ring tone id: " << ring_id_field.to<uint64_t>() << std::endl;
                }

                if (!picture_name_field.is_null()) {
                    std::cout << "Picture name: " << picture_name_field.to<std::string>() << std::endl;
                }
            }

            std::cout << "Phone number: " << number << " (";
            switch ((long)type) {
                case HOME:   { std::cout << "Home"; break; }
                case MOBILE: { std::cout << "Mobile"; break; }
                case WORK:   { std::cout << "Work"; break; }
                case FAX:    { std::cout << "Fax"; break; }
                case PAGER:  { std::cout << "Pager"; break; }
            }
            if (speed_dial >= 0) {
                std::cout << ", speed dial " << speed_dial;
            }

            std::cout << ")" << std::endl;
        }
    }
    return;
}

/**
 * Retrieve picture_name field from a contact
 */
std::string PhoneBook::get_picture_name(uint32_t id)
{
    //-------------------------------------------------------------------
    // Select a specific record from the contact table.
    //-------------------------------------------------------------------

    Query q(db,
            "select picture_name from contact where id = $<integer>0"
            );

    SingleField result;
    if (DB_OK == q.execute(result)) {
        return result.to<std::string>();
    }
    else {
        return std::string();
    }
}

/**
 * Start transaction
 */
void PhoneBook::tx_start()
{
    if (DB_OK != txn.begin()) {
        print_error();
    }
}

/**
 * Commit transaction
 */
void PhoneBook::tx_commit()
{
    if (DB_OK != txn.commit()) {
        print_error();
    }
}

