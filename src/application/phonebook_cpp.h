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


/** @file
 *
 * Console example program demonstrating the ITTIA DB SQL C++ Storage API.
 */

#ifndef PHONEBOOK_CPP_H
#define PHONEBOOK_CPP_H

#include <storage/ittiadb/connection.h>
#include <storage/ittiadb/transaction.h>


// Use a local database file.
const char * const DATABASE_NAME_LOCAL    = "phone_book.ittiadb";
// Use the IPC client protocol to access database through dbserver.
const char * const DATABASE_NAME_SERVER   = "idb+tcp://localhost/phone_book.ittiadb";

// Use 128KiB of RAM for memory storage, when selected.
const int64_t MEMORY_STORAGE_SIZE   = 128 * 1024;

/**
 * A list of telephone contacts stored on a mobile phone
 */
class PhoneBook {
private:
    ::storage::ittiadb::Connection db;
    ::storage::ittiadb::Transaction txn;

public:

    /**
     * Types of telephone numbers
     */
    enum PhoneNumberType {
        HOME = 0,
        MOBILE,
        WORK,
        FAX,
        PAGER
    };

    /**
     * Types of phone call events
     */
    enum CallLogType {
        SENT,
        RECEIVED,
        MISSED
    };

private:

    int create_tables(bool with_picture);
    int create_table_contact(bool with_picture);
    int create_table_phone_number();
    int create_sequences();

public:
    PhoneBook()
        : db()
        , txn(db)
    {
    }

    int open_database(int file_mode, const char* database_name);
    int create_database(int file_mode, const char* database_name);
    int close_database();

    uint32_t insert_contact(const char * name, uint32_t ring_id, const char *picture_name);
    void insert_phone_number(uint32_t contact_id, const char *number, PhoneNumberType type, int32_t speed_dial);

    void update_contact_name(uint32_t id, const char * newname);
    void remove_contact(uint32_t id);

    void list_contacts_brief();
    void list_contacts(int sort);

    std::string get_picture_name(uint32_t id);

    void tx_start();
    void tx_commit();
};


#endif

