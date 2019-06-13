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

/** @file memory_disk_hybrid.c
 *
 * Command-line example program demonstrating the ITTIA DB Storage API.
 */

#include <stdlib.h>
#include <sstream> 
#include "storage/data/environment.h"
#include "storage/ittiadb/transaction.h"
#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/table.h"
#include "storage/ittiadb/query.h"
#include "storage/data/single_row.h"
#include "storage/data/single_field.h"
#include "db_iostream.h"

static const char* EXAMPLE_DATABASE = "memory_disk_hybrid.ittiadb";

/// Max Host name length
static const int32_t MAX_HOSTNAME_LEN = 50;
/// Max IP name Length
static const int32_t MAX_IP_LEN = 16;
/// Hostname count
static const int32_t  EMULATE_HOSTSNAMES_COUNT = 230;
/// Max allowed client requests
static const int32_t CLIENT_REQUESTS = 1460;

typedef enum {
    /// In memory 
    IN_MEM  = 0,
    /// On Disk 
    ON_DISK = 1,
    CT_GUARD   = 2
} cache_type_t;

/// Caches sizes. - To monitor if cache should be shaped a bit
static int64_t cache_sizes[ CT_GUARD ] = { 0, 0 };
/// Limits to count of records into in-mem (500) and on-disk (10000) caches
static int64_t cache_size_limits[ CT_GUARD ] = { 100, 220 };

/// When cache_sizes[x] limit reaches cache_size_limits[x], delete cache_del_chunks[x] count of the
/// most aged records from cache 'x'
static int cache_del_chunks[ CT_GUARD ] = { 1, 100 };


/// Class to manage cache operations on hybrid database
class ManageHybridDatabase
{
public:
    /// Create an instance of ManageHybridDatabase with given connection
    ///
    /// @param connection
    ///  A database connection object. The connection must be opened before
    ///  any member functions can be used.
    ManageHybridDatabase(storage::ittiadb::Connection& database);
    /// Destructor
    ~ManageHybridDatabase();
    /// Main entry points for clients.
    ///     - Search for hostname(in order) :
    ///     -inside in - mem cache.If found get cache record and return ip addres.
    ///      Increment request count statistics;
    ///     -inside on 
    ///         - disk cache.If found copy found cache record to in 
    ///         - mem cache.Increment inmem request count /s tatistics;
    ///     -If search both chaches failed, request 'dns-server' (fake generator in this example) to resolve.
    ///     Put resolved data in both in - mem & on - disk caches;
    /// @param hostname
    /// A host name
    /// @param ip
    /// A host ip address
    int resolve_ip_by_hostname(const std::string& hostname, std::string& ip);
    /// Copy the most recent cache records into in-mem cache ( on application start )
    int sdb_copy_disk_to_mem_cache();
    /// When application starts copy the most recent cache records into in-mem cache
    int sdb_merge_mem_cache_to_disk();
private:
    int sdb_put_into_cache(cache_type_t ctype, storage::data::Row& data);
    /// Search requested cache type for hostname and update statistics, if found.        
    int sdb_search_cache(cache_type_t ctype, const std::string& hostname, storage::data::Row& result);
    int sdb_shape_cache(cache_type_t ctype);

private:
    storage::ittiadb::Connection& db;
};
/// Example Data generators 
/// Emulate DNS-server side for our example
class DataGenerator
{
public:
    /// Generate names of form "host_a".."host_z", "host_aa".."host_zz"
    static void generate_hostname(char *hname);
    /// For simplicity of our example, suppose dns request has always successfull result, and each request
    /// always returns different ip - addreses if even hostname is the same.
    static void do_dns_server_request(const std::string& hostname, std::string& ip);
};

int
example_main(int argc, char *argv[])
{
    db_result_t db_rc = DB_OK;
    int rc = DB_NOERROR;
    int64_t rows_affected =-1;

    const char * database_uri = EXAMPLE_DATABASE;
    if (argc > 1) {
        database_uri = argv[1];
    }

    storage::ittiadb::Connection database(database_uri);

    // Init storage configuration with default settings
    storage::ittiadb::Connection::FileStorageConfig file_storage_config;
    // Create database with custom page size;
    file_storage_config.memory_page_size = DB_DEF_PAGE_SIZE * 4;
    file_storage_config.memory_storage_size = file_storage_config.memory_page_size * 100;

    // Open if exist or Create a new database file
    db_rc = database.open(storage::ittiadb::Connection::OpenAlways, file_storage_config);
    GET_ECODE(rc, db_rc, "Failed to open database ");

    //Create hosts disk table
    if ((DB_NOERROR == rc) 
        && (false == storage::ittiadb::Table(database,"hosts_dsk").exists())) {
        GET_ECODE(rc, db_rc);
        rows_affected = storage::ittiadb::Query(database,
            "CREATE TABLE hosts_dsk("
            "hostname ansistr(50) NOT NULL,"
            "hostip ansistr(16) NOT NULL,"
            "requestcount sint32 NOT NULL,"
            "age sint64 NULL,"
            "CONSTRAINT hosts_pkey PRIMARY KEY(hostname))"
            ).execute();

        GET_ECODE(rc, rows_affected, "Create 'hosts_dsk' table ");
        //Create multiset index for hosts disk table
        if (DB_NOERROR == rc)
        {
            rows_affected = storage::ittiadb::Query(database,
                "CREATE INDEX hosts_age_idx ON hosts_dsk (age)"
                ).execute();

            GET_ECODE(rc, rows_affected, "Create host_age_idx index to'hosts_dsk' table ");
        }
    }
    
    //Create hosts memory table
    if (DB_NOERROR == rc 
        && (false == storage::ittiadb::Table(database, "hosts_mem").exists())) {
        rows_affected = storage::ittiadb::Query(database,
            "CREATE MEMORY TABLE hosts_mem("
            "hostname ansistr(50) NOT NULL,"
            "hostip ansistr(16) NOT NULL,"
            "requestcount sint32 NOT NULL,"
            "age sint64 NULL,"
            "CONSTRAINT hosts_pkey PRIMARY KEY(hostname))"
            ).execute();

        GET_ECODE(rc, rows_affected, "Create 'hosts_mem' table ");
        //Create multiset index for hosts memory table
        if (DB_NOERROR == rc)
        {
            rows_affected = storage::ittiadb::Query(database,
                "CREATE INDEX hosts_age_idx ON hosts_mem (age)"
                ).execute();

            GET_ECODE(rc, rows_affected, "Create host_age_idx index to'hosts_mem' table ");
        }
    }

    if (DB_NOERROR == rc)
    {
        ManageHybridDatabase mhd(database);
        rc = mhd.sdb_copy_disk_to_mem_cache();

        // Create Sequence
        if (DB_NOERROR == rc)
        {
            rows_affected = storage::ittiadb::Query(database,
                "CREATE SEQUENCE age_seq START WITH 1").execute();
            GET_ECODE(rc, rows_affected, "Create age_seq sequence ");
        }

        int i = 0;
        // Emulate clients' requests
        for (; i < CLIENT_REQUESTS && (DB_NOERROR == rc); ++i) {
            char hostname[MAX_HOSTNAME_LEN + 1]="";
            //char ip[MAX_IP_LEN + 1];
            std::string ip;
            DataGenerator::generate_hostname(hostname);

            rc = mhd.resolve_ip_by_hostname(hostname, ip);

            std::cout << i << ". Hostname: " << hostname << ", ip: " << ip <<
                ", cache_sizes(" << cache_sizes[IN_MEM] << "," << cache_sizes[ON_DISK]
                << ") rc:" << rc << std::endl;
        }

        rc = mhd.sdb_merge_mem_cache_to_disk();

        // Commit the transaction if active and no error
        if (DB_NOERROR == rc && storage::ittiadb::Transaction(database).isActive())
        {
            storage::ittiadb::Transaction(database).commit();   
        }
        // Drop sequence
        if (DB_NOERROR == rc)
        {
            rows_affected = storage::ittiadb::Query(database,
                "DROP SEQUENCE age_seq").execute();
            GET_ECODE(rc, rows_affected, "Create age_seq sequence ");
        }
    }
    else
    {
        std::cerr << "Couldn't load in-mem cache data" <<std::endl;
    }

    if (DB_NOERROR != rc)
    {
        database.close();
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return (DB_NOERROR != rc)? EXIT_FAILURE: EXIT_SUCCESS;
}

void
DataGenerator::generate_hostname(char *hname)
{
    int i = 0;
    static int hidx = 0;
    int hidx_cur;
    int suffix_len = 1;

    hidx %= EMULATE_HOSTSNAMES_COUNT;

    if ((int)(hidx / 26 / 26)) {
        hidx = 0;
        suffix_len = 1;
    }
    else if ((int)(hidx / 26)) {
        suffix_len = 2;
    }
    else {
        suffix_len = 1;
    }

    strncpy(hname, "host_", MAX_HOSTNAME_LEN);

    hidx_cur = hidx++;
    for (; i < suffix_len; ++i, hidx_cur /= 26) {
        hname[5 + i] = 65 + hidx_cur % 26;
    }
    hname[5 + suffix_len] = 0;
}

void
DataGenerator::do_dns_server_request(const std::string& hostname, std::string& ip)
{
    static int ip_idx = 0;
    std::ostringstream ss;
    ss  << 192
        << "." << (168 + ip_idx++ % 80) 
        << "." << (250 - ip_idx % 242) 
        << "." << (1 + ip_idx % 250);
   // strncpy(ip, ss.str().c_str(), MAX_IP_LEN);
    ip = ss.str();
}

ManageHybridDatabase::ManageHybridDatabase(storage::ittiadb::Connection& database)
    :db(database)
{
    //Empty
}

ManageHybridDatabase::~ManageHybridDatabase()
{
    //Empty
}

int
ManageHybridDatabase::resolve_ip_by_hostname(const std::string& hostname, std::string& ip)
{
    int rc = DB_FAILURE;    
    storage::data::SingleRow cache_row(storage::ittiadb::Table(db, "hosts_dsk").columns());

    // Search in-mem cahce. Searching in-mem cache automatically increase 
    // in-mem statistics (age & requests count)
    rc = sdb_search_cache(IN_MEM, hostname, cache_row);

    if (DB_NOERROR != rc) {
        if (rc == DB_ENOTFOUND) {
            rc = sdb_search_cache(ON_DISK, hostname, cache_row);

            if (DB_NOERROR == rc) {
                // If on-disk cache contains data requested, copy this record to
                // in-mem cache for late usage by clients
                sdb_put_into_cache(IN_MEM, cache_row);
            }
            else if (DB_ENOTFOUND == rc) {
                DataGenerator::do_dns_server_request(hostname, ip);
                cache_row["hostname"].set(hostname);
                cache_row["hostip"].set(ip);
                cache_row["requestcount"].set(1);
                storage::data::SingleField next_seq_field;
                storage::ittiadb::Query(db, 
                    "SELECT NEXT VALUE FOR age_seq").execute(next_seq_field);
                int64_t next_seq_value;
                next_seq_field.get(next_seq_value);
                cache_row["age"].set(next_seq_value);                 
                rc = sdb_put_into_cache(ON_DISK, cache_row);
            }
        }
    }

    if (DB_NOERROR == rc) {
        cache_row["hostip"].get(ip);
    }
    return rc;
}

int
ManageHybridDatabase::sdb_copy_disk_to_mem_cache()
{
    int rc = DB_NOERROR;
    db_result_t db_rc = DB_OK;
    int64_t count2cpy = cache_sizes[IN_MEM];
    storage::ittiadb::Table t_dsk(db, "hosts_dsk");
    storage::ittiadb::Table t_mem(db, "hosts_mem");
    //Open Hosts disk table
    db_rc = t_dsk.open("hosts_age_idx", storage::ittiadb::Table::Exclusive);
    GET_ECODE(rc, db_rc, "Open hosts_dsk table ");
    storage::data::RowSet<> rows_dsk(t_dsk.columns());
    if (DB_NOERROR == rc)
    {
        db_rc = t_dsk.fetch(rows_dsk);
        GET_ECODE(rc, db_rc, "Fetch row set from hosts_dsk table ");
    }
    if (DB_NOERROR == rc) {
        // Open hosts memory table
        db_rc = t_mem.open(storage::ittiadb::Table::Exclusive);
        GET_ECODE(rc, db_rc, "Open hosts_mem table ");
        //Copy fetched disk rows to memory table
        storage::data::RowSet<>::iterator itr = rows_dsk.begin();
        for (; (itr != rows_dsk.end()) && (DB_OK == db_rc); ++itr)
        {
            ++cache_sizes[ON_DISK];
            if (--count2cpy > 0)
            {
                db_rc = t_mem.insert(*itr);
                if (DB_OK == db_rc)
                {
                    ++cache_sizes[IN_MEM];
                }
            }
        }
        GET_ECODE(rc, db_rc, "while copying from on-disk to in-mem caches ");
    }

    return rc;
}

int
ManageHybridDatabase::sdb_merge_mem_cache_to_disk()
{
    int rc = DB_NOERROR;
    db_result_t db_rc = DB_OK;
    int upd_cnt = 0;
    int ins_cnt = 0;

    storage::ittiadb::Table t_dsk(db, "hosts_dsk");
    storage::ittiadb::Table t_mem(db, "hosts_mem");
    db_rc = t_mem.open("hosts_age_idx", storage::ittiadb::Table::Exclusive);
    GET_ECODE(rc, db_rc, "Open hosts_mem table with hosts_age_idx: ");
    storage::data::RowSet<> rows_mem(t_mem.columns());
    if (DB_NOERROR == rc)
    {
        db_rc = t_mem.fetch(rows_mem);
        GET_ECODE(rc, db_rc, "Fetching rowset from hosts-mem table ");
    }
    if (DB_NOERROR == rc)
    {
        //Open disk table
        db_rc = t_dsk.open("hosts_pkey", storage::ittiadb::Table::Exclusive);
        GET_ECODE(rc, db_rc, "Opening hosts_dsk table with hosts_pkey: ");
    }
    //Copy fetched mem rows to disk table
    storage::data::RowSet<>::iterator itr = rows_mem.begin();
    storage::data::SingleRow single_row(t_dsk.columns());
    for (; (itr != rows_mem.end()) && (DB_NOERROR == rc); ++itr)
    {        
        db_rc = t_dsk.search_by_index(DB_SEEK_FIRST_EQUAL, *itr, single_row);
        GET_ECODE(rc, db_rc, NULL);
        if (DB_NOERROR == rc)
        {
            db_rc = t_dsk.update_by_index(single_row, *itr);
            GET_ECODE(rc, db_rc, "Udating from in-mem to on-disk  ");
            upd_cnt++;
        }
        else if (DB_ENOTFOUND == rc)
        {
            db_rc = t_dsk.insert(*itr);
            GET_ECODE(rc, db_rc, "Inserting from in-mem to on-disk  ");
            ins_cnt++;
        }        
    }
    std::cout << "Mem->Disk merge result (error_code, inmem_size, inserted, updated): ("
        << rc << "," << rows_mem.size() << "," << ins_cnt << "," << upd_cnt << ")" << std::endl;
    return rc;
}

int
ManageHybridDatabase::sdb_put_into_cache(cache_type_t ctype, storage::data::Row& data)
{
    int rc = DB_NOERROR;
    db_result_t db_rc=DB_OK;
    const char * table_name = (ctype == IN_MEM) ? "hosts_mem" : "hosts_dsk";
    if (cache_sizes[ctype] > cache_size_limits[ctype]) {
        (void)sdb_shape_cache(ctype);
    }
    storage::ittiadb::Table table(db,table_name);
    db_rc = table.open(storage::ittiadb::Table::Exclusive);
    GET_ECODE(rc, db_rc, "sdb_put_into_cache: Open table ");
    if (DB_NOERROR == rc)
    {
        db_rc = table.insert(data);
        GET_ECODE(rc, db_rc, "sdb_put_into_cache: Couldn't put data into cache ");
        if (DB_NOERROR == rc)
        {
            static int commits = 0;
            cache_sizes[ctype]++;
            if (ON_DISK == ctype && 0 == commits++ % 50) {
                //commit transaction
                storage::ittiadb::Transaction(db).commit();
            }
        }
    }
    return rc;
}

int
ManageHybridDatabase::sdb_search_cache(cache_type_t ctype, const std::string& hostname, storage::data::Row& result)
{
    int rc = DB_NOERROR;
    db_result_t db_rc = DB_OK;
    const char * table_name = ctype == IN_MEM ? "hosts_mem" : "hosts_dsk";
    storage::ittiadb::Table table(db, table_name);
    db_rc = table.open("hosts_pkey", storage::ittiadb::Table::Exclusive);
    GET_ECODE(rc, db_rc, "sdb_search_cache: Opening Table ");
    storage::data::SingleRow key(table.columns());
    key["hostname"].set(hostname.c_str());
    if (DB_NOERROR == rc)
    {
        db_rc = table.search_by_index(DB_SEEK_FIRST_EQUAL, key, 1,result);
        GET_ECODE(rc, db_rc);
    }
    if (DB_NOERROR == rc && ctype == IN_MEM)
    {
        int32_t reqcount;
        int64_t next_seq_value;
        result["requestcount"].get(reqcount);
        storage::data::SingleField next_seq_field;
        storage::ittiadb::Query(db,
            "SELECT NEXT VALUE FOR age_seq").execute(next_seq_field);        
        next_seq_field.get(next_seq_value);
        result["age"].set(next_seq_value);
        db_rc = table.update_by_index(key, 1,result);
        GET_ECODE(rc, db_rc, "Updating result row ");
    }
    return rc;
}

int
ManageHybridDatabase::sdb_shape_cache(cache_type_t ctype)
{
    int rc = DB_NOERROR;
    db_result_t db_rc = DB_OK;
    const char * table_name = ctype == IN_MEM ? "hosts_mem" : "hosts_dsk";
    int count2del = cache_del_chunks[ctype];
    std::cout << "Delete " << count2del << " rows from " 
        << (ctype == IN_MEM ? "IN-MEM" : "ON-DISK") << " cache" << std::endl;
    storage::ittiadb::Table table(db, table_name);
    db_rc = table.open("hosts_age_idx", storage::ittiadb::Table::Exclusive);
    GET_ECODE(rc, db_rc, "sdb_shape_cache: Opening Table ");
    storage::data::SingleRow first_row(table.columns());
    for (; (DB_NOERROR == rc) && (count2del); --count2del)
    {
        db_rc = table.fetch(first_row);
        GET_ECODE(rc, db_rc,"Fetching first row to delete ");
        if (DB_NOERROR == rc)
        {
            db_rc = table.delete_by_index(first_row,1);
            GET_ECODE(rc, db_rc, "Occured when deleting cache records ");
            cache_sizes[ctype]--;
        }
    }

    return rc;
}
