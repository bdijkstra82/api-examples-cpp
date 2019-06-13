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

/// @file full_memory_storage.cpp
///
/// Command-line example program demonstrating the ITTIA DB Storage API.
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

#include <stdio.h>

static const char* EXAMPLE_DATABASE = "full_memory_storage.ittiadb";

/// Max Host name length
static const int32_t MAX_HOSTNAME_LEN = 50;
/// Max IP name Length
static const int32_t MAX_IP_LEN = 16;
// Synthetic data generator definitions:
///< Count of unique host IPs which example data provider can generate
static const int32_t HOSTS_COUNT =1000;
///< Count of unique ports which example data provider can generate
static const int32_t PORTS_COUNT =10;

/// Input/output statistics for remote host connection
// This is a structure which 'external' (to this module)
// part of software uses to exchange data with this module
typedef struct
{
    char hostip[MAX_IP_LEN];    ///< IP of remote host
    int  dport;    ///< remote connection port
    int  sport;    ///< local connection port
    int64_t  io_bytes;  ///< in + out bytes transfered
} io_stat_row_t;

/// @brief Manages Memory cache for Memory DB schema to use in 'full_memory_storage' examples.
///
///  This examples supposed to be by part of some software which collects network information
///  about TCPIPv4 connections of some client host.
///
///  DB contais two tables :
///   -'hosts' table holds summary information about connected hosts :
///     -ipv4 address;
///     -hostname;
///     -input / output statistics;
///     -connections count;
///     
///   -'connstat' table holds detailed information about connections :
///     -source port;
///     -detination port;
///     -input / output statistics;
///     
///  Hosts in 'hosts' table related by foreign key from 'connstat' table.
///In this example we:
///
/// - Create a memory storage that is smaller than the amount of data that will be inserted.
/// - Insert into a memory table many times in separate transactions.
/// - When memory storage becomes full, delete some old records and try inserting again.
class ManageMemoryCache
{
public:
    /// Create an instance of ManageMemoryCache with given connection
    ///
    /// @param connection
    ///  A database connection object. The connection must be opened before
    ///  any member functions can be used.
    explicit ManageMemoryCache(storage::ittiadb::Connection& connection);

    /// Destructor, 
    ~ManageMemoryCache();

    /// Increment connection in/out statistics by io_stat.io_bytes count
    int  sdb_inc_io_stat(const io_stat_row_t& io_stat);
    /// Example data generator
    void generate_iostat_row(io_stat_row_t& r, int solt);

private:
    /// Remove 'count' of the most old/aged records from 'hosts' table.
    /// Recalc 'hosts_rows_count' & connstat_rows_count statistics.
    int sdb_remove_old_hosts(int count);
    /// Append record into 'hosts' table
    int sdb_add_host(const char *hostip, int64_t iostat, int32_t& hostid);
    /// Lookup 'hosts' table to find out 'hostid' by 'hostip'.    
    int sdb_find_host_by_ip(const char * hostip, int32_t& hostid);
    /// Increment summary host statistics. - Just update 'hosts' record.    
    int sdb_inc_host_stat_(int32_t hostid, int64_t new_age, int32_t ccount_delta, int64_t iostat_delta);

    /// Removes 'count' of the most aged 'connstat' records.
    /// Corrects summary hosts statistics.
    int sdb_remove_old_conns(int count);
    ///Insert/Update 'connstat' table with 'stat' data.
    int sdb_inc_conn_stat(int32_t hostid, const io_stat_row_t &stat);

private:
    /// Reference to database connection
    storage::ittiadb::Connection& db;
    ///Records count in 'hosts' table.
    //We use it to estimate when
    //our cache(in it's 'hosts' part) is getting too big.
    //If hosts_rows_count >= HOSTS_DB_LIMIT, then we remove
    //OLD_HOSTS_REMOVE_CHUNK from 'hosts' table.
    int hosts_rows_count;
    ///Records count in 'connstat' table.
    //We use it to estimate when
    //our cache(in it's 'connstat' part) is getting too big:
    //If connstat_rows_count >= CONNSTAT_DB_LIMIT, then we remove
    //OLD_CONNS_REMOVE_CHUNK from 'connstat' table.
    int connstat_rows_count;

    /// Maximum records allowed to hold by "hosts" table @sa hosts_rows_count
    const int32_t HOSTS_DB_LIMIT;
    /// When HOSTS_DB_LIMIT reached drop this count of old hosts rows @sa hosts_rows_count
    const int32_t OLD_HOSTS_REMOVE_CHUNK;
    /// Maximum records we allow to store in "connstat" table @sa connstat_rows_count
    const int32_t CONNS_DB_LIMIT;
    /// When CONNS_DB_LIMIT reached drop this count of old connstat rows @sa connstat_rows_count
    const int32_t OLD_CONNS_REMOVE_CHUNK;
};

int
example_main(int argc, char* argv[])
{
    int rc;
    int64_t rows_affected;

    const char * database_uri = EXAMPLE_DATABASE;
    if (argc > 1) {
        database_uri = argv[1];
    }

    storage::ittiadb::Connection database(database_uri);

    /// Init storage configuration with default settings
    storage::ittiadb::Connection::MemoryStorageConfig memory_storage_config;
    /// Create database with custom page size;
    memory_storage_config.memory_page_size = DB_DEF_PAGE_SIZE * 4 /*DB_MIN_PAGE_SIZE*/;
    memory_storage_config.memory_storage_size = memory_storage_config.memory_page_size * 100;
    /// Create a new database file
    if(DB_NOERROR != GET_ECODE(rc, 
        database.open(storage::ittiadb::Connection::CreateAlways, memory_storage_config),
        "Database open failed "))
    {
        return EXIT_FAILURE;
    }

    //create hosts table
    rows_affected = storage::ittiadb::Query(database, 
        "CREATE TABLE hosts("
        "hostid sint32 NOT NULL,"
        "hostip ansistr(16) NOT NULL,"
        "hostname ansistr(50) NULL,"
        "iostat sint64 NULL,"
        "conncount sint32 NOT NULL,"
        "age sint64 NULL,"
        "CONSTRAINT hosts_pkey PRIMARY KEY(hostid),"
        "CONSTRAINT hosts_ip_idx UNIQUE(hostip))"
        ).execute();
    //create multiset index for hosts table
    rows_affected = (rows_affected >= 0) ? 
        storage::ittiadb::Query(database, 
        "CREATE INDEX hosts_age_idx ON hosts (age)"
        ).execute() : rows_affected;

    if (rows_affected < 0)
    {
        std::cerr << "Failed to create hosts table" << std::endl;
        return EXIT_FAILURE;
    }
    //Create Connecton statastics table ('connstat')
    rows_affected = storage::ittiadb::Query(database, 
        "CREATE TABLE connstat(" 
        "hostid sint32 NOT NULL,"
        "dport sint32 NOT NULL," 
        "sport uint32 NOT NULL," 
        "iostat uint64 NULL,"
        "age sint32 NULL,"
        "CONSTRAINT cs_pkey PRIMARY KEY(hostid, dport, sport),"
        "CONSTRAINT cs_hostid_fkey FOREIGN KEY(hostid) references hosts(hostid))"
        ).execute();
    //create multiset index
    rows_affected = (rows_affected >= 0) ?
        storage::ittiadb::Query(database, 
        "CREATE INDEX cs_by_age ON connstat (age)"
        ).execute() : rows_affected;

    if (rows_affected < 0)
    {
        std::cerr << "Failed to create constat table" << std::endl;
        return EXIT_FAILURE;
    }
    // Create Sequence
    rows_affected = storage::ittiadb::Query(database, "CREATE SEQUENCE age_seq START WITH 1").execute();
    if (rows_affected < 0)
    {
        std::cerr << "Failed to create age_seq sequence " << std::endl;
        return EXIT_FAILURE;
    }
    // Start traffic statistics generation
    int i = 0;
    io_stat_row_t r;
    ManageMemoryCache cacheMgr(database);
    rc = DB_NOERROR;
    for (i = 0; (i < 50000) && (DB_NOERROR == rc); ++i) {
        //io_stat_row_t r;
        cacheMgr.generate_iostat_row(r, i);
        rc = cacheMgr.sdb_inc_io_stat(r);
    }
    if (DB_NOERROR != rc)
    {
        std::cerr << " Stat collection failed" << std::endl;
        return EXIT_FAILURE;
    }

    //Drop sequence
    rows_affected = storage::ittiadb::Query(database, "DROP SEQUENCE age_seq").execute();
    if (rows_affected < 0)
    {
        std::cerr << "Failed to drop age_seq" << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

   return rc;
}

ManageMemoryCache::ManageMemoryCache(storage::ittiadb::Connection& database)
    :db(database)
    , hosts_rows_count(0)
    , connstat_rows_count(0)
    , HOSTS_DB_LIMIT(800)
    , OLD_HOSTS_REMOVE_CHUNK(10)
    , CONNS_DB_LIMIT((HOSTS_DB_LIMIT * 4))
    , OLD_CONNS_REMOVE_CHUNK(20)
{
    //Empty
}

ManageMemoryCache::~ManageMemoryCache() 
{
    //Empty
}

int 
ManageMemoryCache::sdb_remove_old_hosts(int count)
{
    int rc = DB_NOERROR;
    int32_t ccount;
    int32_t hostid;

    std::cout << "(remove_old_hosts):About to delete " << count
        << "/" << hosts_rows_count << "hosts" << std::endl;

    storage::ittiadb::Table table(db, "hosts");

    GET_ECODE(rc, table.open("hosts_age_idx"), "Couldn't open the host table");
    
    for (; count && (DB_NOERROR == rc); --count) {
        //Fetch (from record about to be deleted) current connections count, to
        //recalculate our global 'connstat_rows_count' statistics.
        storage::data::SingleRow first_row(table.columns());
        GET_ECODE(rc, table.fetch(first_row),"Couldn't fetch hosts table while deleting hosts");

       if( (DB_NOERROR == rc) && (DB_NOERROR == GET_ECODE(rc, table.delete_by_index(first_row,1), 
                                     "Couldn't delete row from hosts table") ))
        {
            // Delete & decrement [hosts|connstat]_rows_count
            --hosts_rows_count;
            // Deletes from hosts table do cascade deletion ( by cascade detete fkey )
            // from 'connstat' table, so correct 'connstat_rows_count' too
            first_row["hostid"].get(hostid);
            first_row["conncount"].get(ccount);
            connstat_rows_count -= ccount;
            std::cout << "(remove_old_hosts):(hostid, ccount): ("
                << hostid << "," << ccount << ") " << first_row << std::endl;
        }
    }
    return rc;
}

int 
ManageMemoryCache::sdb_add_host(const char *hostip, int64_t iostat, int32_t& hostid)
{
    int rc = DB_NOERROR;
    //row count in table exceeding max limit
    if (HOSTS_DB_LIMIT <= hosts_rows_count) {
        rc = sdb_remove_old_hosts(OLD_HOSTS_REMOVE_CHUNK);
    }
    if (DB_NOERROR == rc)
    {
        storage::ittiadb::Table table(db, "hosts");

        GET_ECODE(rc, table.open(storage::ittiadb::Table::Exclusive), "(add_host): Error opening table:");

        storage::ittiadb::Query insert(db, 
            "INSERT INTO " 
            "hosts(age, hostid, hostip, iostat,conncount) " 
            "VALUES(NEXT VALUE FOR age_seq, CURRENT VALUE FOR age_seq,?,?,0)"
            );

        if ((DB_NOERROR == rc)
            && (DB_NOERROR == GET_ECODE(rc, insert.prepare(),
                                "(add_host): Error preparing row insert Query: ")))
        {
            int64_t rows_affected = 0;

            storage::data::SingleRow param_row(insert.parameters());
            param_row[0].set(hostip);//hostip
            param_row[1].set(iostat);//iostat

            rows_affected = insert.execute_with(param_row);
            if (1 != rows_affected)
            {
                const storage::data::Error& error = storage::data::Environment::error();
                std::cerr << "(add_host):Couldn't insert hosts table record: " << error << std::endl;
                rc = error.code;
            }
            else
            {
                hosts_rows_count++;
                //Generate age and host id from sequence
                //get current value for host id                
                storage::data::SingleField field;
                storage::ittiadb::Query(db, "SELECT CURRENT VALUE FOR age_seq").execute(field);
                field.get(hostid);
            }
            std::cout << "(add_host): Add Host_" << hosts_rows_count
                << ": (hostid,age,ip,iostat) = (" << hostid << "," << hostid
                << "," << hostip << "," << iostat << ")" 
                /*<< " param_row (ip,iostat) =(" << param_row << ")"*/ << std::endl;
        }

    }
    return rc;
}

int 
ManageMemoryCache::sdb_find_host_by_ip(const char * hostip, int32_t& hostid)
{
    int rc = DB_NOERROR;
    db_result_t db_rc = DB_OK;
    storage::ittiadb::Table table(db, "hosts");
    db_rc = table.open("hosts_ip_idx");
    GET_ECODE(rc, table.open("hosts_ip_idx"), "(find_host_by_ip):Open hosts table:");
    if (DB_NOERROR == rc)
    {
        storage::data::SingleRow key(table.columns());
        storage::data::SingleRow data(table.columns());
        key["hostip"].set(hostip);
        if (DB_NOERROR == 
            GET_ECODE(rc, table.search_by_index(DB_SEEK_FIRST_EQUAL, key, 1, data)))
        {
            //Populate host id from the row found 
            data["hostid"].get(hostid);
        }
    }
    if (DB_NOERROR != rc && DB_ENOTFOUND != rc)
    {
        std::cerr << " sdb_find_host_by_ip failed for find row [" 
            << hostip<< "," <<hostid <<"] with rc = "<<rc << std::endl;
    }

    return rc;
}

int 
ManageMemoryCache::sdb_inc_host_stat_(int32_t hostid, int64_t new_age, int32_t ccount_delta, int64_t iostat_delta)
{
    int rc = DB_NOERROR;
    int32_t ccount;
    int64_t iostat;
    storage::ittiadb::Table table(db, "hosts");
    GET_ECODE(rc, table.open("hosts_pkey", storage::ittiadb::Table::Exclusive),
                           "(inc_host_stat_): FAiled to open hosts table: ");
    storage::data::SingleRow key(table.columns());
    storage::data::SingleRow data(table.columns());
    key["hostid"].set(hostid);

    if ((DB_NOERROR == rc) 
        && (DB_NOERROR == GET_ECODE(rc, table.search_by_index(DB_SEEK_FIRST_EQUAL, key, 1, data),
                                    " (inc_host_stat_):Couldn't search hosts table with KEY:  ")))
    {
        data["conncount"].get(ccount);
        data["iostat"].get(iostat);
        if (new_age > 0)
        {
            data["age"].set(new_age);
        }
        data["conncount"].set(ccount + ccount_delta);
        data["iostat"].set(iostat + iostat_delta);
        if (DB_NOERROR == GET_ECODE(rc, table.update_by_index(key, 1, data), 
                            "(inc_host_stat_):Couldn't update hosts.conncount column "))
        {
            std::cout << " (inc_host_stat_):update hosts.conncount column updated " 
                << data << std::endl;
        }
    }

    return rc;
}

int 
ManageMemoryCache::sdb_remove_old_conns(int count)
{
    int rc = DB_NOERROR;
    int32_t hostid;
    int64_t iostat;
    std::cout << "(remove_old_conns): About to delete " << count << "/"
        << connstat_rows_count << " connstat record(s)" << std::endl;
    storage::ittiadb::Table table(db, "connstat");
    GET_ECODE(rc, table.open("cs_by_age"),"(remove_old_conns):Couldn't open table");

    storage::data::SingleRow first_row(table.columns());
    for (; count && DB_NOERROR == rc; --count) {

        if(DB_NOERROR == GET_ECODE(rc, table.fetch(first_row),
            "(remove_old_conns):Couldn't fetch row from table"))
        {
            first_row["hostid"].get(hostid);
            first_row["iostat"].get(iostat);
            // Decrement summary statistics of host who owns this connection
            rc = sdb_inc_host_stat_(hostid, -1, -1, -iostat);

            if ((DB_NOERROR == rc)
                && (DB_NOERROR == GET_ECODE(rc, table.delete_by_index(first_row,1),
                "(remove_old_conns):Couldn't remove record(s) from connstat table ")))
            {
                connstat_rows_count--;
            }
            else
            {
                std::cerr << "(remove_old_conns):Couldn't Decrement summary statistics "
                          << "of host who owns this connection " << std::endl;
            }
        }
    }

    return rc;
}

int 
ManageMemoryCache::sdb_inc_conn_stat(int32_t hostid, const io_stat_row_t &stat)
{
    int rc = DB_ENOTFOUND;
    db_result_t db_rc = DB_OK;
    int64_t iostat = 0;
    int64_t age = 0;
    io_stat_row_t stat_ = stat;
    int is_new_conn = 0;

    storage::ittiadb::Table table(db, "connstat");
    GET_ECODE(rc, table.open("cs_pkey", storage::ittiadb::Table::Exclusive), 
        "(inc_conn_stat): Couldn't open connstat table");
    storage::data::SingleRow key(table.columns());
    key["hostid"].set(hostid);
    key["dport"].set(stat.dport);
    key["sport"].set(stat.sport);
    storage::data::SingleRow data(table.columns());
    db_rc = table.search_by_index(DB_SEEK_FIRST_EQUAL, key, 3, data);
    GET_ECODE(rc, db_rc);
    if (DB_NOERROR == rc)
    {
        //Update Row
        storage::ittiadb::Query update(db, 
            " UPDATE connstat "
            " SET age = NEXT VALUE FOR age_seq,"
            " iostat = iostat + ?"
            " WHERE hostid =? AND dport =? AND sport =? "
            );
        if( DB_NOERROR == GET_ECODE(rc,update.prepare(),
            "(inc_conn_stat): Query Update connstat row prepare failed "))
        {
            storage::data::SingleRow param_row(update.parameters());
            param_row[0].set(stat.io_bytes); //iostat
            param_row[1].set(hostid);//hostid
            param_row[2].set(stat.dport);//sport
            param_row[3].set(stat.sport);//iostat
            int64_t rows_affected = update.execute_with(param_row);
            if (1 != rows_affected)
            {
                const storage::data::Error& error = storage::data::Environment::error();
                std::cerr << "(inc_conn_stat):Couldn't update connstat table record. [" 
                    << param_row << "] "
                    << error << std::endl;
                rc = error.code;
            }
        }
    }
    else if (DB_ENOTFOUND == rc)
    {
            // Not found. Inserting...
            if (CONNS_DB_LIMIT > connstat_rows_count
                || DB_NOERROR == (rc = sdb_remove_old_conns(OLD_CONNS_REMOVE_CHUNK))
                )
            {
                rc = DB_NOERROR;
                storage::ittiadb::Query insert(db, 
                    "INSERT INTO " 
                    "connstat(hostid, dport, sport, iostat,age) " 
                    "VALUES(?,?,?,?,NEXT VALUE FOR age_seq)"
                    );
                if (DB_NOERROR == GET_ECODE(rc, insert.prepare(), 
                    "(inc_conn_stat): Query Insert connstat row prepare failed "))
                {
                    storage::data::SingleRow param_row(insert.parameters());
                    param_row[0].set(hostid); //hostid
                    param_row[1].set(stat.dport);//dport
                    param_row[2].set(stat.sport);//sport
                    param_row[3].set(stat.io_bytes);//iostat
                    int64_t rows_affected = insert.execute_with(param_row);
                    if (1 != rows_affected)
                    {
                        const storage::data::Error& error = storage::data::Environment::error();
                        std::cerr << "(inc_conn_stat):Couldn't insert connstat table record. [" << param_row << "] "
                            << error << std::endl;
                        rc = error.code;
                    }
                    else {
                        is_new_conn = 1;
                        connstat_rows_count++;
                    }
                    std::cout << "(inc_conn_stat): Add Conn_" << connstat_rows_count 
                        << "(hostid,dport,sport,iostat)" << param_row << std::endl;
                }
            }
            else
            {
                std::cerr << "(inc_conn_stat):Couldn't search connstat table record." << std::endl;
            }
    }
    // Dont forget to increment summary statistics we collect in 'hosts' table
    if (DB_NOERROR == rc) {
        rc = sdb_inc_host_stat_(hostid, age, is_new_conn, stat_.io_bytes);
    }

    if(DB_NOERROR != rc) {
        std::cerr << "(inc_conn_stat):Couldn't increment hosts.conncount column." << std::endl;
    }
    return rc;
}


int  
ManageMemoryCache::sdb_inc_io_stat(const io_stat_row_t& io_stat)
{
    int rc = DB_NOERROR;
    db_result_t db_rc = DB_OK;
    int32_t hostid = 0;
    int tries = 3;

    int save_hosts_rows_count = hosts_rows_count;
    int save_connstat_rows_count = connstat_rows_count;
    storage::ittiadb::Transaction txn(db);
    GET_ECODE(rc,txn.begin(),"Couldn't begin Transaction: ");
    // Find hostid for given host IP
    rc = sdb_find_host_by_ip(io_stat.hostip, hostid);
    if (rc == DB_ENOTFOUND)
    {
        // No such host found in cache. Put it into
        rc = sdb_add_host(io_stat.hostip, 0, hostid);
    }
    if (DB_NOERROR == rc) {
        // Increment connection statistics
        rc = sdb_inc_conn_stat(hostid, io_stat);
    }

    if (DB_NOERROR == rc) {
        GET_ECODE(rc, txn.commit(),"Couldn't commit transaction: ");
    } 
    // Rollback transaction as error occured
    if (DB_NOERROR != rc)
    {
        txn.rollback(storage::ittiadb::Transaction::ForcedCompletion); //what is DB_FORCED_COMPLETION?
        hosts_rows_count = save_hosts_rows_count;
        connstat_rows_count = save_connstat_rows_count;
        std::cout << "(inc_io_stat):rollback ..." << std::endl;
    }
    if (DB_ENOPAGESPACE == rc || DB_ENOMEM == rc) {

        std::cerr << "(inc_io_stat):No memory left. Consider to lower value of HOSTS_DB_LIMIT/CONNSTAT_DB_LIMIT macro" << std::endl
            << "  or reserve more mem for ITTIA DB storage (memory_page_size/memory_storage_size)" << std::endl;
    }
    return rc;
}


void 
ManageMemoryCache::generate_iostat_row(io_stat_row_t& r, int solt)
{
    static char hosts[HOSTS_COUNT % (240 * 240 * 240)][MAX_IP_LEN + 1];
    static uint32_t ports[PORTS_COUNT % 65000];
    static int host_idx = -1, port_idx = 0;

    if (-1 == host_idx) {
        host_idx = 0;
        for (; host_idx < HOSTS_COUNT % (240 * 240 * 240); ++host_idx) {
            sprintf(hosts[host_idx], "192.%d.%d.%d",
                host_idx / 240 / 240 % 240 + 10,
                host_idx / 240 % 240 + 10,
                host_idx % 240 + 10
                );
            //   std::cout << host_idx << ". Genip: " << hosts[host_idx - 1] << std::endl;
        }
        for (; port_idx < PORTS_COUNT; ++port_idx) { ports[port_idx] = 5000 + port_idx; }
    }

    host_idx += port_idx % PORTS_COUNT ? 0 : 1;

    strncpy(r.hostip, hosts[host_idx % HOSTS_COUNT], MAX_IP_LEN); r.hostip[MAX_IP_LEN] = 0;
    r.dport = ports[port_idx++ % PORTS_COUNT];
    //    r->sport = ports[ port_idx % PORTS_COUNT ];
    r.sport = ports[(PORTS_COUNT - (port_idx++ % PORTS_COUNT)) % PORTS_COUNT];
    r.io_bytes = 10;
}


