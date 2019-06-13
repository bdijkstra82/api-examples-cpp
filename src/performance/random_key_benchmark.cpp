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

#include <fstream>

#include "storage/data/environment.h"
#include "storage/ittiadb/connection.h"
#include "storage/ittiadb/query.h"
#include "storage/ittiadb/transaction.h"
#include "storage/ittiadb/table.h"
#include "storage/data/single_row.h"

#include "db_iostream.h"

#include "time_counter.h"

using ::storage::data::Environment;
using ::storage::ittiadb::Connection;
using ::storage::ittiadb::Query;
using ::storage::ittiadb::Transaction;
using ::storage::ittiadb::Table;

static const char* EXAMPLE_DATABASE = "random_key_benchmark.ittiadb";

inline std::ostream& operator<<(std::ostream& os, const TimeCounter& time_counter)
{
    const size_t ms = time_counter.elapsed_milliseconds();
    std::streamsize old_width = os.width(10);
    os << ms << " milliseconds";
    os.width(old_width);

    if (ms < 2 * 60 * 1000) {
        os << " (" << (ms / (1000)) << " seconds)";
    }
    else if (ms < 2 * 60 * 60 * 1000) {
        os << " (" << (ms / (60 * 1000)) << " minutes)";
    }
    else {
        os << " (" << (ms / (60 * 60 * 1000)) << " hours)";
    }
    return os;
}

template <size_t N>
static void random_string(char (&buf)[N])
{
    for (size_t i = 0; i < N - 1; ++i) {
        // Append a random ANSI character
        buf[i] = (char)0x20 + (rand() % 0x5F);
    }
    buf[N - 1] = '\0';
}

static void
index_random_key_benchmark(Connection& database, const char * data_out_filename, int batches, int rows_per_batch)
{
    class TRow : public ::storage::data::SingleRow {
    public:
        char key[48 + 1];
        int32_t value;

        TRow(const ::storage::data::RowDefinition& columns)
            : SingleRow(columns)
            , key()
            , value(0)
        {
            bind("key", key);
            bind("value", value);
        }
    };

    std::ofstream data_out;
    if (data_out_filename != NULL) {
        (void)data_out.open(data_out_filename);
    }

    int major;
    int minor;
    int patch;
    int build;
    Environment::product_version(major, minor, patch, build);
    std::cout << "Benchmarking ITTIA DB SQL " << major << "." << minor << "." << patch << "." << build << std::endl;

    if (DB_OK != database.open(Connection::CreateAlways)) {
        std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
        return;
    }

    if (0 > Query(database,
          "create table t("
          "  \"key\" varchar(48) primary key,"
          "  \"value\" integer"
          ") cluster by primary key"
          ).execute())
    {
        std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
        return;
    }

    TimeCounter total_time;
    TimeCounter insert_time;
    TimeCounter fetch_time;
    TimeCounter commit_time;

    std::cout << "Inserting and fetching " << (batches * rows_per_batch) << " records in " << batches << " transactions..." << std::endl;

    total_time.start();

    Table table_t(database, "t");

    if (DB_OK != table_t.open("PK")) {
        std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
        return;
    }

    TRow t_row(table_t.columns());
    TRow found_row(table_t.columns());

    for (int batch = 0; batch < batches; batch++) {
        Transaction txn(database);
        if (DB_OK != txn.begin()) {
            std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
            return;
        }

        for (int row = 0; row < rows_per_batch; row++) {
            // Generate one of (48 * 0x5F) possible keys
            random_string(t_row.key);

            // Insert a record with the randomly generated key
            insert_time.start();
            insert_time.start();
            t_row.value = 0;
            if (DB_OK != table_t.insert(t_row)) {
                if (Environment::is_error(DB_EDUPLICATE)) {
                    // A record with the same key already exists; increment its value instead.
                    if (DB_OK != table_t.search_by_index(DB_SEEK_EQUAL, t_row, found_row)) {
                        std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
                        return;
                    }

                    ++found_row.value;

                    if (DB_OK != table_t.update_by_index(t_row, found_row)) {
                        std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
                        return;
                    }
                }
                else {
                    std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
                    return;
                }
            }
            insert_time.stop();

            // Generate one of (48 * 0x5F) possible keys
            random_string(t_row.key);

            const char * match = "";

            // Locate and fetch the record nearest to the randomly generated key
            fetch_time.start();
            if (DB_OK == table_t.search_by_index(DB_SEEK_GREATER_OR_EQUAL, t_row, found_row)) {
                match = ">=";
            }
            else if (Environment::is_error(DB_ENOTFOUND)) {
                if (DB_OK == table_t.search_by_index(DB_SEEK_LESS_OR_EQUAL, t_row, found_row)) {
                    match = "<=";
                }
                else {
                    std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
                    return;
                }
            }
            else {
                std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
                return;
            }
            fetch_time.stop();

            if (data_out.is_open()) {
                data_out << found_row.key << "\t" << match << "\t" << t_row.key << "\t" << found_row.value << std::endl;
            }
        }

        commit_time.start();
        if (DB_OK != txn.commit()) {
            std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
            return;
        }
        commit_time.stop();
    }

    total_time.stop();

    if (DB_OK != database.close()) {
        std::cerr << __FILE__ << ":" << __LINE__ << " " << Environment::error() << std::endl;
        return;
    }
    fetch_time.max_milliseconds();

    std::cout << "Total time:        " << total_time << std::endl;
    std::cout << "Read time:         " << fetch_time << std::endl;
    std::streamsize old_width = std::cout.width(10);
    std::cout << "  Longest read:    " << fetch_time.max_milliseconds() << " milliseconds" << std::endl;
    std::cout.width(old_width);
    std::cout << "Write time:        " << (insert_time + commit_time) << std::endl;
    std::cout << "  Commit overhead: " << commit_time << std::endl;
}

int example_main(int argc, char* argv[])
{
    const char * database_uri = EXAMPLE_DATABASE;
    const char * data_out_filename = NULL;
    if (argc > 1) {
        database_uri = argv[1];
    }
    if (argc > 2) {
        data_out_filename = argv[2];
    }

    srand(0);

    Connection database(database_uri);

    index_random_key_benchmark(database, data_out_filename, 1000, 100);

    std::cout << "Enter SQL statements or an empty line to exit" << std::endl;
    sql_line_shell(database);

    return EXIT_SUCCESS;
}
