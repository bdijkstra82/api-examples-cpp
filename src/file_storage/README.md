% ITTIA DB SQL File Storage Examples

The File Storage examples show how a C/C++ application can create and use ITTIA DB SQL database files on a file system. Database files are continuously updated as database transactions are committed by the application, and ITTIA DB SQL provides options to control how often this will result in I/O operations being performed.


# atomic_file_storage

The Atomic File Storage example uses transactions to save data in a persistent database file. This demonstrates:

 - Creating a local ITTIA DB SQL database file with a custom storage configuration.
 - Committing changes in an atomic database transaction.
 - Closing a database to discard uncommitted changes.

A database is created by specifying a file name with a relative or absolute path.

```CPP
db_result_t rc;
static const char* EXAMPLE_DATABASE = "atomic_file_storage.ittiadb";
        
storage::ittiadb::Connection database(EXAMPLE_DATABASE);

// Init storage configuration with default settings
storage::ittiadb::Connection::FileStorageConfig file_storage_config;
// Create database with custom page size;
file_storage_config.page_size = DB_DEF_PAGE_SIZE * 2;
// Create a new database file
rc = database.open(storage::ittiadb::Connection::CreateAlways, file_storage_config);
if (DB_OK != rc)
{
    std::cerr << storage::data::Environment::error() << std::endl;
    return EXIT_FAILURE;
}
```

After the file is created, the application can add tables, indexes, and other schema elements to the database using the connection handle.

The database is modified by inserting, updating, or deleting records using database cursors or SQL statements. Changes are only saved when a transaction is committed. When the database connection is shut down, uncommitted changes are automatically rolled back. This behavior ensures that only complete updates are saved permanently.

```C
 //Start transaction
storage::ittiadb::Transaction txn(database);
storage::ittiadb::Table table(database, "tablename");
table.open();
rc = txn.begin();
if (DB_OK != rc)
{
    std::cerr << "Error beginning transaction: " 
        << storage::data::Environment::error() << std::endl;
    return EXIT_FAILURE;
}
//Add first row
storage::data::SingleRow row(table.columns());
row["ansi_field"].set("ansi_str1");
row["int64_field"].set(1);
row["float64_field"].set(1.231);
row["utf8_field"].set("utf8-1");
rc = table.insert(row);


// Commit after inserting 3 records; don't commit the last 2 rows.
rc = txn.commit();

```

# bulk_import

The Bulk Import example shows how to efficiently import existing data into a new ITTIA DB SQL database file. This demonstrates:

 - Creating a database file without a transaction log.
 - Recovering from errors during a bulk import.

ITTIA DB SQL uses a transaction log to automatically recover from a crash or unexpected power loss. When a database file is first created, however, the transaction log may cause unnecessary overhead, since the file can be recreated if a problem occurs.

```CPP
// Init storage configuration with default settings
storage::ittiadb::Connection::FileStorageConfig file_storage_config;
file_storage_config.file_mode &= ~DB_NOLOGGING;
// Create database & schema with logging disabled
if (EXIT_SUCCESS != create_database_schema(database, file_storage_config))
{
    std::cerr << storage::data::Environment::error() << std::endl;
    return EXIT_FAILURE;
}
```

A transaction log will be created the next time the file is opened, if the `DB_NOLOGGING` flag is not used. The transaction log is removed automatically when all connections to the database file are closed.


# background_commit

The Background Commit example achieves a high transaction throughput rate by flushing multiple transaction to the storage media at once. By leveraging ITTIA DB SQL transaction completion modes, the application has full control over transaction durability without compromising atomicity. This demonstrates:

 - Committing transactions with lazy transaction completion mode.
 - Manually flushing the transaction journal to persist changes.
 - Closing the database to persist changes.

```CPP
//insert 100 rows
for (int i = 0; i < 100 && DB_OK == db_rc; ++i) {
    db_rc = txn.begin();
    int do_lazy_commit = i % 5;
        
    row["int64_field"].set(i+1);
    row["float64_field"].set(do_lazy_commit? 50.0:16.0);
        
    db_rc = (DB_OK == db_rc) ? table.insert(row) : db_rc;
    if (DB_OK == db_rc)
    {
        if (do_lazy_commit) {
            stat.lazy_tx++;
            db_rc = txn.commit(storage::ittiadb::Transaction::LazyCompletion);
        }
        else {
            stat.forced_tx++;
            db_rc = txn.commit(storage::ittiadb::Transaction::ForcedCompletion);
        }
    }

    if (i % 30) {
        //what storage API?
        db_rc = (DB_OK == db_rc) ? 
            db_flush_tx(database.handle(), DB_FLUSH_JOURNAL):db_rc;
    }
}
```

Transactions committed with the default completion mode are saved to the database file immediately, before the `db_commit_tx` function returns. Transactions committed with the lazy completion mode may not be written to the file until `db_flush_tx` is called.
