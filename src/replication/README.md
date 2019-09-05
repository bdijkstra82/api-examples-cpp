% ITTIA DB SQL Replication Examples

# initialize_mirror

The Replication Mirror example shows how to create a main database and a replication mirror. Changes to the main database are copied to the replication mirror during each replication exchange.


# incremental_backup

The SQL Incremental Backup example shows how to protect a database from hardware failure with ITTIA DB SQL backup and replication features. A full backup is created and incrementally updated in the background, while the main database is continuously modified. To simulate hardware failure, the main database is then removed and restored from the backup copy.

This example demonstrates:

- How to create a full database backup and configure database replication for incremental backup.
- Replication exchange and backup in background threads.
- Restoring a database from backup after simulated hardware failure.
- Best practices to [prevent deadlocks][1] when configuring replication.

[1]: https://www.ittia.com/html/ittia-db-docs/users-guide/locking.html

A full database backup is created using a separate connection in a background thread:

```CPP
Connection main_database(td->database_name);
main_database.open(Connection::OpenExisting);
main_database.backup(td->incremental_backup_name, 0);
```

Replication is configured to copy all changes from the main database to the backup database:

```CPP
main_database_catalog.set_config(MAIN_REP_ADDRESS);
backup_catalog.set_config(BACKUP_REP_ADDRESS);

ReplicationCatalog::TableInfo backup_table_info(DB_REP_MODE_IN);
backup_catalog.table_set("t1", backup_table_info);
backup_catalog.table_set("t2", backup_table_info);

ReplicationCatalog::PeerDefinition backup_definition(
    DB_REPTYPE_ADHOC,
    MAIN_REP_ADDRESS,
    main_database.uri());
backup_catalog.create_peer( PEER_NAME, backup_definition);
```

Incremental backup is achieved by periodically performing a replication exchange:

```CPP
ReplicationPeer backup_peer(backup_database, PEER_NAME);
while(td->is_backup_enabled)
{
    example_thread::sleep_for_milliseconds(100);
    backup_peer.exchange();
}
```

ITTIA DB SQL uses transaction logging to protect both the main and backup database files from inconsistencies. While the backup copy may not contain the most recent transactions, it is always updated to an atomic transaction boundary.

To show the transaction boundaries, the example always inserts records in batches of three. After the example is finished, you can run the following SQL query on the backup file to check the number of rows in each transaction:

```SQL
select count(data) from t1 group by data;
```

