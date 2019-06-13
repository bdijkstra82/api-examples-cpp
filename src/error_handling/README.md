% ITTIA DB SQL Error Handling Examples

ITTIA DB SQL uses database transactions to protect data integrity. When an error occurs in either the application or database API, incomplete changes to the database storage can be discarded by rolling back the current transaction. This approach preserves the consistency of related database records even if data is malformed or would violate defined constraints before it is stored in the database.

# transaction_rollback

The Transaction Rollback example uses transactions to maintain consistency when an error is encountered in a data stream. This demonstrates:

 - Handling constraint violations with transaction rollback.

The example reads packets of data from a simulated sensor. When a packet is completed, the results are committed, but only if the packet data is determined to be accurate.

```CPP
// Start a new transaction when a new packet is encountered
 if( current_packet != readings[i].packetid ) {
    // Commit only if the last packet contained accurate data
    if( is_wrong_packet ) {
        // Previous packet had bad data, so roll back its changes 
        if ( DB_OK != tx.rollback() ) {
            GET_ECODE(rc, DB_FAIL, "unable to roll back packet data");
        }
    }
    else if ( in_tx ) {
        // Commit previous packet's changes
        if ( DB_OK != tx.commit() ) 
        {
            GET_ECODE(rc, DB_FAIL, "unable to commit packet data");
        }
        good_packets++;
    }
    else {
        std::cerr << "unable to commit packet data"<<std::endl;
    }
    is_wrong_packet = 0;
    current_packet = readings[i].packetid;
    if(DB_OK != tx.begin())
    {
        GET_ECODE(rc, DB_FAIL, "Transaction Begin Failed ");
    }
    in_tx = 1;
}
```

# savepoint_rollback

The Savepoint Rollback example uses savepoints to handle errors in a function that is part of a larger transaction. This demonstrates:

 - Handling recoverable errors within a large multi-step transaction.
