% ITTIA DB SQL Security Examples

Sensitive information must be protected both at rest and in motion with data encryption. When an embedded device is deployed in a public space, physical access to removable storage media can expose data if it is not encrypted. If a device shares data on a public WiFi network or with other devices on the Internet, unencrypted communications pose a similar risk.

The Security examples show how an embedded application can secure an ITTIA DB SQL database file with AES encryption and restrict access from remote connections.

# storage_encryption

The Storage Encryption example encrypts a database file with AES. The application can only open the database with the correct encryption key. This demonstrates:

 - Creating an encrypted file storage database.
 - Attempting to open an encrypted database with and without the decryption key.
 - Changing the encryption key for an existing database.

The encryption key for an ITTIA DB SQL database file can contain any random data. The key should be stored in a secure location; the example uses a static variable.

```CPP
/* Hard-coded database file name and encryption key. */
static const char * EXAMPLE_DATABASE
    = "storage_encryption.ittiadb";
static const char encryption_key[256 / 8]
    = "32byte_256bit_length_encrypt_key";
```

When a database is first created, use the `db_auth_info_t` data structure to specify the encryption algorithm, such as AES-256, and the secret encryption key.

```CPP
/* Set AES-256 ecryption parameters */
storage_cfg.auth_info = &auth_info;
auth_info.cipher_type = DB_CIPHER_AES256_CTR;
auth_info.cipher_data = (void *) encryption_key;
auth_info.cipher_data_size = sizeof( encryption_key );

// Create encrypted database file
storage::ittiadb::Connection database(EXAMPLE_DATABASE);
    
if (DB_OK != database.open(storage::ittiadb::Connection::CreateAlways, storage_cfg))
{
    (void)GET_ECODE(rc, DB_FAIL, "unable to create database ");
}
```

All data stored in the database is encrypted before it is written to disk. If all connections to the database file are closed, it cannot be reopened without the encryption key. 
