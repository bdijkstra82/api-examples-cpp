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

/// @file print_schemas.cpp
///
/// Command-line example program demonstrating the ITTIA DB Storage API.
///

#include <stdio.h>

#include <storage/ittiadb/connection.h>
#include <storage/ittiadb/transaction.h>
#include <storage/ittiadb/catalog.h>
#include <storage/ittiadb/query.h>
#include <storage/data/row_set.h>

using ::storage::data::Environment;
using ::storage::ittiadb::Connection;
using ::storage::ittiadb::Transaction;
using ::storage::ittiadb::Catalog;
using ::storage::ittiadb::Query;
using ::storage::data::RowSet;

static void print_database_schema(Connection& db);
static void print_table_schema(const db_tabledef_t & tabledef);
static void print_foreign_key_definition(const db_tabledef_t & tabledef, const db_foreign_key_def_t & fk, const db_tabledef_t & ref_table);

static void
print_database_schema(Connection& database)
{
    RowSet<> table_list;
    Query(database,
          "select table_name from tables where table_level > 1"
          ).execute(table_list);
    Transaction(database).commit();

    Catalog catalog(database);
    db_tabledef_t tabledef;
    db_tabledef_t ref_table;
    db_tabledef_init(&tabledef, NULL);
    db_tabledef_init(&ref_table, NULL);

    // Print tables and indexes
    for (RowSet<>::const_iterator iter = table_list.begin(); iter != table_list.end(); ++iter)
    {
        iter->at(0).get(tabledef.table_name);
        if (DB_OK == catalog.describe_table(tabledef.table_name, tabledef, DB_DESCRIBE_TABLE_FIELDS | DB_DESCRIBE_TABLE_INDEXES))
        {
            print_table_schema(tabledef);
        }
    }

    // Print foreign key constraints
    for (RowSet<>::const_iterator iter = table_list.begin(); iter != table_list.end(); ++iter)
    {
        iter->at(0).get(tabledef.table_name);
        if (DB_OK == catalog.describe_table(tabledef.table_name, tabledef, DB_DESCRIBE_TABLE_FOREIGN_KEYS)) {
            for (int j = 0; j < tabledef.nforeign_keys; j++) {
                const db_foreign_key_def_t & fk = tabledef.foreign_keys[j];

                if (DB_OK == catalog.describe_table(fk.ref_table, ref_table, DB_DESCRIBE_TABLE_FIELDS)) {
                    print_foreign_key_definition(tabledef, fk, ref_table);
                }
            }
        }
    }

    db_tabledef_destroy(&tabledef);
    db_tabledef_destroy(&ref_table);
}

static void
print_table_schema(const db_tabledef_t & tabledef)
{
    FILE * out_file = stdout;
    int j;
    int k;
    db_len_t ix_constraint_count;
    int cluster_by_primary_key = 0;
    const db_objname_t * pk_name = NULL;
    db_fieldno_t pk_fieldno = -1;

    // Get the total number of constraints to list in the table definition.
    ix_constraint_count = tabledef.nindexes;
    for (j = 0; j < tabledef.nindexes; j++) {
        const db_indexdef_t * ix = &tabledef.indexes[j];
        if( (ix->index_mode & DB_INDEX_MODE_MASK) == DB_PRIMARY_INDEX) {
            if (ix->index_mode & DB_CLUSTERING_INDEX)
                cluster_by_primary_key = 1;
            if (!(ix->index_name[0] == 'P' && ix->index_name[1] == 'K' && ix->index_name[2] == '\0'))
                pk_name = ix->index_name;
            if (ix->nfields == 1) {
                pk_fieldno = ix->fields[0].fieldno;
                ix_constraint_count--;
            }
        }
        else if( (ix->index_mode & DB_INDEX_MODE_MASK) == DB_MULTISET_INDEX)
            ix_constraint_count--;
    }

    // Table fields
    fprintf(out_file, "CREATE TABLE %s\n(\n", tabledef.table_name);

    for (j = 0; j < tabledef.nfields; j++) {
        const db_fielddef_t * fd = &tabledef.fields[j];

        fprintf(out_file, "    %s ", fd->field_name);

        switch( reinterpret_cast<uintptr_t>(fd->field_type) ) {
        case DB_COLTYPE_SINT8_TAG:      fprintf(out_file, "SINT8"); break;
        case DB_COLTYPE_UINT8_TAG:      fprintf(out_file, "UINT8"); break;
        case DB_COLTYPE_SINT16_TAG:     fprintf(out_file, "SINT16"); break;
        case DB_COLTYPE_UINT16_TAG:     fprintf(out_file, "UINT16"); break;
        case DB_COLTYPE_SINT32_TAG:     fprintf(out_file, "SINT32"); break;
        case DB_COLTYPE_UINT32_TAG:     fprintf(out_file, "UINT32"); break;
        case DB_COLTYPE_SINT64_TAG:     fprintf(out_file, "SINT64"); break;
        case DB_COLTYPE_UINT64_TAG:     fprintf(out_file, "UINT64"); break;
        case DB_COLTYPE_FLOAT32_TAG:    fprintf(out_file, "FLOAT32"); break;
        case DB_COLTYPE_FLOAT64_TAG:    fprintf(out_file, "FLOAT64"); break;
        case DB_COLTYPE_CURRENCY_TAG:   fprintf(out_file, "CURRENCY"); break;
        case DB_COLTYPE_DATE_TAG:       fprintf(out_file, "DATE"); break;
        case DB_COLTYPE_TIME_TAG:       fprintf(out_file, "TIME"); break;
        case DB_COLTYPE_DATETIME_TAG:   fprintf(out_file, "DATETIME"); break;
        case DB_COLTYPE_TIMESTAMP_TAG:  fprintf(out_file, "TIMESTAMP"); break;
        case DB_COLTYPE_INTERVAL_YEAR_TO_MONTH_TAG:
                                        fprintf(out_file, "INTERVAL YEAR(9) TO MONTH"); break;
        case DB_COLTYPE_INTERVAL_DAY_TO_SECOND_TAG:
                                        fprintf(out_file, "INTERVAL DAY(9) TO SECOND(6)"); break;
        case DB_COLTYPE_ANSISTR_TAG:    fprintf(out_file, "VARCHAR(%d)", (int)fd->field_size); break;
        case DB_COLTYPE_UTF8STR_TAG:    fprintf(out_file, "UTF8STR(%d)", (int)fd->field_size); break;
        case DB_COLTYPE_UTF16STR_TAG:   fprintf(out_file, "UTF16STR(%d)", (int)fd->field_size); break;
        case DB_COLTYPE_UTF32STR_TAG:   fprintf(out_file, "UTF32STR(%d)", (int)fd->field_size); break;
        case DB_COLTYPE_BINARY_TAG:     fprintf(out_file, "VARBINARY(%d)", (int)fd->field_size); break;
        case DB_COLTYPE_BLOB_TAG:       fprintf(out_file, "BLOB"); break;
        }

        if (fd->fieldno == pk_fieldno) {
            // PRIMARY KEY implies NOT NULL
            if (pk_name == NULL)
                fprintf(out_file, " PRIMARY KEY");
            else
                fprintf(out_file, " CONSTRAINT %s PRIMARY KEY", pk_name);
        }
        else if ((fd->field_flags & DB_NULL_MASK) == DB_NULLABLE) {
            // NULL is the default, but print it anyway
            fprintf(out_file, " NULL");
        }
        else {
            fprintf(out_file, " NOT NULL");
        }

        if (j + 1 < tabledef.nfields || ix_constraint_count > 0)
            fputs(",", out_file);
        fputc('\n', out_file);
    }

    // Indexes
    for (j = 0; j < tabledef.nindexes; j++) {
        const db_indexdef_t * ix = &tabledef.indexes[j];

        switch (ix->index_mode & DB_INDEX_MODE_MASK) {
        case DB_PRIMARY_INDEX:
            // Primary key was declared in field definition.
            if (pk_fieldno != -1)
                continue;

            if (pk_name == NULL)
                fprintf(out_file, "    PRIMARY KEY(");
            else
                fprintf(out_file, "    CONSTRAINT %s PRIMARY KEY(", pk_name);
            break;

        case DB_UNIQUE_INDEX:
            fprintf(out_file, "    CONSTRAINT %s UNIQUE(", ix->index_name);
            break;
        case DB_MULTISET_INDEX:
            // Multiset indexes are written separately.
            continue;

        }
        for (k = 0; k < ix->nfields; k++) {
            if (k > 0)
                fprintf(out_file, ", ");
            fprintf(out_file, "%s", tabledef.fields[ix->fields[k].fieldno].field_name);
        }

        fputc( ')' ,out_file);
        // Write a comma after all but the last constraint.
        if( --ix_constraint_count > 0 )
            fputc(',', out_file);

        fputc('\n', out_file);
    }
    fprintf(out_file, ")");
    if (tabledef.table_type == DB_TABLETYPE_CLUSTERED && cluster_by_primary_key) {
        fprintf(out_file, " CLUSTER BY PRIMARY KEY");
    }
    fprintf(out_file, ";\n");
    fflush(out_file);

    // Indexes
    for (j = 0; j < tabledef.nindexes; j++) {
        const db_indexdef_t * ix = &tabledef.indexes[j];
        const char * numbered = (ix->index_mode & DB_NUMBERED_INDEX) ? " COUNTED" : "";

        if ( (ix->index_mode & DB_INDEX_MODE_MASK) == DB_MULTISET_INDEX) {
            fprintf(out_file, "CREATE%s INDEX %s ON %s(",
                numbered, ix->index_name, tabledef.table_name);

            for (k = 0; k < ix->nfields; k++) {
                if (k > 0)
                    fprintf(out_file, ", ");
                fprintf(out_file, "%s", tabledef.fields[ix->fields[k].fieldno].field_name);
            }
            fprintf(out_file, ");\n");
        }

    }
    fflush(out_file);
}

static void
print_foreign_key_definition(const db_tabledef_t & tabledef, const db_foreign_key_def_t & fk, const db_tabledef_t & ref_table)
{
    FILE * out_file = stdout;
    int k;

    fprintf(out_file, "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (",
        tabledef.table_name, fk.fk_name);

    for (k = 0; k < fk.nfields; k++) {
        if (k > 0)
            fprintf(out_file, ", ");
        fprintf(out_file, "%s", tabledef.fields[fk.fields[k].org_field].field_name);
    }

    fprintf(out_file, ") REFERENCES %s(", fk.ref_table );

    for (k = 0; k < fk.nfields; k++) {
        if (k > 0)
            fprintf(out_file, ", ");
        fprintf(out_file, "%s", ref_table.fields[fk.fields[k].ref_field].field_name);
    }

    fprintf(out_file, ")");

    switch(fk.match_option) {
    case DB_FK_MATCH_SIMPLE: fprintf(out_file, " MATCH SIMPLE"); break;
    case DB_FK_MATCH_FULL:   fprintf(out_file, " MATCH FULL"); break;
    }

    switch (fk.update_rule) {
    case DB_FK_ACTION_RESTRICT: fprintf(out_file, " ON UPDATE RESTRICT"); break;
    case DB_FK_ACTION_CASCADE:  fprintf(out_file, " ON UPDATE CASCADE"); break;
    case DB_FK_ACTION_SETNULL:  fprintf(out_file, " ON UPDATE SET NULL"); break;
    case DB_FK_ACTION_SETDEFAULT:  fprintf(out_file, " ON UPDATE SET DEFAULT"); break;
    }

    switch (fk.delete_rule) {
    case DB_FK_ACTION_RESTRICT: fprintf(out_file, " ON DELETE RESTRICT"); break;
    case DB_FK_ACTION_CASCADE:  fprintf(out_file, " ON DELETE CASCADE"); break;
    case DB_FK_ACTION_SETNULL:  fprintf(out_file, " ON DELETE SET NULL"); break;
    case DB_FK_ACTION_SETDEFAULT:  fprintf(out_file, " ON DELETE SET DEFAULT"); break;
    }

    fprintf(out_file, ";\n");
}

int
example_main(int argc, char *argv[])
{
    int rc = DB_NOERROR;

    const char * database_uri = "";
    if (argc > 1) {
        database_uri = argv[1];
    }
    else {
        fprintf(stderr, "Argument required: database file name\n");
        return EXIT_FAILURE;
    }

    Connection database(database_uri);
    if (DB_OK != database.open(Connection::OpenExisting)) {
        ::storage::data::Error error = Environment::error();
        rc = error.code;
        fprintf(stderr, "Opening database failed: %s\n", error.description);
    }

    if( DB_NOERROR == rc ) {
        print_database_schema( database );
    }

    return (DB_NOERROR != rc) ? EXIT_FAILURE : EXIT_SUCCESS;
}
