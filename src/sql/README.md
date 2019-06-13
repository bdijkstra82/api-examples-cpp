% ITTIA DB SQL Statement Examples

The SQL Statement examples show how a C/C++ application can use SQL to access and modify a database file. SQL is a declarative programming language that handles the details of locating and processing records stored in indexed tables. An application uses browsable cursors to retrieve data from an SQL statement's result set. Query parameters can also be used to supply data and filter criteria to the SQL statement.

# sql_select_query

The SQL Select Query example prepares and executes an SQL query in an ITTIA DB SQL database. This demonstrates:

 - Preparing and executing a SELECT statement.
 - Iterating over a result set and fetching rows.
 - Limiting the result set with FETCH FIRST and OFFSET clauses.

When executed, an SQL select query produces a result set. This example uses a simple query that selects all rows and columns from a table.

```SQL
select id, data
  from storage
```

Each field in the result set is bound to a local variable using a binding array. The fields are numbered according to their position in the select list, starting with 0.

After an SQL cursor is executed, records are processed by iterating over the cursor and fetching data through the row bindings. The fetch operation copies data from the result set into the local variables.

```CPP
storage::data::RowSet<>::iterator itr = result_set.begin();
for (; itr != result_set.end() && (DB_NOERROR == rc); ++itr)
{
    const int32_t id = itr->at(0).to<int32_t>();
    const int32_t data = itr->at(1).to<int32_t>();
    process_record(id, data);
}
```

# sql_parameters

The SQL Parameters example sets SQL data and search criteria with query parameters, to avoid modifying the SQL query string at run-time. This demonstrates:

 - Executing a prepared SQL query multiple times with different parameter values.
 - Paging a result set with parameterized FETCH FIRST and OFFSET clauses.

An SQL query is parameterized by replacing literal values with `?` placeholders.

```SQL
select id, data
  from storage
  where id > ? and data > ?
```

Before the cursor is executed, the parameter variables must each be assigned a value. starting with field number 0 and numbered according to the order they appear in the SQL statement. 

```CPP
int32_t id_param, data_param;
id_param = 98;
data_param = 0;
storage::data::SingleRow params_row(select_query.parameters());
params_row[0].set(id_param);
params_row[1].set(data_param);
storage::data::RowSet<> result_set(select_query.columns());
if (DB_OK != select_query.execute_with(params_row, result_set)) {
    GET_ECODE(rc, DB_FAIL, "executing parameterize query");
}
```
