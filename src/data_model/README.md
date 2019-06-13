% ITTIA DB SQL Data Modeling Examples

ITTIA DB SQL uses the relational data model to organize data into indexed tables. Related information is divided into separate tables that can be joined together with SQL queries in a flexible way. The database schema defines the structure of the tables, including foreign key constraints that enforce table relationships, and can evolve over time as the needs of the application change with each upgrade.

The Data Modeling examples show how to use ITTIA DB SQL data types, constraints, and related features to manage data in an embedded application.

# unicode_character_strings

The Unicode Character Strings example stores text translated into multiple languages in an ITTIA DB SQL database table. To look up a localized string, the application supplies an identifier and a language code. This example demonstrates:

 - Storing localized text in a database table.
 - Reading Unicode-encoded text from a database table.

# datetime_intervals

The Datetime and Intervals example uses ITTIA DB SQL data types to store date, time, and duration values. This example demonstrates:

 - Adding a timestamp to a row inserted with SQL.
 - Using standard date and time format strings.
 - Using custom date and time format strings.
 - Extracting hours, minutes, and seconds from a time column.
 - Extracting years, months, and days from a date column.
 - Converting date and time columns to and from a UNIX timestamp.
 - Adding months, years, or days to a date column.
 - Adding hours, minutes, or seconds to a time column.
