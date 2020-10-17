This program may need NET Framework Version 4.5 to run!
If NOSQLServer.exe cannot run, you can download net framework 4.5 from http://www.microsoft.com/en-us/download/details.aspx?id=30653

FEATURES AND RESTRICTIONS
----------------------------------------------------------------------------------
* Each columns in where clause (when with_index parameter is false) must be placed in select list!

* Supported data types are : tinyint, smallint, int, bigint, double, text

* Supported operators in a where() function are : ==, !=, <, <=, >, >=, &&, ||, like, not like

* || operator and bracketing unsopperted when where() function's second parameter is true

* Column names of select() function must be in order, for example:
Assume that Persons table's structure is as
-----------------
per_id | per_name
-----------------

If you use c.select("persons",  new string[] { "per_name", "per_id" }).execute_reader();
Then your result will be incorrect!

* count(*) function must be end of select columns list

* When you use update() function, you can use only + operator in values() function

* Supported groupby functions are : count(*), sum(), avg(), max(), min()

* NOSQLServer uses sqllite's indexing

* root password is gokhan

* If electricity cuts when NOSQLServer runs, you can repair your databases after electricity returns if necessary. (If necessary program will inform you when server starts up. If you select to repair database, repairing process may be take a long time so be sure your computer plugged in to UPS)

STRUCTURE OF config.ini
----------------------------------------------------------------------------------
[GENERAL]
Port=9999
CultureInfo=en-US
Recordset_Activity_Timeout=10
----------------------------------------------------------------------------------

DESCRIPTION OF config.ini VARIABLES
----------------------------------------------------------------------------------
Port=Port number what NOSQLServer.exe uses to accept connections
CultureInfo=Country code what NOSQLServer.exe uses to manipulate text fields of tables
Recordset_Activity_Timeout=Value in seconds

NOTE
----------------------------------------------------------------------------------
You have to run NOSQLServer with Administrator rights. (Right click NOSQLServer.exe and click "Run as administrator")

You can communicate with me via my e-mail : gokhankorukcu1980@gmail.com

Project Homepage : http://www.sqlmachine.com


