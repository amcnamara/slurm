> **DEPRECATION NOTICE**: Slurm is no longer maintained, and hasn't been updated to modern versions of Clojure. This repository is a proof of concept for some interesting ideas, but these should not be considered idiomatic for either Clojure or MySQL.


SLURM
=====

Slurm is a Clojure ORM.  Its modelled after a traditional Object-Oriented ORM but also includes a dynamic DSL for read/write operations.  It supports the following features:

* Records can be queried and processed as sequences, with eager- or lazy-loading
* Records can be mapped with single and multi-object relations
* Homoiconic schemas, easy to adapt to existing projects/DBs
* Macro-based read/write functions which symbolically map to schema tables


Overview
--------

The first thing you need to do is define a schema for your DB; this includes credentials and connection information, as well as definitions of all tables and columns.  The schema is defined in a standard hash as follows:

    { :db-host  <ip-address>   (default: localhost)
      :db-port  <port-number>  (default: 3306)
      :db-user  <username>     (default: root)
      :db-pass  <password>     (default: n/a)
      :loading  eager|lazy     (default: lazy)
      :tables   #{<tables>} }

The root-level of the db-schema map concerns db credentials, the value of `:tables` is a collection of table definitions described as follows:

    { :name                        <table-name>
      :primary-key                 <key-name>      (default: :id)
      :primary-key-type            <type>          (default: "int(11)")
      :primary-key-auto-increment  true|false      (default: true)
      :fields                      {<field-map} }

Now that you've defined the table outlines, you need to fill in the fields that the table will hold, as follows:

    { <field-name> <field-type>, ... }

Note that all field names (and table names) should be keywordized.  Field types can either be SQL primatives (described below), or references to other tables.  Single-reference fields (ie. the field will point to at most a single row in the foreign table) are defined by putting the keyword table name of the relational table into the `field-type` value.  Multi-reference fields (ie. one which contains reference to many rows of a foreign table) are defined with the keyword table name prefixed with an asterisk, so a field called `:enrolled_students` with a multi-record relation to a table named `:student` may be defined as follows:

    { :enrolled_students :*student, ... }

Fields which represent standard SQL types may have the following `field-types`:

    "int(n)"
    "varchar(n)"
    "bool"
    "date"
    "datetime"
    "blob"

> **SEE**: MySQL documentation for further details on supported native types.

A complete example of a schema definition can be found under `src/example/db_schema`, it may help clarify some of the naming conventions described here.  To see it in use launch the script under `src/example/db_example.clj` with the instructions given below.


Interaction
-----------

Now that you've got your schema defined you're ready to use slurm!  Run `lein jar` from the slurm source folder and move the resulting `.jar` into your project's lib folder.

Interacting with your database is extremely easy and can all be done from within the `with-orm` macro, which will take a db-schema and a body of code and evaluate that code with some db helpers.  The macro will dynamically bind symbols within the body that you can use to interact with the db, each of the tables defined in the db-schema will have their own read/write functions.  For the following examples we assume that the db-schema passed into with-orm has defined a table named `:employee`.

    (defn my-program [my-db-schema]
      (with-orm my-db-schema
        (let [new-employee (employee! { :name "Alex McNamara" })])))

This will create a new employee record in the database.  The object returned from this function is called a DBObject (DBO), and they're used to hold the state of a particular db row (in this case a `:employee` table's row, with the `:name` field value of "Alex McNamara").  Creating data is that easy, and one of these helpers will be bound for each table definition in the db-schema with via transformation of `:tablename` to `tablename!`, and accept a map of field:value pairs to persist a row.

Queries are just as simple, their helpers would be defined in the above example as `(employee <primary-key>)`, or `(employee <field-name> <field-value>)`.  Genereally, the helpers follow the transformation of `:tablename` to `tablename`, and accept single, double, or variadic args as shown above.  An example, using the above code as reference would be:

    (employee :name "Alex McNamara")

This would grab a sequence of all employee records with the name "Alex McNamara", and return (at least) the one defined in the exercise above.  Passing a single argument to student would query on that table's primary key, so `(employee 13)` would grab the row with `:id` of `13`, and return the single DBO.

> **NOTE**: Primary key queries (single argument) return a singleton DBO, whereas variadic calls return collections of DBOs for matching results.

The last two are more generic operations, `assoc*` takes a DBO and a map of field:value pairs and modifies the row in the database.  It returns a new DBO representing the changed state.  And `delete` takes a DBO and removes the row from the database.  Again, building on the examples from above we can mutate the `:name` field of the existing `new-employee` record and then delete that record entirely:

    (assoc* new-employee { :name "Alexander McNamara" })

    (delete new-employee)

That's it, these four operations should cover the _trivial_ use cases for dealing with DB records.  The current state of query support only enables PK and single-conditional queries; intersection and union conditionals were never implemented.  Writes (especially for _related_ records) should not be considered atomic transactions, and thus not concurrently safe.  There are lots of notes in the code describing potential improvements as well as areas where the implementation compromised robustness in favor of a simplified object data interface.


Examples
-------

There is some example code under `src/example/db_example.clj`, which can simply be run via:

    lein compile
    lein run
