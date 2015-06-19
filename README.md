SLURM
=====

**Slurm is deprecated, and hasn't been updated to modern versions of Clojure. I'm leaving it up as a proof of concept for some neat (but not necessarily good) ideas.**

Slurm is a Clojure ORM.  Its modelled more closely to a traditional OO ORM than a Clojure DSL, specifically to help beginners get off the ground.  It features the following:

* Selectable eager or lazy loading of database objects
* Seamless single and multi object relations
* Simple to define schemas, makes for easy adoption on existing projects/DBs
* Simple interaction mechanisms.
* Allows easy sequence processing of database records

Slurm is in active development, please check back for updates often.  If you need any help or have feedback, you can ping me at alex.mcnamara@dreamux.com.

Overview
--------

The first thing you need to do is define a schema for your DB; this includes credentials and connection information, as well as definitions of all tables and columns.  The schema is defined in a standard hash-map as follows:

    {:db-host <ip-address>  (default: localhost)
     :db-port <port-number> (default: 3306)
     :db-user <username>    (default: root)
     :db-pass <password>    (default: n/a)
     :loading eager|lazy    (default: lazy)
     :tables  #{<tables>}}

The root-level of the db-schema map concerns db credentials, the value of :tables is a collection of table definitions described as follows:

    {:name                       <table-name>
     :primary-key                <key-name>   (default: :id)
     :primary-key-type           <type>       (default: "int(11)")
     :primary-key-auto-increment true|false   (default: true)
     :fields                     {<field-map}}

Now that you've defined the table outlines, you need to fill in the fields that the table will hold, as follows:

    {<field-name> <field-type>, ...}

Note that all field names (and table names for that matter) should be keywordized.  Field types can either be SQL primatives (described below), or references to other tables.  Single references (ie. the field will point to at most a single row in the foreign table) are defined by putting the keword table-name of the table being referenced into the field-type value.  Muli reference fields (ie. one which contains reference to many rows of a different table) are defined with the keyword table-name prefixed with an asterisk, so a field called :enrolled_students with a multi relation to a table named :student may be defined as follows:

    {:enrolled_students :*student, ...}

Fields which represent standard SQL types may have the following field-types:

    "int(n)"
    "varchar(n)"
    "bool"
    "date"
    "datetime"
    "blob"...

> NOTE: n is a number usually representing the size of the data which can be saved into the field.  For example, "varchar(44)" would allow you to store a string of max 44 characters in that field.

> SEE:  MySQL documentation for further details on supported types.

A complete example of a schema definition can be found under src/example/db_schema, it may help clarify some of the naming conventions described here.  To see it in use launch the script under src/example/db_example.clj with the instructions given below.

Interaction
-----------

Now that you've got your schema defined you're ready to use slurm!  Run lein jar from the slurm source folder and move the resulting jar into your project's lib folder (will be added to clojars once stable).

Interacting with your database is extremely easy and can all be done from within the with-orm macro, which will take a db-schema and a body of code and evaluate that code with some db helpers.  The macro will bind some symbols within the body that you can use to interact with the db, each of the tables defined in the db-schema will have their own helper functions; for the following examples we assume that the db-schema passed into with-orm has defined a table named :student.

    (defn my-program [my-db-schema]
      (with-orm my-db-schema
        (let [new-student (student! {:name "Alex McNamara"})])))

This will create a new student record in the database.  The object returned from this function is called a DBObject, and they're used to hold the state of a particular db row (in this case a :student table's row, with the :name field valie of "Alex McNamara").  Creating data is that easy, and one of these helpers will be bound for each table definition in the db-schema with via transformation of :tablename to tablename!, and accept a map of field/value pairs to save.

Queries are just as simple, their helpers would be defined in the above example as (student pk), or (student field-name field-value).  Genereally, the helpers follow the transformation of :tablename to tablename, and accept single, double, or variadic args as shown above.  An example, using the above code as reference would be:

    (student :name "Alex McNamara")

This would grab a sequence of all studend records with the name "Alex McNamara", and return (at least) the one defined in the exercise above.  Passing a single argument to student would query on that table's primary key, so (student 13) would grab the row with :id of 13, and return the single DBO.

> NOTE: Primary key queries (single argument) return a single DBO, whereas variadic calls return collections of DBOs for matching results.

The last two are more generic operations, assoc* takes a dbo and a map of field/value pairs and modifies the row in the database and returns a new dbo representing the changed state.  And delete takes a dbo and removes the row from the database.  Again, building on the examples from above:

    (assoc* new-student {:name "Billy G"})

    (delete new-student)

That's it, four operations should cover almost all use cases for dealing with db data.  Eventually the query support will become more sophisticated, and updates more concurrency-safe, but the current feature set should be enough to get most projects started.  Please send feedback and/or feature requests once you've played with it for a while.

Examples
-------

There is some example code under src/example/db_example.clj, which can simply be run via:

    lein compile
    lein run
