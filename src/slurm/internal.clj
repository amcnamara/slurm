(ns slurm.internal
  (:require [clojure.contrib.sql       :as sql]
	    [slurm.error               :as err])
  (:use     [clojure.contrib.error-kit :only (with-handler handle raise)]
	    [clojure.contrib.string    :only (join substring? lower-case as-str)])
  (:gen-class))

(defn sym [name]
  "Coerces into a symbol"
  (symbol (as-str name)))

(defn sym-bang [name]
  "Coerces into a symbol with appended exclamation"
  (symbol (as-str name "!")))

(defn bind-map [hmap]
  "Creates a binding-like vector of symbol/value pairs from a hash-map"
  (apply vector (interleave (map sym (keys hmap)) (vals hmap))))

(defn exists-db?
  "Checks that a given DB exists on the host"
  [db-connection-spec root-subname db-name]
  (with-handler
    (try
      (let [request (as-str "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = \"" db-name "\"")
	    db-root (into db-connection-spec {:subname root-subname})] ;; Drops db-name from connection request, otherwise will hit db-not-found exception on create
	(sql/with-connection db-root
	  (sql/with-query-results query-results
	    [request]
	    (not (empty? query-results)))))
      (catch Exception e
	(raise err/SchemaError e (as-str "Error verifying db (" db-name ") on host"))))
    (handle err/SchemaError [])))
  
(defn exists-table?
  "Checks that a given table exists on the host/db"
  [db-connection-spec root-subname db-name table-name]
  (with-handler
    (try
      (let [request (as-str  "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"" db-name "\" AND TABLE_NAME = \"" table-name "\"")
	    db-root (into db-connection-spec {:subname root-subname})] 
	(sql/with-connection db-root
	  (sql/with-query-results query-results
	    [request]
	    (not (empty? query-results)))))
      (catch Exception e
	(raise err/SchemaError e (as-str "Error verifying table (" table-name ") on host/db (" db-name ")"))))
    (handle err/SchemaError [])))

(defn create-db
  "Attempts to create a DB if needed."
  [db-connection-spec root-subname db-name]
  (with-handler
    (try
      (let [request (as-str "CREATE DATABASE IF NOT EXISTS " db-name)
	    db-root (into db-connection-spec {:subname root-subname})]
	(sql/with-connection db-root
	  (sql/do-commands request))) ;; TODO: how to verify this other than hitting an exception?
      (catch Exception e
	(raise err/SchemaErrorBadDBName e (as-str "Could not create database (" db-name "), probably due to a badly formed database name."))))
    (handle err/SchemaError [])))

(defn create-table
  "Attempts to create a table with the specified schema."
  [db-connection-spec table-name & table-schema]
  (with-handler
    (try
      (sql/with-connection db-connection-spec
	(apply sql/create-table table-name table-schema))
      (catch Exception e
	(raise err/SchemaErrorBadTableName e (as-str "Could not create table (" table-name "), probably due to a badly formed table name."))))
    (handle err/SchemaError [])))

;; TODO: Use this to verify slurm schema is consistent with database schema
;; IDEA: If not consistent, use descriptions to generate a new/updated
;;       slurm schema.  This would greatly reduce adoption pain for live dbs
(defn describe-table
  "Fetches a table description, returns a seq of column-name/type pairs."
  [db-connection-spec table-name]
  (let [table-name (name table-name)
	request    (str "DESCRIBE " table-name)]
    (try
      (sql/with-connection db-connection-spec
	(sql/with-query-results query-results
	  [request]
	  (doall (for [query-record query-results] (select-keys query-record [:field :type])))))
      (catch com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException e false)))) ;; TODO: what breaks here other than table-not-found?

;; TODO: think about this
(defn dump-load-graph [] ())

(defn escape-field-value
  "Takes a sql input value and escapes it as needed."
  ([value]
     ;; If no type is provided (ie. from the schema) infer the type from the value
     ;; TODO: This isn't rigorous, and might lead to confusing results. Change?
     (escape-field-value value (type value)))
  ([value column-type]
     (if (nil? value)
       0
       (if (or (substring? "varchar" (lower-case (as-str column-type)))
	       (substring? "string"  (lower-case (as-str column-type))))
	 (str "\"" value "\"")
	 value))))

(defn join-as-str [separator & coll]
  (join separator (map as-str coll)))

(defn generate-relation-table-name [table-name-from table-name-to]
  "Formats a keyword to represent a one-to-many table name from the two related tables
   NOTE: This is only used in naming 1:* intermediary relation tables."
  (keyword (as-str table-name-from table-name-to)))

(defn generate-relation-key-name [table-name primary-key-name]
  "Formats a keyword to represent a foreign key column name.
   NOTE: This formatting is used for both 1:1 and 1:* relation key names"
  (keyword (as-str table-name "_" primary-key-name)))

(defn generate-foreign-key-constraint [key-name foreign-table-name foreign-primary-key]
  "Generates a db-build-time sql instruction to create a foreign key"
  (vector "FOREIGN KEY" (as-str "(" key-name ")")
	  "REFERENCES"  (as-str foreign-table-name "(" foreign-primary-key ")")))

(defn generate-foreign-key-constraint-cascade-delete
  "Generates a db-build-time sql instruction to create a foreign key with a cascade
   deletion clause."
  [key-name foreign-table-name foreign-primary-key]
  (conj (generate-foreign-key-constraint key-name foreign-table-name foreign-primary-key)
	"ON DELETE CASCADE"))

(defn sql-list
  "Generates a comma-separated, escaped, and bracket wrapped string for use in sql
   queries from a clojure sequence of inputs."
  [coll]
  (assert (coll? coll))
  (str "(" (reduce as-str (interpose ", " (map escape-field-value coll))) ")"))

(defn prepare-sql-list
  "Generates a prepared-statement formatted sql-list, with n arguments."
  [n]
  (assert (integer? n))
  (sql-list (repeat n \?)))

;; This set contains the valid keys at the root-level of a db-schema definition
(def valid-schema-db-keys
  #{:db-server-pool
    :db-port
    :db-name
    :db-user
    :db-password
    :loading
    :tables})

;; This set contains the valid keys af the table-level of a db-schema definition
(def valid-schema-table-keys
  #{:name
    :primary-key
    :primary-key-type
    :primary-key-auto-increment
    :fields
    :index})