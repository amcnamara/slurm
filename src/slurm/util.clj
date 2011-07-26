(ns slurm.util
  (:require [clojure.contrib.sql    :as sql]
	    [slurm.error            :as err])
  (:use     [clojure.contrib.error-kit]
	    [clojure.contrib.string :only (join substring? lower-case as-str)]))

(defn exists-db?
  "Checks that a given DB exists on the host"
  [db-connection-spec root-subname db-name]
  (with-handler
    (try
      (let [db-name (name db-name)
	    request (str "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = \"" db-name "\"")
	    db-root (into db-connection-spec {:subname root-subname})]
	(sql/with-connection db-root
	  (sql/with-query-results query-results
	    [request]
	    (not (empty? query-results)))))
      (catch Exception e
	(raise err/SchemaError e (str "Error verifying db (" (db-name) ") on host"))))
    (handle err/SchemaError [])))
  
(defn exists-table?
  "Checks that a given table exists on the given host/db"
  [db-connection-spec root-subname db-name table-name]
  (with-handler
    (try
      (let [db-name (name db-name)
	    table-name (name table-name)
	    request (str "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"" db-name "\" AND TABLE_NAME = \"" table-name "\"")
	    db-root (into db-connection-spec {:subname root-subname})]
	(sql/with-connection db-root
	  (sql/with-query-results query-results
	    [request]
	    (not (empty? query-results)))))
      (catch Exception e
	(raise err/SchemaError e (str "Error verifying table (" (name table-name) ") on host/db (" (name db-name) ")"))))
    (handle err/SchemaError [])))

(defn create-db
  "Attempts to create a given DB"
  [db-connection-spec root-subname db-name]
  (with-handler
    (try
      (let [db-name (name db-name)
	    request (str "CREATE DATABASE IF NOT EXISTS " db-name)
	    db-root (into db-connection-spec {:subname root-subname})] ;; drops db-name from connection request, otherwise will hit db-not-found exception on create
	(sql/with-connection db-root
	  (sql/do-commands request))) ;; TODO: how to verify this worked other than not hitting an exception?
      (catch Exception e
	(raise err/SchemaErrorBadDBName e (str "Could not create database (" (name db-name) "), probably due to a badly formed database name."))))
    (handle err/SchemaError [])))

(defn create-table
  "Attempts to create a table with the specified schema"
  [db-connection-spec table-name & table-schema]
  (with-handler
    (try
      (sql/with-connection db-connection-spec
	(apply sql/create-table table-name table-schema))
      (catch Exception e
	(raise err/SchemaErrorBadTableName e (str "Could not create table (" (name table-name) "), probably due to a badly formed table name."))))
    (handle err/SchemaError [])))

;; TODO: use this to verify slurm schema is consistent with database schema
;; IDEA: if not consistent, use descriptions to generate a new/updated slurm schema -- this would greatly reduce adoption pain for live dbs
(defn describe-table
  "Fetches a table description, returns a seq of column-name/type pairs"
  [db-connection-spec table-name]
  (let [table-name (name table-name)
	request (str "DESCRIBE " table-name)]
    (try
      (sql/with-connection db-connection-spec
	(sql/with-query-results query-results
	  [request]
	  (doall (for [query-record query-results] (select-keys query-record [:field :type])))))
      (catch com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException e false)))) ;; TODO: what breaks here other than table-not-found?

;; TODO: add this
(defn dump-load-graph [] ())

(defn escape-field-value [value column-type]
  (if (substring? "varchar" (lower-case (name column-type)))
    (str "\"" value "\"")
    value))

(defn join-as-str [separator & coll]
  (join separator (map as-str coll)))

(defn generate-relation-table-name [table-name-from table-name-to]
  "Formats a keyword to represent a one-to-many table name from the two related tables"
  ;;used only on :many-to-many intermediate tables
  (keyword (str (name table-name-from) (name table-name-to))))

(defn generate-relation-key-name [table-name primary-key-name]
  "Formats a keyword to represent a foreign key column name"
  ;;used in both :one-to-one and :one-to-many relations
  (keyword (str (name table-name) "_" (name primary-key-name))))

(defn generate-foreign-key-constraint [key-name foreign-table-name foreign-primary-key]
  (vector "FOREIGN KEY" (str "(" (name key-name) ")")
	  "REFERENCES" (str (name foreign-table-name) "("
			    (name foreign-primary-key) ")")))

(defn generate-foreign-key-constraint-cascade-delete [key-name foreign-table-name foreign-primary-key]
  (conj (generate-foreign-key-constraint key-name foreign-table-name foreign-primary-key)
	(str "ON DELETE CASCADE")))

(def valid-schema-db-keys
  #{:db-server-pool
    :db-port
    :db-name
    :db-user
    :db-password
    :loading
    :tables})

(def valid-schema-table-keys
  #{:name
    :primary-key
    :primary-key-type
    :primary-key-auto-increment
    :fields
    :index})