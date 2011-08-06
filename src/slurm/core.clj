(ns slurm.core
  (:require [slurm.initialize])
  (:use     [clojure.contrib.string :only (as-str substring? lower-case)]
	    [slurm.internal]
	    [slurm.operations])
  (:gen-class))

;; Pull references
(def init slurm.initialize/init)

;; DB Access Protocols
(defprotocol IDBInfo
  "General info on the DBConnection, common data requests on schema and objects"
  (get-db-loading             [#^DBConnection dbconnection])
  (get-table-names            [#^DBConnection dbconnection])
  (get-table                  [#^DBConnection dbconnection table-name])
  (get-table-primary-key      [#^DBConnection dbconnection table-name])
  (get-table-primary-key-type [#^DBConnection dbconnection table-name])
  (get-table-primary-key-auto [#^DBConnection dbconnection table-name])
  (get-table-schema           [#^DBConnection dbconnection table-name])
  (get-table-fields           [#^DBConnection dbconnection table-name])
  (get-table-field-type       [#^DBConnection dbconnection table-name field-name])
  (get-table-field-load       [#^DBConnection dbconnection table-name field-name])
  (get-table-relations        [#^DBConnection dbconnection table-name])
  (get-table-one-relations    [#^DBConnection dbconnection table-name])
  (get-table-many-relations   [#^DBConnection dbconnection table-name]))

;; Access Objects for common introspection data on schema and load-graph
;; TODO: Get the load graph sorted out
(defrecord DBConnection [spec schema load-graph]
  IDBInfo
  (get-db-loading             [this]
			      (get schema :loading :lazy))
  (get-table-names            [_]
			      (map #(as-str (:name %)) (:tables schema)))
  (get-table                  [_ table-name]
			      (first (filter #(= (keyword table-name) (keyword (:name %))) (:tables schema))))
  (get-table-primary-key      [this table-name]
			      (or (-> (.get-table this table-name) :primary-key) "id"))
  (get-table-primary-key-type [this table-name]
			      (or (-> (.get-table this table-name) :primary-key-type) "int(11)"))
  (get-table-primary-key-auto [this table-name]
			      (or (-> (.get-table this table-name) :primary-key-auto-increment)
				  (substring? "int" (lower-case (name (.get-table-primary-key-type this table-name))))))
  (get-table-schema           [this table-name]
			      (-> (.get-table this table-name) :fields))
  (get-table-fields           [this table-name]
			      (or (keys (.get-table-schema this table-name)) []))
  (get-table-field-type       [this table-name field-name]
			      (or (let [type (get (.get-table-schema this table-name) (keyword field-name))]
				    (if (and (keyword? type) (= \* (first (name type))))
				      (keyword (apply str (rest (name type))))
				      type))
				  "int(11)"))
  (get-table-field-load       [this table-name field-name]
			      nil)
  (get-table-relations        [this table-name]
			      (or (into (.get-table-one-relations this table-name) (.get-table-many-relations this table-name)) []))
  (get-table-one-relations    [this table-name]
			      (or (keys (filter #(not= \* (first (apply name (rest %))))
						(filter #(apply keyword? (rest %)) (.get-table-schema this table-name)))) []))
  (get-table-many-relations   [this table-name]
			      (or (keys (filter #(= \* (first (apply name (rest %))))
						(filter #(apply keyword? (rest %)) (.get-table-schema this table-name)))) [])))

;; DBObject protocols (UD and field loading)
(defprotocol IDBRecord
  "Simple accessor for DBObjects, returns a column or relation object (loads if applicable/needed), and manages the access graph"
  (field  [#^DBObject dbobject column-name])
  (assoc* [#^DBObject dbobject new-columns])
  (delete [#^DBObject dbobject]))

;; Objects representing data mapping from DB rows
(defrecord DBObject [table-name primary-key columns]
  IDBRecord
  ;; TODO: Multi-relation fields should fetch with joins, current implementation in crazy inefficient
  (field  [this column-name]
	  (cond ;; Grab a single-relation field
	        (and (contains? (set (.get-table-one-relations (:dbconnection (meta this)) table-name)) (keyword column-name))
		     (not (instance? (type this) (get columns (keyword column-name)))))
		(first (select-db-record (:dbconnection (meta this))
					 (.get-table-field-type       (:dbconnection (meta this)) table-name column-name)
					 (.get-table-primary-key      (:dbconnection (meta this)) (.get-table-field-type (:dbconnection (meta this)) table-name column-name))
					 (.get-table-primary-key      (:dbconnection (meta this)) (.get-table-field-type (:dbconnection (meta this)) table-name column-name))
					 (.get-table-primary-key-type (:dbconnection (meta this)) (.get-table-field-type (:dbconnection (meta this)) table-name column-name))
					 :=
					 (get columns (keyword column-name))))
		;; Grab a multi-relation field
		(contains? (set (.get-table-many-relations (:dbconnection (meta this)) table-name)) (keyword column-name))
		(for [foreign-key (map :primary-key
				       (select-db-record (:dbconnection (meta this))
							 (generate-relation-table-name table-name (.get-table-field-type  (:dbconnection (meta this)) table-name column-name))
							 :id
							 (generate-relation-key-name   table-name (.get-table-primary-key (:dbconnection (meta this)) table-name))
							 (.get-table-primary-key-type  (:dbconnection (meta this)) (.get-table-field-type (:dbconnection (meta this)) table-name column-name))
							 :=
							 primary-key))]
		  (first (select-db-record (:dbconnection (meta this))
					   (.get-table-field-type       (:dbconnection (meta this)) table-name column-name)
					   (.get-table-primary-key      (:dbconnection (meta this)) (.get-table-field-type (:dbconnection (meta this)) table-name column-name))
					   (.get-table-primary-key      (:dbconnection (meta this)) (.get-table-field-type (:dbconnection (meta this)) table-name column-name))
					   (.get-table-primary-key-type (:dbconnection (meta this)) (.get-table-field-type (:dbconnection (meta this)) table-name column-name))
					   :=
					   foreign-key)))
		;; Grab a non-relation field
		:else (get columns (keyword column-name))))
  (assoc* [this new-columns]
	  (if (empty? new-columns)
	    this
	    (do
	      (update-db-record (:dbconnection (meta this))
				table-name
				(.get-table-primary-key      (:dbconnection (meta this)) table-name)
				(.get-table-primary-key-type (:dbconnection (meta this)) table-name)
				primary-key
				new-columns)
	      (DBObject. table-name primary-key (into columns new-columns)))))
  (delete [this]
	  (delete-db-record (:dbconnection (meta this))
			    table-name
			    (.get-table-primary-key      (:dbconnection (meta this)) table-name)
			    (.get-table-primary-key-type (:dbconnection (meta this)) table-name)
			    primary-key)))

;; Object outlining the construction of a DB row
(defrecord DBConstruct [table-name columns])

;; Object outlining a fetch clause on a db table
(defrecord DBClause [table-name column-name operator value])

;; ORM proper (CR only, UD is on DBOs)
(defprotocol ISlurm
  "Simple CRUD interface for dealing with slurm objects"
  (create [#^SlurmDB slurmdb #^DBConstruct dbconstruct])
  (fetch  [#^SlurmDB slurmdb #^DBClause dbclause]))

;; Representation of DB access
;; TODO: Lots of error checking on this
(defrecord SlurmDB [#^DBConnection dbconnection]
  ISlurm
  (create [_ dbconstruct]
	  (insert-db-record dbconnection
			    (:table-name dbconstruct)
			    (:columns dbconstruct)))
  (fetch  [_ dbclause]
	  (select-db-record dbconnection
			    (:table-name dbclause)
			    (.get-table-primary-key dbconnection (name (:table-name dbclause)))
			    (:column-name dbclause)
			    (.get-table-field-type dbconnection  (name (:table-name dbclause)) (name (:column-name dbclause)))
			    (or (:operator dbclause) :=)
			    (:value dbclause))))

;; Command-line Interface (used to initialize schemas only)
;; TODO: at some point add a REPL to allow playing with the DB through the CLI
(defn -main [& args]
  (let [schema-file (first args)]
    (do
      (if (nil? schema-file)
	(println "Slurm command-line utility used to verify and initialize schema definition.\nUsage: java -jar slurm.jar <schema-filename>")
	(let [orm (init (try (slurp schema-file) (catch Exception e (println "Could not load schema file.\nTrace:" e))))]
	  (do
	    (println "Database schema successfully initialized")))))))