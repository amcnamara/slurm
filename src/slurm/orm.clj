(ns slurm.orm
  (:require [slurm.connection       :as connect]
	    [clojure.contrib.sql    :as sql])
  (:import  [slurm.connection       DBConnection])
  (:use     [clojure.contrib.string :only (as-str substring? lower-case)]
	    [slurm.internal])
  (:gen-class))

;; Forward declaration of record operations
(declare insert-db-record
	 select-db-record
	 update-db-record
	 delete-db-record)

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

;; Record Operations
;; TODO: recursively insert relations, adding the returned DBObject to the parent relation key
;; TODO: this is a sketchy way to pull the pk on the return object, refactor this.
(defn- insert-db-record [dbconnection table-name record]
  (sql/with-connection (:spec dbconnection)
    (sql/transaction
     (let [;; Remove any columns that aren't in the schema definition
	   columns  (try (reduce into
				 (map #(if (not (nil? (record (keyword %))))
					 (hash-map % (record %)))
				      (.get-table-fields dbconnection table-name)))
			 (catch IllegalArgumentException e record)) ;; NOTE: Breaks on multi relation tables, since those tables aren't in the schema getters
	   ;; Find single relations and load their primary key into the record
	   columns* (into columns
			  (reduce into
				  (or (for [[key value] columns]
					(if (and (contains? (set (.get-table-one-relations dbconnection table-name)) (keyword key))
						 (instance? DBObject value))
					  (hash-map (keyword key) (:primary-key value)))) [])))
	   ;; Remove the multi-relation fields from the insert set
	   columns* (filter #(not (contains? (set (.get-table-many-relations dbconnection table-name)) (keyword (first %)))) columns*)]
       (sql/insert-records table-name columns*)
       (sql/with-query-results query-results
	 ;; NOTE: this should return independently on each connection/transaction, races shouldn't be an issue (must verify this)
	 ["SELECT LAST_INSERT_ID()"]
	 ;; Insert multi-relation records
	 (doall
	  (for [relation (.get-table-many-relations dbconnection table-name)]
	    (doall
	     (for [foreign-object (get columns relation)]
	       (insert-db-record dbconnection
				 (generate-relation-table-name         table-name (.get-table-field-type  dbconnection table-name   relation))
				 (hash-map (generate-relation-key-name table-name (.get-table-primary-key dbconnection table-name))
					   (first (apply vals query-results))
					   (generate-relation-key-name (.get-table-field-type dbconnection table-name relation) (.get-table-primary-key dbconnection relation))
					   (if (instance? DBObject foreign-object) (:primary-key foreign-object) foreign-object)))))))
	 ;; NOTE: Original record map still contains foreign objects instead of foreign primary keys for single relations
	 (with-meta (DBObject. (keyword table-name) (first (apply vals query-results)) (into {} columns)) {:dbconnection dbconnection})))))) 

(defn- select-db-record [dbconnection table-name table-primary-key column-name column-type operator value]
  (sql/with-connection (:spec dbconnection)
    (sql/with-query-results query-results
      [(join-as-str " " "SELECT * FROM"
		        table-name
			"WHERE"
			column-name
			(or operator :=)
			(escape-field-value value column-type))]
      (doall (for [result query-results]
	       (let [primary-key (get result (keyword table-primary-key) "NULL") ;; TODO: fire off a warning on no PK
		     base-dbo    (with-meta (DBObject. (keyword table-name) primary-key (dissoc (into {} result) (keyword table-primary-key))) {:dbconnection dbconnection})
		     relations   (if (= :eager (.get-db-loading dbconnection))
				   (for [relation (.get-table-relations dbconnection table-name)]
				     [relation (.field base-dbo relation)]))]
		 ;; Inject relation objects (if applicable) into the DBO
		 (into base-dbo (into (:columns base-dbo) relations))))))))
		   
;; TODO: create a transaction and add hierarchy of changes to include relations (nb. nested transactions escape up)
;; TODO: make typechecking (strings must escape!) more rigorous by comparing with schema instead of value (consider making this a helper)
(defn- update-db-record [dbconnection table-name table-primary-key table-primary-key-type table-primary-key-value columns]
  (let [columns (dissoc columns (keyword table-primary-key))
	columns (into columns
		      (for [[key value] columns]
			(if (and (contains? (set (.get-table-one-relations dbconnection table-name)) (keyword key))
				 (instance? DBObject value))
			  [key (:primary-key (first
					      (select-db-record dbconnection
								(.get-table-field-type       dbconnection table-name key)
								(.get-table-primary-key      dbconnection (.get-table-field-type dbconnection table-name key))
								(.get-table-primary-key      dbconnection (.get-table-field-type dbconnection table-name key))
								(.get-table-primary-key-type dbconnection (.get-table-field-type dbconnection table-name key))
								:=
								(:primary-key value))))])))
        columns (apply (partial join-as-str ", ")
		       (filter (complement nil?)
			       (for [[column-name column-value] columns]
				 (cond (string? column-value)    (as-str column-name " = \"" column-value "\"")
				       (not (nil? column-value)) (as-str column-name " = "   column-value)))))]
    (sql/with-connection (:spec dbconnection)
      (sql/do-commands (join-as-str " " "UPDATE"
				        table-name
					"SET"
				        columns
					"WHERE"
					table-primary-key
					"="
					(escape-field-value table-primary-key-value table-primary-key-type))))))
		     
;; TODO: need manual cleanup of relation tables for MyISAM (foreign constraints should kick in for InnoDB)
(defn- delete-db-record [dbconnection table-name primary-key primary-key-type primary-key-value]
  (sql/with-connection (:spec dbconnection)
    (sql/do-commands (join-as-str " " "DELETE FROM"
				      table-name
				      "WHERE"
				      primary-key
				      "="
				      (escape-field-value primary-key-value primary-key-type)))))