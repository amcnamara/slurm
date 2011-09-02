(ns slurm.orm
  (:require [slurm.connection       :as connection]
	    [clojure.contrib.sql    :as sql])
  (:import  [slurm.connection       DBConnection])
  (:use     [slurm.internal]
	    [clojure.contrib.string :only (as-str)]))

;; Forward declaration of record operations
(declare insert-db-record
	 select-db-record
	 update-db-record
	 delete-db-record)

;; ORM proper (CR only, UD is on DBOs)
(defprotocol ISlurm
  ;;Simple CRud interface for dealing with slurm DBOs.  Create takes a DBConstruct
  ;;and creates a new record in the DB and returns a DBO representation of the data.
  ;;Fetch takes a DBClause and returns a sequence of DBOs representing the record
  ;;result-set of the given query operation.
  (create [#^SlurmDB slurmdb #^DBConstruct dbconstruct])
  (fetch  [#^SlurmDB slurmdb #^DBClause dbclause]))

(defrecord SlurmDB [#^DBConnection dbconnection]
  ;;SlurmDB is a representation of DB connection/access and object mapping, operations defined in ISlurm.
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

;; DBObject protocol and implementation (UD and field loading)
(defprotocol IDBRecord
  ;;Simple accessor for DBObjects.  Field returns a column or relation object (loads if
  ;;applicable/needed), and manages the access graph. Assoc* takes a map of column/values
  ;;and updates the record in the DB, and returns a new DBO representing the changed
  ;;state.  Delete removes the record from the database."
  (field  [#^DBObject dbobject column-name])
  (assoc* [#^DBObject dbobject new-columns])
  (delete [#^DBObject dbobject]))

(defrecord DBObject
  ;;DBOs are representations of data mappings from DB records, operations defined in IDBRecord.
  [table-name primary-key columns]
  IDBRecord
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
	      (with-meta (DBObject. table-name primary-key (into columns new-columns)) (meta this)))))
  (delete [this]
	  (delete-db-record (:dbconnection (meta this))
			    table-name
			    (.get-table-primary-key      (:dbconnection (meta this)) table-name)
			    (.get-table-primary-key-type (:dbconnection (meta this)) table-name)
			    primary-key)))

;; Record Operations
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
  (let [request (join-as-str " " :SELECT :* :FROM table-name :WHERE column-name (or operator :=) (escape-field-value value column-type))]
    (sql/with-connection (:spec dbconnection)
      (sql/with-query-results query-results
	[request]
	(doall (for [result query-results]
		 (let [primary-key (get result (keyword table-primary-key) "NULL") ;; TODO: fire off a warning on no PK
		       base-dbo    (with-meta (DBObject. (keyword table-name) primary-key (dissoc (into {} result) (keyword table-primary-key))) {:dbconnection dbconnection})
		       relations   (if (= :eager (.get-db-loading dbconnection))
				     (for [relation (.get-table-relations dbconnection table-name)]
				       [relation (.field base-dbo relation)]))]
		   ;; Inject relation objects (if applicable) into the DBO
		   (into base-dbo (into (:columns base-dbo) relations)))))))))
		   
;; TODO: create a transaction and add hierarchy of changes to include relations (nb. nested transactions escape up)
;; TODO: make typechecking (strings must escape!) more rigorous by comparing with schema instead of value
(defn- update-db-record [dbconnection table-name table-primary-key table-primary-key-type table-primary-key-value columns]
  (assert (not-empty columns))
  (let [columns* (apply dissoc columns (.get-table-many-relations dbconnection table-name))
        name-sig (reduce as-str (interpose ", " (map #(as-str % :=?) (keys columns*))))
        columns* (flatten (reduce into (for [[key value] columns*] [(escape-field-value (or (:primary-key value) value))])))
	columns* (concat columns* [(escape-field-value table-primary-key-value table-primary-key-type)])
	request  (join-as-str " " :UPDATE table-name :SET name-sig :WHERE table-primary-key :=?)]
    (sql/with-connection (:spec dbconnection)
      (sql/transaction
       (if (not-empty columns*)
	 (sql/do-prepared request columns*))
       ;; Update multi-relation intermediary tables
       (doseq [relation (.get-table-many-relations dbconnection table-name)]
	 ;; If the updaded column map contains multi relation keys, update the intermediary tables accordingly
	 (if (contains? columns relation)
	   (let [local-key-name      (generate-relation-key-name table-name table-primary-key)
		 foreign-key-name    (generate-relation-key-name (.get-table-field-type dbconnection table-name relation)
								 (.get-table-primary-key dbconnection relation))
		 insert-binding      (sql-list [local-key-name foreign-key-name])
		 relation-table-name (generate-relation-table-name table-name (.get-table-field-type dbconnection table-name relation)) 
		 relation-keys       (map #(or (:primary-key %) %) (get columns relation))
		 value-sig           (reduce str (interpose ", " (repeat (count relation-keys) (prepare-sql-list 2))))
		 value-map           (flatten (map #(vector table-primary-key-value %) relation-keys))
		 delete-request      (join-as-str " " :DELETE :FROM relation-table-name :WHERE local-key-name :=?)
		 insert-request      (join-as-str " " :INSERT :INTO relation-table-name insert-binding :VALUES value-sig)]
	     (do
	       (sql/do-prepared delete-request [(escape-field-value table-primary-key-value)])
	       (if (not-empty value-map)
		 (sql/do-prepared insert-request value-map))))))))))

;; TODO: need manual cleanup of relation tables for MyISAM (foreign constraints should kick in for InnoDB)
(defn- delete-db-record [dbconnection table-name primary-key primary-key-type primary-key-value]
  (sql/with-connection (:spec dbconnection)
    (let [request (join-as-str " " :DELETE :FROM table-name :WHERE primary-key :=?)]
      (sql/do-prepared request [(escape-field-value primary-key-value primary-key-type)]))))