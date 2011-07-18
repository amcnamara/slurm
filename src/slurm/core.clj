(ns slurm.core
  (:gen-class)
  (:require [clojure.contrib.sql :as sql]
	    [clojure.contrib.string :as str-tools])
  (:use [clojure.contrib.error-kit]
	[slurm.error]
	[slurm.util]))

;; DB Access Objects
(defprotocol IDBInfo
  "General info on the DBConnection, common data requests on schema and objects"
  (get-table-names            [#^DBConnection db-connection])
  (get-table                  [#^DBConnection db-connection table-name])
  (get-table-primary-key      [#^DBConnection db-connection table-name])
  (get-table-primary-key-type [#^DBConnection db-connection table-name])
  (get-table-primary-key-auto [#^DBConnection db-connection table-name])
  (get-table-fields           [#^DBConnection db-connection table-name])
  (get-table-one-relations    [#^DBConnection db-connection table-name])
  (get-table-many-relations   [#^DBConnection db-connection table-name])
  (get-field-type             [#^DBConnection db-connection table-name field-name])
  (get-field-load             [#^DBConnection db-connection table-name field-name]))

;; Functions for returning common introspection data on schema and load-graph
(defrecord DBConnection [spec schema load-graph]
  IDBInfo
  (get-table-names            [_]
			      (map #(str-tools/as-str (:name %)) (:tables schema)))
  (get-table                  [_ table-name]
			      (first (filter #(= (keyword table-name) (keyword (:name %))) (:tables schema))))
  (get-table-primary-key      [this table-name]
			      (or (-> (.get-table this table-name) :primary-key) "id"))
  (get-table-primary-key-type [this table-name]
			      (or (-> (.get-table this table-name) :primary-key-type) "int(11)"))
  (get-table-primary-key-auto [this table-name]
			      (or (-> (.get-table this table-name) :primary-key-auto-increment)
				  (str-tools/substring? "int" (str-tools/lower-case (name (.get-table-primary-key-type this table-name))))))
  (get-table-fields           [this table-name]
			      (-> (.get-table this table-name) :schema))
  (get-table-one-relations    [this table-name]
			      (-> (.get-table this table-name) :relations :one-to-one))
  (get-table-many-relations   [this table-name]
			      (-> (.get-table this table-name) :relations :one-to-many))
  (get-field-type             [this table-name field-name]
			      (get (.get-table-fields this table-name) (keyword field-name)))
  (get-field-load             [this table-name field-name]
			      nil)) ;; TODO: get load-graph sorted out

(defprotocol IDBField
  "Simple accessor for DBObjects, returns a column or relation object (loads if applicable/needed), and manages the access graph"
  (field [#^DBObject object field]))

(defrecord DBObject [table-name primary-key columns]
  IDBField
  (field [_ column-name] (get columns (keyword column-name)))) ;; TODO: lazies should return a clause, then load and return result on fetch

(defrecord DBConstruct [table-name columns])

(defrecord DBClause [table-name column-name operator value])

;; Record Operations
;; TODO: recursively insert relations, adding the returned DBObject to the parent relation key
(defn- insert-db-record [db-connection-spec table-name record]
  (sql/with-connection db-connection-spec
    (sql/transaction
     (sql/insert-records
      table-name
      record)
     (let [request "SELECT LAST_INSERT_ID()"] ;; NOTE: this should return independently on each connection, races shouldn't be an issue (should verify this)
       (sql/with-query-results query-results
	 [request]
	 (DBObject. (keyword table-name) (first (apply vals query-results)) (into {} record))))))) ;; TODO: kill PK in column struct

(defn- select-db-record [db-connection-spec table-name table-primary-key column-name column-type operator value]
  (let [table-name (name table-name)
	table-primary-key (name table-primary-key)
	column-name (name column-name)
	column-type (name column-type) ;; NOTE: Column type matters when querying strings (must be escaped) and for
	                               ;;       some advanced queries (eg. between clauses) which shouldn't be escaped.
	operator (name (or operator :=))
	column-type-is-string (str-tools/substring? "varchar" (str-tools/lower-case column-type))
	escaped-value (if column-type-is-string
			(str "\"" value "\"")
			value)
	request (str "SELECT * FROM " table-name " WHERE " column-name " " operator " " escaped-value)] ;; TODO: Create a helper to sanitize clauses
    (println "SQL -> " request)
    (do
      (sql/with-connection db-connection-spec
	(sql/with-query-results query-results
	  [request]
	  (doall (for [result query-results]
		   (let [primary-key (get result (keyword table-primary-key) "NULL") ;; TODO: fire off a warning on no PK
			 columns (dissoc (into {} result) (keyword table-primary-key))]
		     (DBObject. (keyword table-name) primary-key columns)))))))))

(defn- update-db-record [db-connection-spec table-name primary-key primary-key-type primary-key-value columns]
  (let [table-name (name table-name)
	primary-key (name primary-key)
	primary-key-type (name primary-key-type)
	primary-key-type-is-string (str-tools/substring? "varchar" (str-tools/lower-case primary-key-type))
	escaped-primary-key-value (if primary-key-type-is-string
				    (str "\"" primary-key-value "\"")
				    primary-key-value)
	escaped-column-values (filter (complement nil?)
				      (for [[column-name column-value] columns]
					(cond (string? column-value)    (str (name column-name) " = \"" column-value "\"") ;; TODO: make this more rigorous by comparing type with schema instead of value
					      (not (nil? column-value)) (str (name column-name) " = " column-value))))
	formatted-columns (apply str (interpose ", " escaped-column-values))
	request (str "UPDATE " table-name " SET " formatted-columns " WHERE " primary-key " = " escaped-primary-key-value)]
    (println "SQL -> " request)
    (sql/with-connection db-connection-spec
      (sql/do-commands request)))) ;; TODO: create a transaction and add hierarchy of changes to include relations (nb. nested transactions escape up)

;; TODO: need manual cleanup of relation tables for MyISAM (foreign constraints should kick in for InnoDB)
(defn- delete-db-record [db-connection-spec table-name primary-key primary-key-type primary-key-value]
  (let [table-name (name table-name)
	primary-key (name primary-key)
	primary-key-type (name primary-key-type)
	primary-key-type-is-string (str-tools/substring? "varchar" (str-tools/lower-case primary-key-type))
	escaped-primary-key-value (if primary-key-type-is-string
				    (str "\"" primary-key-value "\"")
				    primary-key-value)
	request (str "DELETE FROM " table-name " WHERE " primary-key " = " escaped-primary-key-value)]
    (println "SQL -> " request)
    (sql/with-connection db-connection-spec
      (sql/do-commands request))))

;; ORM Interface (proper)
(defprotocol ISlurm
  "Simple CRUD interface for dealing with slurm objects"
  (create [#^DBConnection SlurmDB #^DBConstruct object])
  (fetch  [#^DBConnection SlurmDB #^DBClause clause])
  (update [#^DBConnection SlurmDB #^DBObject object])
  (delete [#^DBConnection SlurmDB #^DBObject object]))

;; TODO: Lots of error checking on this
(defrecord SlurmDB [#^DBConnection db-connection]
  ISlurm
  (create [_ object]
	  (insert-db-record (:spec db-connection) (name (:table-name object)) (:columns object)))
  (fetch  [_ clause]
	  (select-db-record (:spec db-connection)
			    (name (:table-name clause))
			    (.get-table-primary-key db-connection (name (:table-name clause)))
			    (name (:column-name clause))
			    (.get-field-type db-connection (name (:table-name clause)) (name (:column-name clause)))
			    (name (or (:operator clause) :=))
			    (:value clause)))
  (update [_ object]
	  (update-db-record (:spec db-connection)
			    (name (:table-name object))
			    (.get-table-primary-key db-connection (name (:table-name object)))
			    (.get-table-primary-key-type db-connection (name (:table-name object)))
			    (:primary-key object)
			    (:columns object)))
  (delete [_ object]
	  (delete-db-record (:spec db-connection)
			    (name (:table-name object))
			    (.get-table-primary-key db-connection (name (:table-name object)))
			    (.get-table-primary-key-type db-connection (name (:table-name object)))
			    (get (:columns object) (keyword (.get-table-primary-key db-connection (name (:table-name object))))))))

;; DB Interface (direct access)
(defprotocol IDBAccess
  "Interface for directly querying the DB, perhaps useful for optimization (note, will not coerce results into Slurm objects).  Using SlurmDB is preferred."
  (query   [#^DBConnection DB query])
  (command [#^DBConnection DB command]))

(defrecord DB [#^DBConnection db-connection]
  IDBAccess
  (query [_ query]
	 (let [db-connection-spec (:spec db-connection)]
	   (sql/with-connection db-connection-spec
	     (sql/with-query-results query-results
	       [query]
	       (doall
		(for [result query-results]
		  result))))))
  (command [_ command]
	   (let [db-connection-spec (:spec db-connection)]
	     (sql/with-connection db-connection-spec
	       (sql/do-commands command)))))
  
;; Initialization and Verification
;; TODO: ugly, fix
(defn init
  "Configures DB connection, and initializes DB schema (creates db and tables if needed)."
  [schema-def
   & [fetch-graph]]
  (with-handler
    (let [db-schema (try (read-string (str schema-def)) (catch Exception e (println "Could not read schema definition.\nTrace:" e)))
	  db-host (get db-schema :db-server-pool "localhost")
	  ;; TODO: support server pools at some point, for now just grab a single hostname
	  db-host (if (string? db-host) db-host (first db-host)) ;; allow vector (multiple) or string (single) server defs
	  db-port (or (:db-port db-schema) 3306)
	  db-root-subname (str "//" db-host ":" db-port "/")
	  db-name (or (:db-name db-schema) "slurm_db")
	  db-user (or (:user db-schema) "root") ;; TODO: this is probably a bad idea for a default
	  db-password (:password db-schema)
	  db-connection-spec {:classname "com.mysql.jdbc.Driver"
			      :subprotocol "mysql"
			      :subname (str db-root-subname db-name)
			      :user db-user
			      :password db-password}
	  db (DBConnection. db-connection-spec db-schema nil)]
	;; check db schema for bad keys, and verify the db connection (create db if it doesn't exist)
	(if (not (exists-db? db-connection-spec db-root-subname db-name))
	  (do
	    (raise SchemaWarningDBNoExist db-name "DB not found on host, will attempt to create it")
	    (create-db db-connection-spec db-root-subname db-name)))
	(if (not-empty (seq (filter (complement valid-schema-db-keys) (keys db-schema))))
	  (raise SchemaWarningUnknownKeys
		 (filter (complement valid-schema-db-keys) (keys db-schema))
		 (str "Verify schema root keys")))
	;; Consume the table definitions
	(doseq [table (:tables db-schema)]
	  (if (not-empty (seq (filter (complement valid-schema-table-keys) (keys table))))
	    (raise SchemaWarningUnknownKeys
		   (filter (complement valid-schema-table-keys) (keys table))
		   (str "Verify table definition for '" (name (:name table "<TABLE-NAME-KEY-MISSING>")) "'")))
	  (if (nil? (.get-table db (:name table)))
	    (raise SchemaErrorBadTableName table))
	  (let [table-name (:name table)
		table-columns (cons [(.get-table-primary-key db table-name)
				     (.get-table-primary-key-type db table-name)
				     "PRIMARY KEY"
				     (if (.get-table-primary-key-auto db table-name)
				         "AUTO_INCREMENT")]
				    (or (.get-table-fields db table-name) []))
		;; Single relations are keyed into the table schema directly
		relations (for [relation (-> table :relations :one-to-one)]
			    (let [related-table-name (first (filter #(= (str-tools/as-str %) (str-tools/as-str relation)) (.get-table-names db)))
				  related-table-key (generate-relation-key-name related-table-name (.get-table-primary-key db related-table-name))
				  foreign-key-constraint (generate-foreign-key-constraint related-table-key related-table-name (.get-table-primary-key db related-table-name))]
			      [[related-table-key (.get-table-primary-key-type db related-table-name)]
			       foreign-key-constraint])) ;; returns a seq of relation key/type and constraint
		;; TODO: figure out how to set engine when creating tables (looks like this will need to be a patch on contrib)
		table-columns (concat table-columns (apply concat relations))]
	    ;; TODO: figure out what to do on db-schema updates (currently left to the user
	    ;;       to update table and slurm-schema on any change). Inconsistencies between
	    ;;       db-schema and slurm-schema should probably trigger a warning, verify
	    ;;       schema against table definition if it already exists.
	    (do
	      (if (not (exists-table? db-connection-spec db-root-subname db-name table-name))
		(do
		  (raise SchemaWarningTableNoExist (name table-name) "Table not found on host/db, will attempt to create it")
		  (apply create-table db-connection-spec table-name table-columns))) ;; if table doesn't exist, create it
	      (if (not-empty (seq (filter (complement valid-schema-relation-keys) (keys (-> table :relations)))))
		(raise SchemaWarningUnknownKeys
		       (filter (complement valid-schema-relation-keys) (keys (-> table :relations)))
		       (str "Verify :relations keys on table '" (name table-name) "'")))
	      ;; Multi relations are keyed to an intermediate relation table
	      (doseq [relation (-> table :relations :one-to-many)] ;; create multi-relation intermediate tables
		(let [related-table-name (str-tools/as-str relation)
		      related-table (or (.get-table db (str-tools/as-str (first (filter #(= (str-tools/as-str %) (str-tools/as-str related-table-name))
											(.get-table-many-relations db table-name)))))
					(raise SchemaErrorBadTableName relation
					       (str "Relation table name not found or badly formed, on relation '"
						    (str-tools/as-str relation) "' in table definition '"
						    (str-tools/as-str table-name) "'")))
		      origin-table-key (generate-relation-key-name table-name (.get-table-primary-key db table-name))
		      related-table-key (generate-relation-key-name related-table-name (.get-table-primary-key db related-table-name))
		      related-table-key-type (.get-table-primary-key-type db related-table-name)
		      relation-table-name (generate-relation-table-name table-name related-table-name)
		      relation-table-columns [[:id "int(11)" "PRIMARY KEY" "AUTO_INCREMENT"]] ;; relation tables cannot have configurable primary keys
		      relation-table-columns (conj relation-table-columns [origin-table-key (.get-table-primary-key-type db table-name)])
		      relation-table-columns (conj relation-table-columns (generate-foreign-key-constraint-cascade-delete origin-table-key
															  table-name
															  (.get-table-primary-key db table-name)))
		      relation-table-columns (conj relation-table-columns [related-table-key related-table-key-type])
		      relation-table-columns (conj relation-table-columns (generate-foreign-key-constraint-cascade-delete related-table-key
															  related-table-name
															  (.get-table-primary-key db (:name related-table))))]
		  (if (not (exists-table? db-connection-spec db-root-subname db-name relation-table-name))
		    (do
		      (raise SchemaWarningTableNoExist
			     (str-tools/as-str related-table-name)
			     (str "Relation intermediary table (for "
				  (str-tools/as-str table-name) "->"
				  (str-tools/as-str related-table-name) ") not found on host/db, will attempt to create it"))
		      (apply create-table db-connection-spec relation-table-name relation-table-columns))))))))
      db)
    (handle SchemaWarning [] (continue-with nil)) ;; NOTE: Logging behaviour is handled in the SchemaError/Warning def
    (handle SchemaError [])))                     ;;       Empty handlers are to supress java exception

;; Command-line Interface (used to init schemas)
;; TODO: at some point add a REPL to allow playing with the DB through the CLI
(defn -main [& args]
  (let [schema-file (first args)]
    (do
      (if (nil? schema-file)
	(println "Slurm command-line utility used to verify and initialize schema definition.\nUsage: java -jar slurm.jar <schema-filename>")
	(let [db (init (try (slurp schema-file) (catch Exception e (println "Could not load schema file.\nTrace:" e))))]
	  (println "Database schema successfully initialized"))))))