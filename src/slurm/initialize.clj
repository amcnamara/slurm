(ns slurm.initialize
  (:require [slurm.error      :as err]
	    [slurm.connection :as connection]
	    [slurm.orm        :as orm])
  (:import  [slurm.connection DBConnection]
	    [slurm.orm        SlurmDB])
  (:use     [clojure.contrib.error-kit :only (with-handler handle continue-with raise)]
	    [clojure.contrib.string    :only (as-str)]
	    [slurm.internal]))

;; Initialization and Verification
;; TODO: Figure out how to set engine when creating tables (patch on contrib?)
;; TODO: Figure out what to do on db-schema updates (currently left to the dev
;;       to update table and slurm-schema on any change). Inconsistencies between
;;       db-schema and slurm-schema should probably trigger a warning, verify
;;       schema against table definition if it already exists.
;; TODO: Ugly, fix
(defn init
  "Configures DB connection, and initializes DB schema (creates db and tables if needed)."
  [schema-def & [fetch-graph]]
  (with-handler
    (let [db-schema          (try (read-string (str schema-def)) (catch Exception e (raise err/SchemaError e "Could not read schema definiton.")))
	  db-host            (get db-schema :db-host "localhost")
	  db-host            (if (string? db-host) db-host (first db-host)) ;; allow vector (multiple) or string (single) server defs
	  db-port            (or (:db-port db-schema) 3306)
	  db-root-subname    (str "//" db-host ":" db-port "/")
	  db-name            (or (:db-name db-schema) "slurm_db")
	  db-user            (or (:user db-schema) "root") ;; TODO: this is probably a bad idea for a default
	  db-password        (:password db-schema)
	  db-connection-spec {:classname "com.mysql.jdbc.Driver"
			      :subprotocol "mysql"
			      :subname (str db-root-subname db-name)
			      :user db-user
			      :password db-password}
	  db (DBConnection. db-connection-spec db-schema nil)]
	;; check db schema for bad keys, and verify the db connection (create db if it doesn't exist)
	(if (not (exists-db? db-connection-spec db-root-subname db-name))
	  (do
	    (raise err/SchemaWarningDBNoExist db-name "DB not found on host, will attempt to create it")
	    (create-db db-connection-spec db-root-subname db-name)))
	(if (not-empty (seq (filter (complement valid-schema-db-keys) (keys db-schema))))
	  (raise err/SchemaWarningUnknownKeys
		 (filter (complement valid-schema-db-keys) (keys db-schema))
		 (str "Verify schema root keys")))
	;; Consume the table definitions
	(doseq [table (:tables db-schema)]
	  (if (not-empty (seq (filter (complement valid-schema-table-keys) (keys table))))
	    (raise err/SchemaWarningUnknownKeys
		   (filter (complement valid-schema-table-keys) (keys table))
		   (str "Verify table definition for '" (name (:name table "<TABLE-NAME-KEY-MISSING>")) "'")))
	  (if (nil? (.get-table db (:name table)))
	    (raise err/SchemaErrorBadTableName table))
	  (let [table-name    (:name table)
		table-columns (cons [(.get-table-primary-key db table-name)
				     (.get-table-primary-key-type db table-name)
				     "PRIMARY KEY"
				     (if (.get-table-primary-key-auto db table-name)
				       "AUTO_INCREMENT")]
				    ;; Define all non-relation columns (if any)
				    (or (map #(vector % (.get-table-field-type db table-name %))
					     (clojure.set/difference (set (.get-table-fields    db table-name))
								     (.get-table-many-relations db table-name)
								     (.get-table-one-relations  db table-name)))
					[]))
		relations     (for [relation (.get-table-one-relations db table-name)] ;; Single relations are keyed into the table schema directly
				(let [related-table-name (.get-table-field-type db table-name relation)
				      foreign-key-constraint (generate-foreign-key-constraint relation related-table-name (.get-table-primary-key db related-table-name))]
				  [[relation (.get-table-primary-key-type db related-table-name)]
				   foreign-key-constraint])) ;; returns a seq of relation key/type and constraint
		table-columns (concat table-columns (apply concat relations))]
	    (do
	      ;; If table doesn't exist, create it
	      (if (not (exists-table? db-connection-spec db-root-subname db-name table-name))
		(do
		  (raise err/SchemaWarningTableNoExist (name table-name) "Table not found on host/db, will attempt to create it")
		  (apply create-table db-connection-spec table-name table-columns)))
	      ;; Multi relations are keyed to an intermediary relation table
	      (doseq [relation (.get-table-many-relations db table-name)]
		(let [related-table-name     (.get-table-field-type db table-name relation)
		      related-table          (or (.get-table db related-table-name)
						 (raise err/SchemaErrorBadTableName relation
							(as-str "Relation table name not found or badly formed, on relation '"
								relation "' in table definition '" table-name "'")))
		      origin-table-key       (generate-relation-key-name table-name (.get-table-primary-key db table-name))
		      related-table-key      (generate-relation-key-name related-table-name (.get-table-primary-key db related-table-name))
		      related-table-key-type (.get-table-primary-key-type db related-table-name)
		      relation-table-name    (generate-relation-table-name table-name related-table-name)
		      relation-table-columns [[:id "int(11)" "PRIMARY KEY" "AUTO_INCREMENT"]] ;; Relation tables cannot have configurable primary keys
		      relation-table-columns (conj relation-table-columns [origin-table-key (.get-table-primary-key-type db table-name)])
		      relation-table-columns (conj relation-table-columns (generate-foreign-key-constraint-cascade-delete origin-table-key
															  table-name
															  (.get-table-primary-key db table-name)))
		      relation-table-columns (conj relation-table-columns [related-table-key related-table-key-type])
		      relation-table-columns (conj relation-table-columns (generate-foreign-key-constraint-cascade-delete related-table-key
															  related-table-name
															  (.get-table-primary-key db related-table-name)))]
		  (if (not (exists-table? db-connection-spec db-root-subname db-name relation-table-name))
		    (do
		      (raise err/SchemaWarningTableNoExist
			     (as-str related-table-name)
			     (join-as-str " " "Relation intermediary table"
					      (as-str "(" table-name "->" related-table-name ")")
					      "not found on host/db, will attempt to create it"))
		      (apply create-table db-connection-spec relation-table-name relation-table-columns))))))))
	;; Return a record which holds the db interface
	(SlurmDB. db))
    (handle err/SchemaWarning [] (continue-with nil)) ;; NOTE: Logging behaviour is handled in the SchemaError/Warning def
    (handle err/SchemaError   [])))                   ;;       Empty handlers are meant to supress java exception
