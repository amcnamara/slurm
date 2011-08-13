(ns slurm.core
  (:require [slurm.clause     :as clause]
	    [slurm.construct  :as construct])
  (:use     [slurm.initialize :only (init)]
	    [slurm.internal   :only (sym sym-bang bind-map)])
  (:gen-class))

(defn accessor-map
  "Creates a hash-map keyed with table names to partial functions of DB fetch and
   create operations.  The functions are interned so that they don't get GC'd."
  [slurmdb]
  (let [dbconnection (:dbconnection slurmdb)]
    (reduce into
	    (for [table (.get-table-names dbconnection)]
	      (into (construct/gen-partial-construct slurmdb table)
		    (clause/gen-partial-clause slurmdb table))))))

(defmacro with-orm
  "Captures <table-name>! and <table-name> symbols and binds them to partial
   slurm db constructors and getters (respectively) to simplify orm interaction
   in the body, as follows:

   - Create DBObject:
       (<table-name>! col-map)
   - Fetch DBObjects:
       (<table-name> pk-value)
       (<table-name> column-name column-value)
       (<table-name> & clauses)

   NB: These symbols are captured, and may cause unexpected behavour if you try
       to close similarly-named symbols around this macro.

   DBOs are the object representation of db records, and alongside the interface
   described above give full CRUD support across the DB, as follows:

   - Update DBObject:
       (assoc* <dbobject> col-map)
   - Delete DBObject:
       (delete <dbobject>)

   The update operation will modify the db record and return a new DBO representing
   the changed state of the DB, however care should be taken to avoid using DBOs
   which point to a previous state of the data.

   Refer: http://clojure.org/state for tips on dealing with state in clojure."
  [db-schema & body]
  `(let [slurmdb# (init ~db-schema)
	 sym-map# (accessor-map slurmdb#)
	 binding# (bind-map sym-map#)]
     (eval `(let ~binding# (doall ~@'~body)))))

;; TODO: at some point add a REPL to allow playing with the DB through the CLI
(defn -main
  "Command-line interface (used to initialize schemas only)"
  [& args]
  (let [schema-file (first args)]
    (do
      (if (nil? schema-file)
	(println "Slurm command-line utility used to verify and initialize schema definition.\nUsage: java -jar slurm.jar <schema-filename>")
	(let [orm (init (try (slurp schema-file) (catch Exception e (println "Could not load schema file.\nTrace:" e))))]
	  (do
	    (println "Database schema successfully initialized")))))))