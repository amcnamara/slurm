(ns slurm.clause
  (:use [slurm.internal :only (sym)]))

;; Representation of a fetch clause on a db table
(defrecord DBClause [table-name column-name operator value])

(defmacro gen-partial-clause
  "Interns a partial function for the fetching of a/many db records as bound
   in core/with-orm.  Returns a hash-map with a bindable symbol keyed to the
   ns-qualified function name."
  [slurmdb table-name]
  `(let [fname# (sym ~table-name)]
     (hash-map fname#
	       (intern 'slurm.clause fname# (fn ([pk#]       (first (.fetch ~slurmdb (DBClause. ~table-name (.get-table-primary-key (:dbconnection ~slurmdb) ~table-name) :=  pk#))))
					        ([col# val#] (.fetch ~slurmdb (DBClause. ~table-name col# := val#))))))))