(ns slurm.construct
  (:use [slurm.internal :only (sym-bang)]))

;; Representation of db row data for creation of new record
(defrecord DBConstruct [table-name columns])

(defmacro gen-partial-construct
  "Interns a partial function for the creation of a new db record as bound
   in core/with-orm.  Returns a hash-map with a bindable symbol keyed to the
   ns-qualified function name."
  [slurmdb table-name]
  `(let [fname# (sym-bang ~table-name)]
     (hash-map fname#
	       (intern 'slurm.construct fname# #(.create ~slurmdb (DBConstruct. ~table-name %))))))