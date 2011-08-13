(ns slurm.construct
  (:use [slurm.internal :only (sym-bang)]))

;; Representation of db row data for creation of new record
(defrecord DBConstruct [table-name columns])

;; TODO: Gensym apparently doesn't produce namespace-unique names(!?),
;;       so susequent calls to this macro overwrite previous partial
;;       defs... this prevents memory leaking with many setup calls but
;;       hinders ability to have mutually exclusive connections.  Need
;;       to spend some hammock-time on this.
(defmacro gen-partial-construct
  "Interns a partial function for the creation of a new db record as bound
   in core/with-orm.  Returns a hash-map with a bindable symbol keyed to the
   ns-qualified function name."
  [slurmdb table-name]
  `(let [fname# (sym-bang ~table-name)]
     (hash-map fname#
	       (intern 'slurm.construct fname# #(.create ~slurmdb (DBConstruct. ~table-name %))))))