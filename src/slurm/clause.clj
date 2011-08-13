(ns slurm.clause
  (:use [slurm.internal :only (sym)]))

;; Representation of a fetch clause on a db table
(defrecord DBClause [table-name column-name operator value])

;; (.fetch  slurmdb (DBClause. table (.get-table-primary-key dbconnection) := pk))