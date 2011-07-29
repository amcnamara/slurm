(ns example.db_io
  (:require [slurm.core :as slurm])
  (:import  [slurm.core DBConstruct DBObject DBClause]))

(def db-schema (try (slurp "src/example/db-schema") (catch Exception e (println "Failed to open schema file\n" e))))

(defn -main []
  ;; Initialize the tables and create some dummy data
  (let [orm        (slurm/init db-schema)
	my-address (slurm/.create orm (DBConstruct. :address
						   {:country      "Canada"
						    :postal_code  "A1B2C3"
						    :province     "Quebec"
						    :city         "Montreal"
						    :street       "St-Jacques"
						    :number       1234
						    :not-a-column "aoeu"}))
	my-user    (slurm/.create orm (DBConstruct. :student
						   {:name    "Alex McNamara"
						    :address my-address}))]
    (println (slurm/.fetch orm (DBClause. :student :name := "Alex McNamara"))) ;; Clauses will fetch seqs of valid dbobject records
                                                                               ;; NOTE: Try changing the :loading value in db-schema from :eager to :lazy
                                                                               ;;       and see the difference it has on fetches.
    (println (slurm/.field my-user :address))                                  ;; Fetching the field will load the foreign dbobject if needed (ie. if lazy)
    (slurm/.delete orm my-address)                                             ;; Remove the records from the DB
    (slurm/.delete orm my-user)))