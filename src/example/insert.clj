(ns example.insert
  (:require [slurm.core :as slurm])
  (:import  [slurm.core DBConstruct DBObject DBClause]))

(def db-schema (try (slurp "src/example/db-schema") (catch Exception e (println "Failed to open schema file\n" e))))

(defn -main []
  (let [orm        (slurm/init db-schema)
	my-address (slurm/.create orm (DBConstruct. :address
						   {:country "Canada"
						    :postal_code "A1B2C3"
						    :province "Quebec"
						    :city "Montreal"
						    :street "St-Jacques"
						    :number 1234
						    :not-a-column "aoeu"}))
	my-user    (slurm/.create orm (DBConstruct. :student
						   {:name "Alex McNamara"
						    :address my-address}))]
    (println my-user)                           ;; Notice that the address field is lazy loaded
    (println (slurm/.field my-user :address)))) ;; Fetching the field will load the foreign dbobject