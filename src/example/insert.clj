(ns example.insert
  (:require [slurm.core :as slurm])
  (:import  [slurm.core DBConstruct DBObject DBClause]))

(def db-schema (try (slurp "src/example/db-schema") (catch Exception e (println "Failed to open schema file\n" e))))

(defn -main []
  (let [orm (slurm/init db-schema)]
    (println (slurm/.field (slurm/.create orm (DBConstruct. :student
							    {:name "Alex McNamara"
							     :address (slurm/.create orm (DBConstruct. :address
												       {:country "Canada"
													:postal_code "A1B2C3"
													:province "Quebec"
													:city "Montreal"
													:street "St-Jacques"
													:number 1234
													:not-a-column "noval"}))}))
			   :address))))