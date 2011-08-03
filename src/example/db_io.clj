(ns example.db_io
  (:require [slurm.core :as slurm])
  (:import  [slurm.core DBConstruct DBObject DBClause]))

(def db-schema (try (slurp "src/example/db-schema") (catch Exception e (println "Failed to open schema file\n" e))))

(defn -main []
  ;; Initialize the tables and create some dummy data
  (let [slurmdb    (slurm/init db-schema)
	course1    (slurm/.create slurmdb (DBConstruct. :course
						       {:name "Software Design I"}))
	course2    (slurm/.create slurmdb (DBConstruct. :course
						       {:name "Intro to Programming"}))
	cs-degree  (slurm/.create slurmdb (DBConstruct. :degree
						       {:name "Computer Science, B.Sc."
						        :courses [course1, course2]}))]
    (println (slurm/.fetch slurmdb (DBClause. :degree :id := 1)))  ;; Clauses will fetch seqs of valid dbobject records
                                                                   ;; NOTE: Try changing the :loading value in db-schema from :eager to :lazy
                                                                   ;;       and see the difference it has on fetches.
    (println (slurm/.field  cs-degree :courses))                   ;; Fetching a field will load the foreign dbobject(s) if needed (ie. if lazy)
    (println (slurm/.assoc* cs-degree {:name "Bachelor of CS"}))   ;; Returns a new DBObject and triggers update on DB
    (slurm/.delete cs-degree)                                      ;; Remove the objects from the DB
    (slurm/.delete course1)
    (slurm/.delete course2)))