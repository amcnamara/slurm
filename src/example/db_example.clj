(ns example.db_example
  (:require [slurm.initialize :as slurm]
	    [slurm.orm        :as orm])
  (:import  [slurm.orm        DBObject DBConstruct DBClause]))

(def db-schema (try (slurp "src/example/db_schema") (catch Exception e (println "Failed to open schema file\n" e))))

(defn -main []
  ;; Initialize the tables and create some dummy data
  (let [slurmdb    (slurm/init db-schema)
	course1    (.create slurmdb (DBConstruct. :course {:name "Software Design I"}))
	course2    (.create slurmdb (DBConstruct. :course {:name "Intro to Programming"}))
	cs-degree  (.create slurmdb (DBConstruct. :degree {:name "Computer Science, B.Sc." :courses [course1, course2]}))]
    ;; NOTE: Try changing the :loading value in db-schema from :eager to :lazy and see the difference it has on fetches.
    (println (.fetch  slurmdb   (DBClause. :degree :id := 1)))  ;; Clauses will fetch seqs of valid dbobject records
    (println (.field  cs-degree :courses))                      ;; Fetching a field will load the foreign dbobject(s) if needed (ie. if lazy)
    (println (.assoc* cs-degree {:name "Bachelor of CS"}))      ;; Returns a new DBObject and triggers update on DB
    (.delete cs-degree)                                         ;; Remove the objects from the DB
    (.delete course1)
    (.delete course2)))