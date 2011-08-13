(ns example.db_example
  (:use [slurm.core :only (with-orm)])
  (:gen-class))

(def db-schema (try (slurp "src/example/db_schema") (catch Exception e (println "Failed to open schema file\n" e))))

(defn -main []
  (with-orm db-schema
    (let [test-course1 (course!  {:name "Software Design I"})
	  test-address (address! {:number 2354 :street "Rue St-Jacques" :city "Montreal" :province "QC" :country "Canada"})
	  test-student (student! {:name "Alex McNamara" :courses [test-course1] :address test-address})]
      (println "Test student DBO: " test-student)
      (println "Query course where id = 1, returns DBO: " (course 1)))))