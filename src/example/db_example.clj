(ns example.db_example
  (:use [slurm.core :only (with-orm)]))

(def db-schema (try (slurp "src/example/db_schema") (catch Exception e (println "Failed to open schema file\n" e))))

(defn -main []
  (with-orm db-schema
    ;; Create some DBOs
    (let [test-course1 (course!  {:name "Software Design I"})
	  test-course2 (course!  {:name "Algorithms and Data Structures I"})
	  test-address (address! {:number 2354 :street "Rue St-Jacques" :city "Montreal" :province "QC" :country "Canada"})
	  test-student (student! {:name "Alex McNamara" :courses [test-course1 test-course2] :address test-address})]
      ;; Fetch the DBO for course where primary-key (:id was created as a default in this case, since
      ;; db-schema didn't specify a pk) has a value of 1.
      (course 1)
      ;; Fetch a seq of address DBOs which have a city column value of Montreal
      (address :city "Montreal")
      ;; Change the test-student record (returns a new DBO)
      ;; NOTE: We're ignoring the returned DBO here, but normally you'd want to keep test-student in an atom
      ;;       or some other state-monitor and safely swap in the new DBO.  Refer: http://clojure.org/state
      (assoc* test-student {:name "Bender Bending Rodriguez"})
      ;; Clean up DB
      (delete test-course1)
      (delete test-course2)
      (delete test-address)
      (delete test-student))))