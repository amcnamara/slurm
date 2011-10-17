(defproject slurm "0.3.1"
  :description "A SQL ORM which uses a weighted object accessor graph to more efficiently load objects from the database"
  :dependencies     [[org.clojure/clojure         "1.2.0"]
                     [org.clojure/clojure-contrib "1.2.0"]
		     [com.mysql/connectorj        "5.1.12"]]
  :dev-dependencies [[lein-clojars                "0.6.0"]]
  :main example.db_example)
