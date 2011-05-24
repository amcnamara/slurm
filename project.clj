(defproject slurm "0.1.0-ALPHA"
  :description "A SQL ORM which uses a weighted object accessor graph to more efficiently load objects from the database"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]
		     [swank-clojure "1.2.0-SNAPSHOT"]]
  :disable-implicit-clean true
  :main slurm.core)