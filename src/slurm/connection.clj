(ns slurm.connection
  (:use [clojure.contrib.string :only (as-str substring? lower-case)])
  (:gen-class))

;; DB Access Protocols
(defprotocol IDBInfo
  "General info on the DBConnection, common data requests on schema and objects"
  (get-db-loading             [#^DBConnection dbconnection])
  (get-table-names            [#^DBConnection dbconnection])
  (get-table                  [#^DBConnection dbconnection table-name])
  (get-table-primary-key      [#^DBConnection dbconnection table-name])
  (get-table-primary-key-type [#^DBConnection dbconnection table-name])
  (get-table-primary-key-auto [#^DBConnection dbconnection table-name])
  (get-table-schema           [#^DBConnection dbconnection table-name])
  (get-table-fields           [#^DBConnection dbconnection table-name])
  (get-table-field-type       [#^DBConnection dbconnection table-name field-name])
  (get-table-field-load       [#^DBConnection dbconnection table-name field-name])
  (get-table-relations        [#^DBConnection dbconnection table-name])
  (get-table-one-relations    [#^DBConnection dbconnection table-name])
  (get-table-many-relations   [#^DBConnection dbconnection table-name]))

;; Access Objects for common introspection data on schema and load-graph
;; TODO: Get the load graph sorted out
(defrecord DBConnection [spec schema load-graph]
  IDBInfo
  (get-db-loading             [this]
			      (get schema :loading :lazy))
  (get-table-names            [_]
			      (map #(as-str (:name %)) (:tables schema)))
  (get-table                  [_ table-name]
			      (first (filter #(= (keyword table-name) (keyword (:name %))) (:tables schema))))
  (get-table-primary-key      [this table-name]
			      (or (-> (.get-table this table-name) :primary-key) "id"))
  (get-table-primary-key-type [this table-name]
			      (or (-> (.get-table this table-name) :primary-key-type) "int(11)"))
  (get-table-primary-key-auto [this table-name]
			      (or (-> (.get-table this table-name) :primary-key-auto-increment)
				  (substring? "int" (lower-case (name (.get-table-primary-key-type this table-name))))))
  (get-table-schema           [this table-name]
			      (-> (.get-table this table-name) :fields))
  (get-table-fields           [this table-name]
			      (or (keys (.get-table-schema this table-name)) []))
  (get-table-field-type       [this table-name field-name]
			      (or (let [type (get (.get-table-schema this table-name) (keyword field-name))]
				    (if (and (keyword? type) (= \* (first (name type))))
				      (keyword (apply str (rest (name type))))
				      type))
				  "int(11)"))
  (get-table-field-load       [this table-name field-name]
			      nil)
  (get-table-relations        [this table-name]
			      (or (into (.get-table-one-relations this table-name) (.get-table-many-relations this table-name)) []))
  (get-table-one-relations    [this table-name]
			      (or (keys (filter #(not= \* (first (apply name (rest %))))
						(filter #(apply keyword? (rest %)) (.get-table-schema this table-name)))) []))
  (get-table-many-relations   [this table-name]
			      (or (keys (filter #(= \* (first (apply name (rest %))))
						(filter #(apply keyword? (rest %)) (.get-table-schema this table-name)))) [])))
