(ns slurm.error
  (:use [clojure.contrib.error-kit]))

(def warn-log  "warning.log")
(def error-log "error.log")

(defn write-log [log record]
  (spit log (str record "\n") :append true))

(defn get-timestamp [] (.getTime (java.util.Date.)))

;; Error definitions (break flow)
(deferror SchemaError
  []
  "General schema fault container"
  [error info]
  (let [timestamp (get-timestamp)]
    (println (str "Schema error (logged: " error-log " -- TS:" timestamp ")"))
    (write-log error-log (str "=== Schema Error TS:" timestamp " ===\nOBJECT     >>> " (if (seq? error) (apply str (interpose ", " error)) error))))
    (if info
      (write-log error-log (str "TRACE-INFO >>> " info))))

(deferror SchemaErrorBadTableName
  [SchemaError]
  "Table name not provided or otherwise bad"
  [error info]
  (write-log error-log "ERROR: SchemaErrorBadTableName\n       No name key or bad table name provided."))

(deferror SchemaErrorBadDBName
  [SchemaError]
  "DB name is bad (doesn't exist)"
  [error info]
  (write-log error-log "ERROR: SchemaErrorBadDBName\n       Bad DB name provided."))

;; Warning definitions (don't break flow)
(deferror SchemaWarning
  []
  "General schema warning container"
  [warning info]
  (let [timestamp (get-timestamp)]
    (println (str "Schema warning (logged: " warn-log " -- TS:" timestamp ")"))
    (write-log warn-log (str "--- Schema Warning TS:" timestamp " ---\nOBJECT     >>> " (if (seq? warning) (apply str (interpose ", " warning)) warning)))
    (if info
      (write-log warn-log (str "TRACE-INFO >>> " info)))))
  
(deferror SchemaWarningUnknownKeys
  [SchemaWarning]
  "A warning (continues) if the definition contains unrecognized keys"
  [warning info]
  (write-log warn-log "WARNING: SchemaWarningUnknownKeys\n         There were unrecognized keys found in your schema definition, they will be ignored."))

(deferror SchemaWarningDBNoExist
  [SchemaWarning]
  "A warning (continues) if DB name provided doesn't exist, try to create the database or ignore if possible"
  [warning info]
  (write-log warn-log "WARNING: SchemaWarningDBNoExist\n         DB name provided does not exist"))

(deferror SchemaWarningTableNoExist
  [SchemaWarning]
  "A warning (continues) if table name provided doesn't exist, try to create the table or ignore if possible"
  [warning info]
  (write-log warn-log "WARNING: SchemaWarningTableNoExist\n         Table name provided does not exist"))