;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.daemon.logviewer
  (:use compojure.core)
  (:use [clojure.set :only [difference]])
  (:use [clojure.string :only [blank?]])
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log timer])
  (:use [backtype.storm.ui helpers])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:import [org.slf4j LoggerFactory])
  (:import [ch.qos.logback.classic Logger])
  (:import [org.apache.commons.logging LogFactory])
  (:import [org.apache.commons.logging.impl Log4JLogger])
  (:import [ch.qos.logback.core FileAppender])
  (:import [org.apache.log4j Level])
  (:import [org.yaml.snakeyaml Yaml]
           [org.yaml.snakeyaml.constructor SafeConstructor])
  (:import [backtype.storm.security.auth AuthUtils])
  (:require [compojure.route :as route]
            [compojure.handler :as handler])
  (:require [backtype.storm.daemon common [supervisor :as supervisor]])
  (:import [java.io File FileFilter])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.util.response :as resp]
            [clojure.string :as string])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (read-storm-config))

(defn cleanup-cutoff-age-millis [conf now-millis]
  (- now-millis (* (conf LOGVIEWER-CLEANUP-AGE-MINS) 60 1000)))

(defn mk-FileFilter-for-log-cleanup [conf now-millis]
  (let [cutoff-age-millis (cleanup-cutoff-age-millis conf now-millis)]
    (reify FileFilter (^boolean accept [this ^File file]
                        (boolean (and
                          (.isFile file)
                          (re-find worker-log-filename-pattern (.getName file))
                          (<= (.lastModified file) cutoff-age-millis)))))))

(defn select-files-for-cleanup [conf now-millis]
  (let [file-filter (mk-FileFilter-for-log-cleanup conf now-millis)]
    (.listFiles (File. LOG-DIR) file-filter)))

(defn get-metadata-file-for-log-root-name [root-name]
  (let [metaFile (clojure.java.io/file LOG-DIR "metadata"
                                       (str root-name ".yaml"))]
    (if (.exists metaFile)
      metaFile
      (do
        (log-warn "Could not find " (.getCanonicalPath metaFile)
                  " to clean up for " root-name)
        nil))))

(defn get-worker-id-from-metadata-file [metaFile]
  (get (clojure-from-yaml-file metaFile) "worker-id"))

(defn get-topo-owner-from-metadata-file [metaFile]
  (get (clojure-from-yaml-file metaFile) TOPOLOGY-SUBMITTER-USER))

(defn get-log-root->files-map [log-files]
  "Returns a map of \"root name\" to a the set of files in log-files having the
  root name.  The \"root name\" of a log file is the part of the name preceding
  the extension."
  (reduce #(assoc %1                                      ;; The accumulated map so far
                  (first %2)                              ;; key: The root name of the log file
                  (conj (%1 (first %2) #{}) (second %2))) ;; val: The set of log files with the root name
          {}                                              ;; initial (empty) map
          (map #(list
                  (second (re-find worker-log-filename-pattern (.getName %))) ;; The root name of the log file
                  %)                                                          ;; The log file
               log-files)))

(defn identify-worker-log-files [log-files]
  (into {} (for [log-root-entry (get-log-root->files-map log-files)
                 :let [metaFile (get-metadata-file-for-log-root-name
                                  (key log-root-entry))
                       log-root (key log-root-entry)
                       files (val log-root-entry)]
                 :when metaFile]
             {(get-worker-id-from-metadata-file metaFile)
              {:owner (get-topo-owner-from-metadata-file metaFile)
               :files
                 ;; If each log for this root name is to be deleted, then
                 ;; include the metadata file also.
                 (if (empty? (difference
                                  (set (filter #(re-find (re-pattern log-root) %)
                                               (read-dir-contents LOG-DIR)))
                                  (set (map #(.getName %) files))))
                  (conj files metaFile)
                  ;; Otherwise, keep the list of files as it is.
                  files)}})))

(defn get-dead-worker-files-and-owners [conf now-secs log-files]
  (if (empty? log-files)
    {}
    (let [id->heartbeat (supervisor/read-worker-heartbeats conf)
          alive-ids (keys (remove
                            #(or (not (val %))
                                 (supervisor/is-worker-hb-timed-out? now-secs (val %) conf))
                            id->heartbeat))
          id->entries (identify-worker-log-files log-files)]
      (for [[id {:keys [owner files]}] id->entries
            :when (not (contains? (set alive-ids) id))]
        {:owner owner
         :files files}))))

(defn cleanup-fn! []
  (let [now-secs (current-time-secs)
        old-log-files (select-files-for-cleanup *STORM-CONF* (* now-secs 1000))
        dead-worker-files (get-dead-worker-files-and-owners *STORM-CONF* now-secs old-log-files)]
    (log-debug "log cleanup: now(" now-secs
               ") old log files (" (seq (map #(.getName %) old-log-files))
               ") dead worker files (" (seq (map #(.getName %) dead-worker-files)) ")")
    (dofor [{:keys [owner files]} dead-worker-files
            file files]
      (let [path (.getCanonicalPath file)]
        (log-message "Cleaning up: Removing " path)
        (try
          (if (or (blank? owner) (re-matches #".*\.yaml$" path))
            (rmr path)
            ;; worker-launcher does not actually launch a worker process.  It
            ;; merely executes one of a prescribed set of commands.  In this case, we ask it
            ;; to delete a file as the owner of that file.
            (supervisor/worker-launcher *STORM-CONF* owner (str "rmr " path)))
          (catch Exception ex
            (log-error ex)))))))

(defn start-log-cleaner! [conf]
  (let [interval-secs (conf LOGVIEWER-CLEANUP-INTERVAL-SECS)]
    (when interval-secs
      (log-debug "starting log cleanup thread at interval: " interval-secs)
      (schedule-recurring (mk-timer :thread-name "logviewer-cleanup")
                          0 ;; Start immediately.
                          interval-secs
                          cleanup-fn!))))

(defn tail-file [path tail]
  (let [flen (.length (clojure.java.io/file path))
        skip (- flen tail)]
    (with-open [input (clojure.java.io/input-stream path)
                output (java.io.ByteArrayOutputStream.)]
      (if (> skip 0) (.skip input skip))
      (let [buffer (make-array Byte/TYPE 1024)]
        (loop []
          (let [size (.read input buffer)]
            (when (and (pos? size) (< (.size output) tail))
              (do (.write output buffer 0 size)
                  (recur))))))
      (.toString output))
    ))

(defn get-log-user-whitelist [fname]
  (let [wl-file (get-log-metadata-file fname)
        m (clojure-from-yaml-file wl-file)]
    (if-let [whitelist (.get m LOGS-USERS)] whitelist [])))

(defn authorized-log-user? [user fname conf]
  (if (or (blank? user) (blank? fname))
    nil
    (let [whitelist (get-log-user-whitelist fname)
          logs-users (concat (conf LOGS-USERS)
                             (conf NIMBUS-ADMINS)
                             whitelist)]
       (some #(= % user) logs-users))))

(defn log-root-dir
  "Given an appender name, as configured, get the parent directory of the appender's log file.

Note that if anything goes wrong, this will throw an Error and exit."
  [appender-name]
  (let [appender (.getAppender (LoggerFactory/getLogger Logger/ROOT_LOGGER_NAME) appender-name)]
    (if (and appender-name appender (instance? FileAppender appender))
      (.getParent (File. (.getFile appender)))
      (throw
       (RuntimeException. "Log viewer could not find configured appender, or the appender is not a FileAppender. Please check that the appender name configured in storm and logback agree.")))))

(defn log-page [fname tail grep user root-dir]
  (let [file (-> (File. root-dir fname) .getCanonicalFile)
        path (.getCanonicalPath file)]
    (if (= (File. root-dir)
           (.getParentFile file))
      (let [tail (if tail
                   (min 10485760 (Integer/parseInt tail))
                   10240)
            tail-string (tail-file path tail)]
        (if (or (blank? (*STORM-CONF* UI-FILTER))
                (authorized-log-user? user fname *STORM-CONF*))
          (if grep
             (clojure.string/join "\n<br>"
               (filter #(.contains % grep) (.split tail-string "\n")))
             (.replaceAll tail-string "\n" "\n<br>"))

          (unauthorized-user-html user)))

      (-> (resp/response "Page not found")
          (resp/status 404)))))

(defn log-level-page [name level]
  (let [log (LogFactory/getLog name)]
    (if level
      (if (instance? Log4JLogger log)
        (.setLevel (.getLogger log) (Level/toLevel level))))
    (str "effective log level for " name " is " (.getLevel (.getLogger log)))))

(defn log-template
  ([body] (log-template body nil))
  ([body user]
    (html4
     [:head
      [:title "Storm log viewer"]
      (include-css "/css/bootstrap-1.4.0.css")
      (include-css "/css/style.css")
      (include-js "/js/jquery-1.6.2.min.js")
      (include-js "/js/jquery.tablesorter.min.js")
      (include-js "/js/jquery.cookies.2.2.0.min.js")
      (include-js "/js/script.js")
      ]
     [:body
      (concat
        (when (not (blank? user)) [[:div.ui-user [:p "User: " user]]])
        (seq body))
      ])))

(def http-creds-handler (AuthUtils/GetUiHttpCredentialsPlugin *STORM-CONF*))

(defroutes log-routes
  (GET "/log" [:as {servlet-request :servlet-request, log-root :log-root} & m]
       (let [user (.getUserName http-creds-handler servlet-request)]
         (log-template (log-page (:file m) (:tail m) (:grep m) user log-root) user)))
  (GET "/loglevel" [:as {servlet-request :servlet-request} & m]
       (let [user (.getUserName http-creds-handler servlet-request)]
         (log-template (log-level-page (:name m) (:level m)) user) user))
  (route/resources "/")
  (route/not-found "Page not found"))

(def logapp
  (handler/site log-routes)
)

(defn conf-middleware
  "For passing the storm configuration with each request."
  [app log-root]
  (fn [req]
    (app (assoc req :log-root log-root))))


(defn start-logviewer! [conf]
  (let [port (int (conf LOGVIEWER-PORT))
        log-root (log-root-dir (conf LOGVIEWER-APPENDER-NAME))
        header-buffer-size (int (.get conf UI-HEADER-BUFFER-BYTES))
        filter-class (conf UI-FILTER)
        filter-params (conf UI-FILTER-PARAMS)]
    (try
      (run-jetty (conf-middleware logapp log-root) 
                        {:port (int (conf LOGVIEWER-PORT))
                         :join? false
                         :configurator (fn [server]
                                         (doseq [connector (.getConnectors server)]
                                           (.setHeaderBufferSize connector header-buffer-size))
                                         (config-filter server logapp
                                                        filter-class
                                                        filter-params))})
    (catch Exception ex
      (log-error ex)))))

(defn -main []
  (let [conf (read-storm-config)]
    (start-log-cleaner! conf)
    (start-logviewer! (read-storm-config))))
