(ns background-processing.background-processor
  (:require
   [amazonica.aws.sqs :as sqs]
   [background-processing.background-job :as background-job]
   [better-cond.core :refer [cond] :rename {cond with}]
   [com.stuartsierra.component :as component]))

(defmulti perform-async
  (fn [background-processor job performer-fn]
    (background-job/type job)))

(defmethod perform-async ::background-job/cpu-bound [{:keys [cpu-executor]} job performer-fn]
  (send-via cpu-executor (agent job) performer-fn))

(defmethod perform-async ::background-job/io-bound [{:keys [io-executor]} job performer-fn]
  (send-via io-executor (agent job) performer-fn))

(defrecord BackgroundProcessor [queue-name err cpu-executor io-executor]
  component/Lifecycle
  (start [this]
    (let [queue (sqs/find-queue queue-name)
          runner (future
                   (while (-> (Thread/currentThread) .isInterrupted not)
                     (with
                      :when-let [message (-> (sqs/receive-message :queue-url queue :wait-time-seconds 1)
                                             :messages
                                             first)]
                      :let [job (-> message :body read-string)]
                      (perform-async this
                                     job
                                     (fn [job]
                                       (try
                                         (background-job/perform job)
                                         (-> message (assoc :queue-url queue) sqs/delete-message)
                                         (catch Throwable e
                                           (binding [*out* err]
                                             (println ::start.exception e :start.message job)))))))))]
      (assoc this :future runner)))

  (stop [this]
    (some-> this :future future-cancel)))

(defn new [& {:keys [queue-name err io-executor cpu-executor]
              :or {err *err*
                   cpu-executor clojure.lang.Agent/pooledExecutor
                   io-executor clojure.lang.Agent/soloExecutor}}]
  (map->BackgroundProcessor {:queue-name queue-name
                             :err err
                             :cpu-executor cpu-executor
                             :io-executor io-executor}))

