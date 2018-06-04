(ns background-processing.enqueuer
  (:require
   [amazonica.aws.sqs :as sqs]
   [better-cond.core :refer [cond] :rename {cond with}]
   [clojure.core.async :as async :refer [>!!]]
   [com.stuartsierra.component :as component]))

(defrecord Enqueuer [queue-name channel err]
  component/Lifecycle
  (start [this]
    (let [queue (sqs/find-queue queue-name)
          runner (future
                   (while (-> (Thread/currentThread) .isInterrupted not)
                     (with
                      :let [[message port] (async/alts!! [channel (async/timeout 100)])]
                      :when (= port channel)
                      :let [serialized (pr-str message)]
                      (try
                        (sqs/send-message queue serialized)
                        (catch Throwable e
                          (binding [*out* err]
                            (println ::start.exception e ::start.serialized serialized))
                          (>!! channel message))))))]
      (assoc this :future runner)))

  (stop [this]
    (some-> this :future future-cancel)))

(defn new [& {:keys [queue-name channel err]
              :or {err *err*}}]
  (map->Enqueuer {:queue-name queue-name
                  :channel (or channel (async/chan))
                  :err err}))
