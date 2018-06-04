(ns background-processing.background-job
  (:refer-clojure :exclude [type]))

(def types #{::io-bound ::cpu-bound})

(defprotocol BackgroundJob
  (perform [_]
    "The actual task to be performed")
  (type [_]
    "One of: `#'types`, or your own (which should be accompanied
by a defmethod for `#'background-processing.background-processor/perform-async`)"))
