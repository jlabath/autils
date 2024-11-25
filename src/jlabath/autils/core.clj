(ns jlabath.autils.core
  (:import [java.util.concurrent Executors])
  (:require [clojure.core.async :as a]))

(def ^:private executor (Executors/newVirtualThreadPerTaskExecutor))

(defn go-call
  "calls the given function in separate virtual thread and returns a future
  exceptions will not fail and will be returned as values
  "
  [f]
  (let [wrapped-f (fn []
                    (try
                      (f)
                      (catch java.lang.Exception ex ex)))]
    (.submit executor wrapped-f)))

(defmacro go
  "similar to clojure.core.async/go - except it runs in virtual thread and therefore can block
  returns a future that can be defered if desired
  "
  [& body]
  `(let [f# (fn [] (do ~@body))]

     (go-call f#)))

(defn wait-all
  "waits for all the futures or promises to finish (meaning deref succeeds)
  returns vector of the deref results
  "
  [futures]
  (->> futures
       (map deref)
       (into [])))

(defn together
  "runs functions concurrently waits them to finish and returns the results"
  [& functions]
  (->> functions
       (map go-call)
       (into [])
       wait-all))

(defn winner
  "runs functions concurrently and returns a first result (or exception)"
  [& functions]
  (let [prom (promise)
        prom-wrap (fn [f] (fn [] (deliver prom (f))))
        wrap (comp go-call prom-wrap)]
    (->> functions
         (map wrap)
         (into []))
    prom))

(defn- pmap-xf
  [concurrency f]
  (comp
   (partition-all concurrency)
   (map (fn [batch]
          (map #(partial f %) batch)))
   (map #(apply together %))
   (mapcat identity)))

(defn pmap
  "runs function on each item of the collection at given concurrency
   if concurrency is not given it defaults to 10
  "
  ([concurrency f coll]
   (into [] (pmap-xf concurrency f) coll))

  ([f coll]
   (pmap 10 f coll)))

(defn coll->ch
  "converts collection into channel and closes it when exahusted"
  [coll]
  (let [ch (a/chan)]
    (go (loop [xs coll]
          (if-let [item (clojure.core/first xs)]
            (do
              (a/>!! ch item)
              (recur (next xs)))
            (a/close! ch))))
    ch))

(defn ch->vec
  "converts a channel to vector"
  [ch]
  (loop [v (vector)]
    (let [value (a/<!! ch)]
      (if (nil? value)
        v
        (recur (conj v value))))))

(defn- ch-map-worker
  [f in-ch out-ch]
  (loop []
    (when-let [item (a/<!! in-ch)]
      (->> (try
             (f item)
             (catch java.lang.Throwable ex ex))
           (a/>!! out-ch))
      (recur))))

(defn ch-map
  "processes items from input channel by calling f on items in the channel at given concurrency"
  ([concurrency f ch]
   (let [res-ch (a/chan concurrency)
         tasks (vec (for [_x (range concurrency)] (go-call (partial ch-map-worker f ch res-ch))))]
     (go
       (wait-all tasks)
       (a/close! res-ch))
     res-ch))
  ([f ch]
   (ch-map 10 f ch)))

(defn async-map
  "like pmap but uses channels and vthreads and returned results will (most likely) not be in the order in which theirinputs were received"
  ([concurrency f coll]
   (let [inp-ch (coll->ch coll)
         out-ch (ch-map concurrency f inp-ch)]
     (ch->vec out-ch)))

  ([f coll]
   (async-map 10 f coll)))
