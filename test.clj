#!/usr/bin/env bb

(require
 '[clojure.core.async :refer [>!! <!!] :as a]
 '[clojure.test :refer [deftest is run-tests]]
 '[jlabath.autils.core :as au])

(defn slow-identity [val]
  (Thread/sleep (+ 200 (rand-int 1000)))
  val)

(defn custom-slow-identity [ms val]
  (Thread/sleep ms)
  val)

(deftest test-go-call
  (let [fut (au/go-call #(/ 4 2))
        fut2 (au/go-call #(/ 4 0))]
    (is (= 2 @fut))
    (is (instance? Throwable @fut2))))

(deftest test-go
  (let [ch (a/chan)]
    (au/go
      (>!! ch (slow-identity 93)))
    (is (= 93 (<!! ch)))))

(deftest test-many-go
  (let [ch (a/chan)
        tasks (vec (map (fn [_] (au/go (>!! ch (slow-identity 1)))) (range 10000)))]
    (au/go
      ;;wait for all tasks to finish
      (doseq [t tasks] (deref t))
      ;;close the channel so that the loop below can end
      (a/close! ch))

    (is (= 10000 (loop [acc 0]
                   (if-some [v (<!! ch)]
                     (recur (+ acc v))
                     acc))))))

(deftest test-all
  (let [ch (a/chan)
        tasks (vec (map (fn [_] (au/go (>!! ch (slow-identity 1)))) (range 102)))]
    (au/go
      ;;wait for all tasks to finish
      (au/wait-all tasks)
      ;;close the channel so that the loop below can end
      (a/close! ch))

    (is (= 102 (loop [acc 0]
                 (if-some [v (<!! ch)]
                   (recur (+ acc v))
                   acc))))))

(deftest test-together
  (let [group (au/together
               (partial slow-identity 1)
               (partial slow-identity 22)
               (partial slow-identity 333)
               (partial slow-identity 4444))]
    (is (= [1 22 333 4444] group))))

(deftest test-winner
  (let [res @(au/winner
              (partial custom-slow-identity 10 :neil)
              (partial custom-slow-identity 10 :bob)
              (partial custom-slow-identity 12 :joe)
              (partial custom-slow-identity 1 :flash)
              (partial custom-slow-identity 13 :phil))]
    (is (= :flash res))))

(deftest test-pmap
  (let [results (au/pmap 20 slow-identity (range 40))]
    (is (= (* (/ 39 2) (+ 1 39)) (apply + results)))))

(deftest test-coll->ch
  (let [ch (au/coll->ch (range 5))
        nxt (fn [] (<!! ch))]
    (is (= 0 (nxt)))
    (is (= 1 (nxt)))
    (is (= 2 (nxt)))
    (is (= 3 (nxt)))
    (is (= 4 (nxt)))
    (is (nil? (nxt)))))

(deftest test-ch->vec
  (let [ch (au/coll->ch (range 10000))
        result (au/ch->vec ch)]
    (is (vector? result))
    (is (= 10000 (count result)))
    (is (= 999 (get result 999)))))

(deftest test-ch-map
  (let [ch (au/coll->ch (range 10))
        result-ch (au/ch-map slow-identity ch)
        result (au/ch->vec result-ch)]
    (is (vector? result))
    (is (= 10 (count result)))))

(deftest test-async-map
  (time (let [results (au/async-map 10 (partial custom-slow-identity 100) (range 100))]
          (is (= 100 (count results)))
          (prn results))))

(run-tests)
