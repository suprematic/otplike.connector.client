(ns otplike.connector.example
  (:require
   [clojure.core.match :refer [match]]
   [otplike.connector :as connector]
   [otplike.process :as process :refer [!]]
   [otplike.trace :as trace]))


(def url "ws://localhost:9100/connect")


(defonce root (atom nil))


(defn test-register [n]
  (process/spawn
    (process/proc-fn []
      (println "registering")
      (connector/register [:name n])
      (println "waiting for message")
      (process/receive!
        [:hi! pid]
        (do
          (println "Hooray! Got my message!" pid)
          (! pid :hi!))))))


(defn test-route [n]
  (process/spawn
    (process/proc-fn []
      (println "sending pid")
      (! n [:hi! (process/self)])
      (println "waiting for message")
      (process/receive!
        :hi! (println "Hooray! Got hi back!")))))


(defn test-unregister [n]
  (process/spawn
    (process/proc-fn []
      (println "registering")
      (connector/register [:name n])
      (println "waiting for message")
      (connector/unregister [:name n])
      (process/receive!
        _ :ok
        (after 2000
          :ok)))))


(defn trace-processes []
  (process/trace
    trace/crashed?
    (fn [{pid :pid {:keys [reason]} :extra}]
      (println
        (str pid " terminated abnormally with reason:\n")
        (clojure.pprint/write reason :stream nil)))))



(defn stop []
  (let [[pid _] (reset-vals! root nil)]
    (if pid
      (let[stop-promise (promise)]
        (! pid [:stop stop-promise])
        (deref stop-promise 3000 :timeout))
      
      :not-started)))


(defn start []
  (if @root
    :already-stated
    (reset! root
      (process/spawn-opt
        (process/proc-fn []
          (trace-processes)
          (match (process/await!
                   (connector/start-link< url {} #_{:ping-timeout-ms 5000}))
            [:ok pid]
            (loop []
              (process/receive!
                [:stop stop-promise]
                (do
                  (process/exit pid :shutdown)
                  (process/receive!
                    [:EXIT pid reason]
                    (do
                      (println "connector exited with reason" reason)
                      (deliver stop-promise :ok))))

                [:EXIT pid reason]
                (do
                  (reset! root nil)
                  (println "connector exited with reason" reason))

                msg
                (do
                  (println "the root proc got an unexpected message" msg)
                  (recur))))

            [:error reason]
            (do
              (reset! root nil)
              (println "cannot start connector" \newline
                (clojure.pprint/write reason :stream nil)))))
        {:flags {:trap-exit true}}))))


(defn restart []
  (stop)
  (start))
