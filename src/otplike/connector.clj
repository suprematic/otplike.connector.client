(ns otplike.connector
  (:refer-clojure :exclude [send])
  (:require 
   [clojure.core.match :refer [match]]
   [cognitect.transit :as transit]
   [defun.core :refer [defun defun-]]
   [gniazdo.core :as ws]
   [otplike.gen-server :as gs]
   [otplike.process :as process :refer [!]])
  (:import
   [otplike.process Pid TRef]))


;; =================================================================
;; Internal
;; =================================================================


(def ^:private default-ping-timeout-ms 20000)


(def ^:private default-pongs-missing-allowed 2)


(defonce ^:private node-id* (atom nil))


(defn- log [& args]
  (apply println "[connector] :: " args))


(def ^:private default-transit-write-handlers
  {Pid
   (transit/write-handler
     "pid"
     (fn [^Pid pid]
       {:id (.id pid)
        :node (if (= 0 (.node pid)) @node-id* (.node pid))}))
   TRef
   (transit/write-handler
     "otp-ref" (fn [^TRef tref] {:id (.id tref)}))})


(def ^:private default-transit-read-handlers
  {"pid"
   (transit/read-handler
     (fn [{:keys [id node]}]
       (Pid. id (if (= node @node-id*) 0 node))))
   "otp-ref"
   (transit/read-handler
     (fn [{:keys [id]}]
       (TRef. id)))})


(defn- transit-writer [stream {:keys [transit-write-handlers]}]
  (transit/writer
    stream
    :json
    {:handlers (merge default-transit-write-handlers transit-write-handlers)}))


(defn- transit-reader [stream {:keys [transit-read-handlers]}]
  (transit/reader
    stream
    :json
    {:handlers (merge default-transit-read-handlers transit-read-handlers)}))


(defn- transit-send [ws form opts]
  (let [os (java.io.ByteArrayOutputStream. 4096)]
    (-> os (transit-writer opts) (transit/write form))
    (ws/send-msg ws (.toString os))))


(defn- transit-read [string opts]
  (transit/read
    (transit-reader
      (java.io.ByteArrayInputStream.
        (.getBytes string java.nio.charset.StandardCharsets/UTF_8))
      opts)))


(defn- connect [url opts]
  (ws/connect url
    :on-close
    (fn [code reason]
      (gs/cast ::server [:exit [:connection-closed code reason]]))

    :on-receive
    (fn [msg]
      (let [command
            (try
              (transit-read msg opts)
              (catch Exception ex
                (log "cannot parse command" ex)
                (gs/cast :server [:exit (process/ex->reason ex)])
                (throw ex)))]
        (if @node-id*
          (gs/cast ::server [:ws command])
          (! ::server [:ws command]))))))


(defn- send-command [{:keys [ws opts] :as state} command]
  (transit-send ws [:command command] opts)
  state)


(defn- route [state dest msg]
  (send-command
    state
    [:route
     (if (process/pid? dest)
       [:node (.node ^Pid dest)]
       dest)
     [:send dest msg]]))


(defn- add-pending-name [state pid reg-name]
  (match (get-in state [:name->pid reg-name])
    nil
    (match (get-in state [:pending-name->pid reg-name])
      nil
      [:ok
       (-> state
         (assoc-in [:pending-name->pid reg-name] pid)
         (update-in [:pending-pid->names pid] #(conj (or % #{} reg-name))))]

      pid
      (do
        (log "an attempt to register own name second time"
          :name reg-name :pid pid)
        [:ok state])

      pending-pid
      (do
        (log "an attempt to register a name pending for another process"
          :name reg-name :pid pid :pending-pid pending-pid)
        [:error [::unregistered [:name reg-name] :already-registered]]))

    pid
    (do
      (log "an attempt to register own name second time"
        :name reg-name :pid pid)
      [:ok state])

    owner-pid
    (do
      (log "an attempt to register a name registered by another process"
        :name reg-name :pid pid :owner-pid owner-pid)
      [:error [::unregistered [:name reg-name] :already-registered]])))


(defn maybe-unlink [{:keys [pending-pid->name pid->names] :as state} pid]
  (when-not (or (contains? pending-pid->name pid) (contains? pid->names pid))
    (log "unlinking process" :pid pid)
    (process/unlink pid))
  state)


(defn- remove-pending-name [{:keys [pending-pid->names] :as state} reg-name]
  (let [pid (-> state :pending-name->pid (get reg-name))
        pending-pid->names
        (let [names (-> pending-pid->names (get pid) (disj reg-name))]
          (if (empty? names)
            (dissoc pending-pid->names pid)
            (assoc pending-pid->names pid names)))]
    (-> state
      (update :pending-name->pid dissoc reg-name)
      (assoc :pending-pid->names pending-pid->names))))


(defn- add-reg-name [state pid reg-name]
  (-> state
    (assoc-in [:name->pid reg-name] pid)
    (update-in [:pid->names pid] #(conj (or % #{}) reg-name))))


(defn- remove-reg-name [{:keys [pid->names name->pid] :as state} reg-name]
  (if-some [pid (get name->pid reg-name)]
    (let [pid-names (-> pid->names (get pid) (disj reg-name))
          pid->names
          (if (empty? pid-names)
            (dissoc pid->names pid)
            (assoc pid->names pid pid-names))]
      (-> state
        (update :name->pid dissoc reg-name)
        (assoc :pid->names pid->names)))
    state))


(defun- complete-key-registration
  ([state ([:name reg-name] :as k)]
   (if-some [pid (-> state :pending-name->pid (get reg-name))]
     (do
       (log "registration completed" :key k :pid pid)
       (-> state
         (remove-pending-name reg-name)
         (add-reg-name pid reg-name)))
     (if-some [owner-pid (-> state :name->pids (get reg-name))]
       (do
         (log "got registration completion for already completed registration"
           :key k :owner-pid owner-pid)
         state)
       (do
         (log "got registration completion for an unregistered key" :key k)
         (send-command state [:unregister [[:name reg-name]]]))))))


(defn- complete-registration [state ks]
  (reduce complete-key-registration state ks))


(defun- add-pending-key
  ([state pid [:name reg-name]]
   (add-pending-name state pid reg-name))

  ([_state _pid k]
   [::invalid-key k]))


(defn- register* [state pid ks]
  (loop [new-state state
         rest-ks ks]
    (if (empty? rest-ks)
      (do
        (log "registering keys" :keys ks :pid pid)
        (process/link pid)
        (send-command new-state [:register ks]))
      (let [k (first rest-ks)]
        (match (add-pending-key new-state pid k)
          [:ok next-state]
          (recur next-state (rest ks))

          [:error reason]
          (do
            (process/exit pid reason)
            state))))))


(defn- do-unregister-name [state pid reg-name]
  (let [pid-names (-> state :pid->names (get pid))
        pid-names (disj pid-names reg-name)
        state (update state :name->pid dissoc reg-name)]
    (log "unregistering name" :name reg-name :pid pid)
    (let [state (send-command state [:unregister [[:name reg-name]]])]
      (if (empty? pid-names)
        (let [state (update state :pid->names dissoc pid)]
          (maybe-unlink state pid))
        (update state :pid->names assoc pid pid-names)))))


(defn- unregister-name [state pid reg-name]
  (match (get-in state [:name->pid reg-name])
    pid
    (do-unregister-name state pid reg-name)

    nil
    (match (get-in state [:pending-name->pid reg-name])
      pid
      (do
        (log "removing pending registration" :name reg-name :pid pid)
        (-> state
          (update :pending-name->pid dissoc reg-name)
          (maybe-unlink pid)))

      nil
      (do
        (log "an attempt to unregister a not registered name"
          :name reg-name :pid pid)
        state)
      
      pending-pid
      (do
        (log "an attempt to unregister a name pending for another process"
          :name reg-name :pid pid)
        state))

    owner-pid
    (do
      (log "an attempt to unregister a name registered by another process"
        :name reg-name :pid pid :owner-pid owner-pid)
      state)))


(defun- unregister-key
  ([state pid [:name reg-name]]
   (unregister-name state pid reg-name))

  ([state pid k]
   (log "an attempt to unregister an invalid key" :key k :pid pid)
   state))


(defn- unregister* [state pid ks]
  (reduce #(unregister-key %1 pid %2) state ks))


(defn- unregister-pid [state pid]
  (let [names (-> state :pid->names (get pid))
        pending-names (-> state :pending-pid->names (get pid))
        all-names (into names pending-names)]
    (as-> state state
      (send-command state [:unregister (mapv #(vector :name %) all-names)])
      (reduce #(remove-pending-name %1 %2) state pending-names)
      (reduce #(remove-reg-name %1 %2) state names))))


(defun- fail-key-registration
  ([state [:name reg-name] reason]
   (if-some [pid (get-in state [:pending-name->pid reg-name])]
     (let [state (remove-pending-name state reg-name)]
       (maybe-unlink state pid)
       (process/exit pid [:unregistered [:name reg-name] reason])
       state)
     (do
       (log "no process to unregister" :name reg-name)
       state))))


(defn- fail-registration [state ks reason]
  (reduce #(fail-key-registration %1 %2 reason) state ks))


(defun- send*
  ([state (pid :guard process/pid?) msg]
   (! pid msg)
   state)

  ([state [:name reg-name] msg]
   (when-let [pid (-> state :name->pid (get reg-name))]
     (! pid msg))
   state))


(defun- handle-command
  ([[:registered ks] state]
   (do
     (log "got :registered" :keys ks)
     (complete-registration state ks)))

  ([[:unregister ks reason] state]
   (do
     (log "got :unregister" :keys ks :reason reason)
     (fail-registration state ks reason)))

  ([[:message [:send dest msg]] state]
   (log "got :message" :to dest :message msg)
   (send* state dest msg))

  ([[:no-route k msg] state]
   (log "got :no-route" :key k :message msg)
   ;; TODO for linking/monitoring
   state)

  ([[:node-down node-id] state]
   (log "got :node-down" node-id)
   ;; TODO for linking/monitoring
   state)

  ([command state]
   (log "got unrecognized command" :command command)
   state))


(defun- handle-ws-message
  ([state [:pong payload]]
   (log "got pong" payload)
   (assoc state :pongs-waiting 0))

  ([state [:ping payload]]
   (let [{:keys [ws opts]} state]
     (log "got ping, sending pong" payload)
     (transit-send ws [:pong payload] opts)
     state))

  ([state [:command command]]
   (log "got command" command)
   (handle-command command state))

  ([state message]
   (log "unrecognized ws message" message)
   state))


(defn- handle-ping-timeout
  [{:keys
    [ws ping-timeout-ms ping-counter pongs-missing-allowed pongs-waiting opts]
    :as state}]
  (if (> pongs-waiting pongs-missing-allowed)
    (do
      (log "exit, pongs missing" :pongs-missing-allowed pongs-missing-allowed)
      [:stop [:pongs-missing pongs-waiting] state])
    (do
      (log "sending ping"
        :payload ping-counter
        :pongs-waiting pongs-waiting
        :pongs-missing-allowed pongs-missing-allowed)
      (transit-send ws [:ping ping-counter] opts)
      [:noreply
       (-> state
         (update :ping-counter inc)
         (update :pongs-waiting inc))
       ping-timeout-ms])))


;; ====================================================================
;; gen-server callbacks
;; ====================================================================


(defn init [url opts]
  (reset! node-id* nil)
  (let [ws (connect url opts)
        ping-timeout-ms (or (:ping-timeout-ms opts) default-ping-timeout-ms)
        pongs-missing-allowed
        (or (:pongs-missing-allowed opts) default-pongs-missing-allowed)]
    (process/async
      (process/receive!
        [:ws [:connected node-id]]
        (reset! node-id* node-id)

        (after 10000
          (process/exit :timeout)))
      (log "connected, node id " @node-id*)
      [:ok
       {:ws ws
        :ping-timeout-ms ping-timeout-ms
        :pongs-missing-allowed pongs-missing-allowed
        :ping-counter 0
        :pongs-waiting 0
        :pid->names {}
        :pending-name->pid {}
        :pending-pid->names {}
        :name->pid {}
        :opts opts}
       ping-timeout-ms])))


(defun handle-cast
  ([[:exit reason] state]
   (log "exit, reason:" reason)
   [:stop reason state])

  ([[:ws message] ({:ping-timeout-ms ping-timeout-ms} :as state)]
   [:noreply (handle-ws-message state message) ping-timeout-ms]))


(defun handle-info
  ([:timeout state]
   (handle-ping-timeout state))

  ([[::route dest msg] state]
   [:noreply (route state dest msg)])

  ([[::register pid ks] state]
   [:noreply (register* state pid ks)])

  ([[::unregister pid ks] state]
   [:noreply (unregister* state pid ks)])

  ([[:EXIT pid reason] state]
   (log "registered exit" :pid pid :reason reason)
   [:noreply (unregister-pid state pid)]))


(defn terminate [reason {:keys [ws] :as _state}]
  (log "stopping, reason" reason)
  (ws/close ws))


;; =================================================================
;; API
;; =================================================================


(defn start-link< [url opts]
  (gs/start-link-ns ::server
    [url opts]
    {:spawn-opt {:flags {:trap-exit true}}}))


(defn send [k msg]
  (! ::server [::route k msg]))


(defn register [k]
  (! ::server [::register (process/self) [k]]))


(defn unregister [k]
  (! ::server [::unregister (process/self) [k]]))
