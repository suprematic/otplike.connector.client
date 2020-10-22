(ns otplike.connector.monitors
  (:refer-clojure :exclude [empty]))


;; =================================================================
;; API
;; =================================================================


(defn empty []
  {:pid->mrefs {}
   :node->monitors {}
   :pending-mref->monitor {}
   :confirmed-mref->monitor {}
   :monitored {}})


(defn add-pending [monitors mref pid target]
  (-> monitors
    (assoc-in [:pending-mref->monitor mref] {:pid pid :target target})
    (update-in [:pid->mrefs pid] #(conj (or % #{}) mref))))


(defn remove-pending [monitors mref]
  (if-some [{:keys [pid]} (get-in monitors [:pending-mref->monitor mref])]
    (let [monitors (update monitors :pending-mref->monitor dissoc mref)
          mrefs (-> monitors :pid->mrefs (get pid) (disj mref))]
      (if (empty? mrefs)
        (update monitors :pid->mrefs dissoc pid)
        (assoc-in monitors [:pid->mrefs pid] mrefs)))
    monitors))


(defn get-pending [monitors mref]
  (get-in monitors [:pending-mref->monitor mref]))


(defn confirm-pending [monitors mref target-node]
  (if-some [{:keys [pid target]}
            (get-in monitors [:pending-mref->monitor mref])]
    (let [info {:mref mref :pid pid :target target :node target-node}]
      (-> monitors
        (update :pending-mref->monitor dissoc mref)
        (assoc-in [:confirmed-mref->monitor mref] info)
        (assoc-in [:node->monitors target-node mref] info)))
    monitors))


(defn remove-confirmed [monitors mref]
  (if-some [{:keys [node pid]}
            (get-in monitors [:confirmed-mref->monitor mref])]
    (let [monitors (update monitors :confirmed-mref->monitor dissoc mref)
          node-monitors (-> monitors :node->monitors (get node) (dissoc mref))
          monitors
          (if (empty? node-monitors)
            (update monitors :node->monitors dissoc node)
            (assoc-in monitors [:node->monitors node] node-monitors))
          pid-mrefs (-> monitors :pid->mrefs (get pid) (disj mref))]
      (if (empty? pid-mrefs)
        (update monitors :pid->mrefs dissoc pid)
        (assoc-in monitors [:pid->mrefs pid] pid-mrefs)))
    monitors))


(defn get-confirmed [monitors mref]
  (get-in monitors [:confirmed-mref->monitor mref]))


(defn get-node-monitors [monitors node]
  (-> monitors (get-in [:node->monitors node]) vals))


(defn remove-node
  [{:keys [node->monitors pid->mrefs] :as monitors} node]
  (let [node-monitors (get node->monitors node)
        mrefs (keys node-monitors)
        pids (mapv :pid (vals node-monitors))
        monitoring-pids (select-keys pid->mrefs pids)
        new-pid->mrefs
        (reduce-kv
          (fn [acc k v]
            (let [pid-mrefs (apply disj v mrefs)]
              (if (empty? pid-mrefs)
                (dissoc acc k)
                (assoc acc k (apply disj v mrefs)))))
          pid->mrefs
          monitoring-pids)]
    (-> monitors
      (assoc :pid->mrefs new-pid->mrefs)
      (update :node->monitors dissoc node)
      (update :pending-mref->monitor #(apply dissoc % mrefs))
      (update :confirmed-mref->monitor #(apply dissoc % mrefs)))))


(defn remove-pid
  [{:keys [node->monitors confirmed-mref->monitor pid->mrefs] :as monitors}
   pid]
  (if-some [pid-mrefs (get pid->mrefs pid)]
    (let [pid-mref->monitor (select-keys confirmed-mref->monitor pid-mrefs)
          pid-nodes (->> pid-mref->monitor vals (mapv :node) set)
          node->monitors
          (reduce
            (fn [acc node]
              (let [mref->monitors (apply dissoc (get acc node) pid-mrefs)]
                (if (empty? mref->monitors)
                  (dissoc acc node)
                  (assoc acc node mref->monitors))))
            node->monitors
            pid-nodes)]
      (-> monitors
        (assoc :node->monitors node->monitors)
        (update :pid->mrefs dissoc pid)
        (update :pending-mref->monitor #(apply dissoc % pid-mrefs))
        (update :confirmed-mref->monitor #(apply dissoc % pid-mrefs))))
    monitors))


(defn add-monitored [monitors mref pid]
  (update monitors :monitored assoc mref pid))


(defn get-monitored [monitors mref]
  (get-in monitors [:monitored mref]))


(defn remove-monitored [monitors mref]
  (update monitors :monitored dissoc mref))
