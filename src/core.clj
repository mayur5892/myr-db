(ns core
  (:require [medley.core :as m]))

(def sql->clj-spec {:int int?
               :string string?
               :double double?})

(defn- valid? [constraints data]
  (when (= (count constraints) (count data))
    (some (fn [[ k v]]
            (let [constraint (constraints k)]
              (if (coll? constraint)
                (some (fn [c]
                        (some-> (sql->clj-spec c)
                                (#(% v))))
                      constraint)
                ((sql->clj-spec constraint) v))))
          data)))

(defn init-db []
  (let [db (atom {})]
    db))


(defn create-table [db table-name columns]
  (let [primary-column (some (fn [[ k v]]
                               (println k v )
                               (when (and
                                       (coll? v)
                                       ((set v) :primary-key))
                                 k))
                             columns)]
    (swap! db assoc-in [table-name :meta :constraints] columns)
    (swap! db assoc-in [table-name :meta :subscriptions] [])
    (swap! db assoc-in [table-name :meta :primary-key] primary-column)
    (swap! db assoc-in [table-name :data] {}))
  )

(defn insert [db table-name input-data]
  (let [constraints (get-in @db [table-name :meta :constraints])
        primary-key (get-in @db [table-name :meta :primary-key])]
    (when (valid? constraints input-data)
      ;; check primary key already present
      (swap! db assoc-in [table-name :data]
             (assoc
               (get-in @db [table-name :data])
               (input-data primary-key)
               input-data))
      )))

(defn index-search [data query]
  (if (fn? query)
    (->> (keys data)
        (filter query)
         (filter #(data %)))
    (data query)))

(defn seq-search [data query key]
  (if (fn? query)
    (->> (vals data)
         (filter (fn [row]
                   (query (row key))) ))
    (filter #(= query (% key))
            (vals data))))

(defn select [db table-name clause]
  (let [data (get-in @db [table-name :data])
        primary-key (get-in @db [table-name :meta :primary-key])
        [k query] (first clause)]
    (if (= primary-key k)
      (index-search data query)
      (seq-search data query k))
    ))

(defn event-listener [db table-name event old-data id]
  (let [subscriptions (get-in @db [table-name :meta :subscriptions])]
    (doseq [{:keys [clause subscription]} subscriptions]
      (when (= id (:id clause))
        (subscription event (old-data id) ((get-in @db [table-name :data]) id)))
      )))

(defn update-db [db table-name {:keys [id] :as query}]
  (let [data (get-in @db [table-name :data])]
    (swap! db m/update-existing-in [table-name :data id] merge query)
    @(future (event-listener db table-name :update data id))))

(defn subscribe [db table-name clause subscription]
  (swap! db update-in [table-name :meta :subscriptions]
         conj
         {:clause clause :subscription subscription}))


(comment
  (def db (init-db))
  (create-table db :employee {:id [:int :primary-key]  :email :string :salary :double})
  (insert db :employee {:id 1 :email "roy@acme.com" :salary 1233.44})
  (select db :employee {:id 1})
  (select db :employee {:salary #(> % 1000)})
  (update-db db :employee {:id 1 :salary 2000})
  (defn employee-subscription [opr old-employee new-employee]
    (println opr old-employee new-employee))
  (subscribe db :employee {:id 1} employee-subscription))
