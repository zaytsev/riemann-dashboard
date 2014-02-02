(ns riemann.plugin.dashboard
  "A clone of Riemann-Dash (https://github.com/aphyr/riemann-dash) implemented as
   Riemann core service"
  (:require [aleph.formats :as formats]
            [aleph.http :refer [start-http-server]]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.java.io :refer [file reader resource writer]]
            [clojure.tools.logging :refer [info warn]]
            [gloss.core :as gloss]
            [lamina.core :refer [close
                                 enqueue
                                 enqueue-and-close
                                 lazy-seq->channel
                                 map*
                                 on-closed
                                 receive-all]]
            [riemann.common :refer [event-to-json]]
            [riemann.config :refer [service!]]
            [riemann.index :as index]
            [riemann.pubsub :as p]
            [riemann.query :as query]
            [riemann.service :refer [Service ServiceEquiv]])
  (:import (java.io InputStreamReader PushbackReader)))

(defn http-query-map
  "Converts a URL query string into a map."
  [string]
  (apply hash-map
    (map formats/url-decode
      (mapcat (fn [kv] (string/split kv #"=" 2))
        (string/split string #"&")))))

(defn split-lines
  "Takes a channel of bytes and returns a channel of utf8 strings, split out by
  \n."
  [ch]
  (formats/decode-channel
    (gloss/string :utf-8 :delimiters ["\n"])
    ch))

(defn ws-pubsub-handler [core ch hs]
  (let [topic  (formats/url-decode (last (string/split (:uri hs) #"/" 3)))
        params (http-query-map (:query-string hs))
        query  (params "query")
        pred   (query/fun (query/ast query))
        sub    (p/subscribe! (:pubsub core) topic
                 (fn [event]
                   (when (pred event)
                     (enqueue ch (event-to-json event))))
                 true)]
    (info "New websocket subscription to" topic ":" query)
    (receive-all ch (fn [msg]
                      (when-not msg
                                        ; Shut down channel
                        (info "Closing websocket "
                          (:remote-addr hs) topic query)
                        (close ch)
                        (p/unsubscribe! (:pubsub core) sub))))))

(defn ws-index-handler
  "Queries the index for events and streams them to the client. If subscribe is
  true, also initiates a pubsub subscription to the index topic with that
  query."
  [core ch hs]
  (let [params (http-query-map (:query-string hs))
        query  (params "query")
        ast    (query/ast query)]
    (when-let [i (:index core)]
      (doseq [event (index/search i ast)]
        (enqueue ch (event-to-json event))))
    (if (= (params "subscribe") "true")
      (ws-pubsub-handler core ch (assoc hs :uri "/pubsub/index"))
      (close ch))))

(defn json-stream-response
  "Given a channel of events, returns a Ring HTTP response with a body
  consisting of a JSON array of the channel's contents."
  [ch]
  (let [out (map* #(str (json/generate-string %) "\n") ch)]
    {:status 200
     :headers {"Content-Type" "application/x-json-stream"}
     :body out}))

(defn channel->reader
  [ch]
  (InputStreamReader.
    (formats/channel->input-stream ch)))

(defn json-channel
  "Takes a channel of bytes containing a JSON array, like \"[1 2 3]\" and
  returns a channel of JSON messages: 1, 2, and 3."
  [ch1]
  (let [reader (channel->reader ch1)
        ch2 (-> reader
              (json/parsed-seq true)
              lazy-seq->channel)]
    (on-closed ch2 #(close ch1))
    ch2))

(def default-dashboard-config {})

(defn check-config! [config]
  (cond
    (string? config)
    (let [f (file config)
          complain #(throw (IllegalArgumentException.
                             (str "Configuration file " config " " %)))]
      (cond
        (not (.isFile f)) (complain "is not a regular file")
        (not (.exists f)) (complain "is not exists")
        (not (.canRead f)) (complain "is not readable")
        (not (.canWrite f)) (complain "is not writeable")
        :else config))
    (map? config) (atom config)
    :else (throw (IllegalArgumentException. "Invalid config: " config))))

(defmulti read-config type)

(defmethod read-config java.lang.String [fname]
  (binding [*read-eval* false]
    (with-open [r (reader fname)]
      (read (PushbackReader. r)))))

(defmethod read-config clojure.lang.Atom [conf]
  @conf)

(defmulti write-config (fn [conf v]
                         (type conf)))

(defmethod write-config java.lang.String [fname v]
  (with-open [w (writer fname)]
    (binding [*out* w]
      (pr v)
      v)))

(defmethod write-config clojure.lang.Atom [conf v]
  (reset! conf v))

(defn config-ok [ch v]
  (enqueue ch {:status 200
               :headers {"Content-Type" "application/json"}
               :body (json/encode v)}))

(defmulti handle-config-req (fn [config ch hs] (:request-method hs)))

(defmethod handle-config-req :get [config ch _]
  (config-ok ch (read-config config)))

(defmethod handle-config-req :post [config ch hs]
  (let [body (formats/channel-buffer->string (:body hs))
        v (json/parse-string body)]
    (config-ok ch (write-config config v))))

(defmethod handle-config-req :default [_ ch _]
  (enqueue ch {:status 405
               :body "Method not allowed"}))

(defn config-handler [config ch hs]
  (try
    (handle-config-req config ch hs)
    (catch Exception e
      (warn "Failed to handle config request" e)
      (enqueue ch {:status 500
                   :body "Internal error"}))))

(def default-content-type "application/octet-stream")

(defn guess-content-type [^String resource-path]
  (if resource-path
    (let [ext-ind (.lastIndexOf resource-path ".")
          ext (when (> ext-ind 0) (.substring resource-path (+ ext-ind 1)))]
      ({"js" "text/javascript"
        "html" "text/html"
        "css" "text/css"
        "png" "image/png"} ext default-content-type))
    default-content-type))

(defn resource-handler [ch hs]
  (let [uri (:uri hs)
        uri (if (or (empty? uri) (= "/" uri)) "/index.html" uri)
        resource-path (str "dashboard/public" uri)
        resource-url (resource resource-path)]
    (if-not (nil? resource-url)
      (enqueue ch {:status 200
                   :headers {"Content-Type" (guess-content-type resource-path)}
                   :body (slurp resource-url)})
      (enqueue-and-close ch {:status 404
                             :body "Not Found"}))))

(defn ws-handler [core config]
  "Returns a function which is called with new websocket connections.
  Responsible for routing requests to the appropriate handler."
  (fn [ch req]
    (info "Dashboard connection from" (:remote-addr req)
      (:uri req)
      (:query-string req))
    (condp re-matches (:uri req)
      #"/index/?"        (ws-index-handler @core ch req)
      #"/pubsub/[^/]+/?" (ws-pubsub-handler @core ch req)
      #"/config" (config-handler config ch req)
      (resource-handler ch req))))

(defrecord DashboardServer [host port config core server]
  ServiceEquiv
  (equiv? [this other]
    (and (instance? DashboardServer other)
      (= host (:host other))
      (= port (:port other))))

  Service
  (conflict? [this other]
    (and (instance? DashboardServer other)
      (= host (:host other))
      (= port (:port other))))

  (reload! [this new-core]
    (reset! core new-core))

  (start! [this]
    (locking this
      (when-not @server
        (reset! server (start-http-server (ws-handler core (check-config! config))
                         {:host host
                          :port port
                          :websocket true}))
        (info "Dashboard server" host port "online"))))

  (stop! [this]
    (locking this
      (when @server
        (@server)
        (info "Dashboard server" host port "shut down")))))

(defn dashboard-server
  "Starts a new dashboard server for a core. Starts immediately.

  Options:
  :host   The address to listen on (default 127.0.0.1)
          Currently does nothing; this option depends on an incomplete
          feature in Aleph, the underlying networking library. Aleph will
          currently bind to all interfaces, regardless of this value.
  :post   The port to listen on (default 5559)"
  ([] (dashboard-server {}))
  ([opts]
     (service!
       (DashboardServer.
         (get opts :host "127.0.0.1")
         (get opts :port 5559)
         (get opts :config default-dashboard-config)
         (atom nil)
         (atom nil)))))
