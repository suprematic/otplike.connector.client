(defproject otplike.connector.client "0.6.0-SNAPSHOT"
  :description "otplike connector client server"
  :url "https://github.com/suprematic/otplike.connector.client"
  :license
  {:name "Eclipse Public License - v1.0"
   :url  "https://www.eclipse.org/legal/epl-v10.html"}

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [org.clojure/core.match "1.0.0"]
   [com.cognitect/transit-clj "0.8.313"]
   [stylefruits/gniazdo "1.1.4"]
   [defun "0.3.1"]
   [otplike "0.6.1-alpha-SNAPSHOT"]]

  :profiles
  {:repl
   {:source-paths ["src" "example"]
    :repl-options {:init-ns otplike.connector.example}}})
