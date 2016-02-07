Riemann Dashboard plugin
=================

Test
Test2

A clone of [Riemann-Dash](https://github.com/aphyr/riemann-dash) packaged as a [Riemann](http://riemann.io) plugin.

Get Started
-----------------

Build plugin jar

``` bash
    $ git clone ...
	$ lein jar
```

Add it to riemann classpath

Configuration
-----------------


``` clojure
	(load-plugin "dashboard")
	(dashboard/dashboard-server {:host "my-ip" :port "my-port" :config "/path/to/my.config"})
```
