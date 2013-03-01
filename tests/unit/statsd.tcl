start_server {tags {"statsd"}} {
    test "Statsd Config Get/Set Host" {
        # Default
        assert_equal "statsd-host {localhost 8125}" [r config get statsd-host]

        # Change host only
        #r config set statsd-host foo
        #assert_equal "statsd-host {foo 8125}" [r config get statsd-host]

        # Change host + port
        #r config set statsd-host bar 1234
        #assert_equal "statsd-host {bar 1234}" [r config get statsd-host]
    }
}
