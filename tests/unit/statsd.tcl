start_server {tags {"statsd"}} {
    test "Statsd Config Enable/Disable" {
        # Default
        assert_equal "statsd-enabled no" [r config get statsd-enabled]

        # Turn on
        r config set statsd-enabled yes
        assert_equal "statsd-enabled yes" [r config get statsd-enabled]

        # Turn off again
        r config set statsd-enabled no
        assert_equal "statsd-enabled no" [r config get statsd-enabled]
    }

    test "Statsd Config Get/Set Host" {
        # Default
        assert_equal "statsd-host {localhost 8125}" [r config get statsd-host]

        # Change host only
        r config set statsd-host "foo"
        assert_equal "statsd-host {foo 8125}" [r config get statsd-host]

        # Change host + port
        r config set statsd-host "bar 1234"
        assert_equal "statsd-host {bar 1234}" [r config get statsd-host]

    }

    test "Statsd Config Get/Set Prefix/Suffix" {
        # Default
        assert_equal "statsd-prefix {}" [r config get statsd-prefix]
        assert_equal "statsd-suffix {}" [r config get statsd-suffix]

        r config set statsd-prefix "prefix"
        r config set statsd-suffix "suffix"

        assert_equal "statsd-prefix prefix" [r config get statsd-prefix]
        assert_equal "statsd-suffix suffix" [r config get statsd-suffix]
    }

}
