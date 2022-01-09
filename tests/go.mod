module github.com/alexrudd/gomdb/tests

go 1.16

require (
	github.com/alexrudd/gomdb v0.0.0-local
	github.com/alexrudd/gomdb/cgroup v0.0.0-local
	github.com/gofrs/uuid v4.2.0+incompatible
	github.com/lib/pq v1.10.2
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/alexrudd/gomdb v0.0.0-local => ../

replace github.com/alexrudd/gomdb/cgroup v0.0.0-local => ../cgroup
