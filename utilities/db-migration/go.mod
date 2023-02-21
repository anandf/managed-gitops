module github.com/redhat-appstudio/managed-gitops/utilities/db-migration

go 1.18

require (
	github.com/go-pg/pg/extra/pgdebug v0.2.0
	github.com/go-pg/pg/v10 v10.10.6
	github.com/golang-migrate/migrate/v4 v4.15.2
)

require (
	github.com/fsnotify/fsnotify v1.5.2 // indirect
	github.com/go-pg/zerochecker v0.2.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/lib/pq v1.10.0 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.20.1 // indirect
	github.com/stretchr/testify v1.7.1 // indirect
	github.com/tmthrgd/go-hex v0.0.0-20190904060850-447a3041c3bc // indirect
	github.com/vmihailenco/bufpool v0.1.11 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.4 // indirect
	github.com/vmihailenco/tagparser v0.1.2 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292 // indirect
	golang.org/x/net v0.0.0-20220722155237-a158d28d115b // indirect
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
	mellium.im/sasl v0.2.1 // indirect
)

replace (
	github.com/redhat-appstudio/managed-gitops/backend-shared => ../../backend-shared
	sigs.k8s.io/controller-runtime => github.com/kcp-dev/controller-runtime v0.12.2-0.20220808200255-4b60fd66e5de
)
