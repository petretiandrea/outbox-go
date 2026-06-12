module petretiandrea.github.com/outbox

go 1.25.0

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/knadh/koanf/parsers/yaml v1.1.0
	github.com/knadh/koanf/providers/confmap v1.0.0
	github.com/knadh/koanf/providers/env v1.1.0
	github.com/knadh/koanf/providers/file v1.2.1
	github.com/knadh/koanf/v2 v2.3.5
	github.com/rabbitmq/amqp091-go v1.11.0
	github.com/segmentio/kafka-go v0.4.51
	github.com/spf13/cobra v1.10.2
	petretiandrea.github.com/outbox/pkg/outbox v0.0.0
)

require (
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/rogpeppe/go-internal v1.15.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace petretiandrea.github.com/outbox/pkg/outbox => ./pkg/outbox
