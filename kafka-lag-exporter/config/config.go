package config

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type KafkaYaml struct {
	Kafka KafkaConfig `yaml:"kafka"`
}

type KafkaConfig struct {
	ClientID         string   `yaml:"client_id"`
	KafkaVersion     string   `yaml:"kafka_version"`
	UseSASL          bool     `yaml:"use_sasl"`
	SaslMechanism    string   `yaml:"sasl_mechanism"`
	UseSASLHandshake bool     `yaml:"use_sasl_handshake"`
	SaslUsername     string   `yaml:"sasl_username"`
	SaslPassword     string   `yaml:"sasl_password"`
	BootStrap        []string `yaml:"boot_strap"`
}

func ReadConfig(filePath string) (KafkaConfig, error) {
	cfg := new(KafkaYaml)

	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		errors.Wrap(err, "Read config failed")
	}

	if err := yaml.Unmarshal(file, &cfg); err != nil {
		errors.Wrap(err, "unable to read config")
	}
	kafkaConf := cfg.Kafka

	return kafkaConf, nil
}
