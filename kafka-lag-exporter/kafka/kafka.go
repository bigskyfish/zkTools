package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"kafka-lag-exporter/config"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	bootStrap []string
	conf      *sarama.Config
}

type LagExport struct {
	conf                  *sarama.Config
	client                *kafkaClient
	KafkaConsumerLag      *prometheus.Desc
	KafkaConsumerOffset   *prometheus.Desc
	KafkaConsumerEndOfSet *prometheus.Desc
}

func initKafka(opts config.KafkaConfig) (*LagExport, error) {
	conf := sarama.NewConfig()
	conf.ClientID = opts.ClientID
	conf.Metadata.RefreshFrequency = 2 * time.Minute
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.KafkaVersion)
	if err != nil {
		return nil, err
	}
	conf.Version = kafkaVersion

	if opts.UseSASL {
		// Convert to lowercase so that SHA512 and SHA256 is still valid
		opts.SaslMechanism = strings.ToLower(opts.SaslMechanism)
		switch opts.SaslMechanism {
		case "scram-sha512":
			conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		case "scram-sha256":
			conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "gssapi":
			//conf.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeGSSAPI)
			//conf.Net.SASL.GSSAPI.ServiceName = opts.serviceName
			//conf.Net.SASL.GSSAPI.KerberosConfigPath = opts.kerberosConfigPath
			//conf.Net.SASL.GSSAPI.Realm = opts.realm
			//conf.Net.SASL.GSSAPI.Username = opts.saslUsername
			//if opts.kerberosAuthType == "keytabAuth" {
			//	conf.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
			//	conf.Net.SASL.GSSAPI.KeyTabPath = opts.keyTabPath
			//} else {
			//	conf.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
			//	conf.Net.SASL.GSSAPI.Password = opts.saslPassword
			//}
			//if opts.saslDisablePAFXFast {
			//	conf.Net.SASL.GSSAPI.DisablePAFXFAST = true
			//}
		case "plain":
			conf.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		default:
			return nil, fmt.Errorf(
				`invalid sasl mechanism "%s": can only be "scram-sha256", "scram-sha512", "gssapi" or "plain"`,
				opts.SaslMechanism,
			)
		}

		conf.Net.SASL.Enable = true
		conf.Net.SASL.Handshake = opts.UseSASLHandshake

		if opts.SaslUsername != "" {
			conf.Net.SASL.User = opts.SaslUsername
		}

		if opts.SaslPassword != "" {
			conf.Net.SASL.Password = opts.SaslPassword
		}
	}

	var (
		client sarama.Client
	)
	for i := 0; i < 10; i++ {
		client, err = sarama.NewClient(opts.BootStrap, conf)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
		log.Printf("Retrying kafka client initialiazation")
	}
	kafkaClient := &kafkaClient{
		brokerList: opts.BootStrap,
		client:     client,
		conf:       conf,
		lock:       &sync.Mutex{},
	}

	lagExport := &LagExport{conf: conf, client: kafkaClient}
	return lagExport, err
}

func NewKafkaCollectExporter(conf config.KafkaConfig) *LagExport {
	kafkaLogExport, err := initKafka(conf)
	if err != nil {
		return nil
	}
	kafkaLogExport.KafkaConsumerLag = prometheus.NewDesc("kafka_consumergroup_group_lag",
		"The offset of the last consumed offset for this partition in this topic partition for this group.",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)
	kafkaLogExport.KafkaConsumerOffset = prometheus.NewDesc("kafka_consumergroup_group_offset",
		"The difference between the last produced offset and the last consumed offset for this partition in this topic partition for this group.",
		[]string{"topic", "partition"}, nil,
	)
	kafkaLogExport.KafkaConsumerEndOfSet = prometheus.NewDesc("kafka_consumergroup_group_max_offset",
		"The highest (maximum) lag in offsets for a given consumer group..",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)
	return kafkaLogExport
}

func (lag *LagExport) collectOffsetDetails(ch chan<- prometheus.Metric) {
	highOffsets := lag.client.getTopicOffsets()
	groupOffsets := lag.client.getGroupOffsets()
	if len(highOffsets) > 0 {
		for topic, partOffSet := range highOffsets {
			for part, offset := range partOffSet {
				ch <- prometheus.MustNewConstMetric(
					lag.KafkaConsumerOffset,
					prometheus.GaugeValue,
					float64(offset),
					topic,
					strconv.Itoa(int(part)),
				)
			}
		}
	}
	if len(groupOffsets) > 0 {
		for group, topics := range groupOffsets {
			for topic, partOffs := range topics {
				for part, offset := range partOffs {
					ch <- prometheus.MustNewConstMetric(
						lag.KafkaConsumerEndOfSet,
						prometheus.GaugeValue,
						float64(offset),
						group,
						topic,
						strconv.Itoa(int(part)),
					)
					if highOffsets[topic] != nil {
						highestOffSet, ok := highOffsets[topic][part]
						if ok {
							ch <- prometheus.MustNewConstMetric(
								lag.KafkaConsumerLag,
								prometheus.GaugeValue,
								float64(highestOffSet-offset),
								group,
								topic,
								strconv.Itoa(int(part)),
							)
						}
					}
				}
			}
		}
	}
}

func (lag *LagExport) Describe(ch chan<- *prometheus.Desc) {
	ch <- lag.KafkaConsumerLag
	ch <- lag.KafkaConsumerOffset
	ch <- lag.KafkaConsumerEndOfSet
}

func (lag *LagExport) Collect(ch chan<- prometheus.Metric) {
	lag.collectOffsetDetails(ch)
	return
}
