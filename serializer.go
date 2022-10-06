package goharvest

import (
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/avro"
)

/*
Interfaces.
*/

// SchemaSerializer specifies the methods of a minimal schema serializer.
type SchemaSerializer interface {
	Serialize(topic string, value []byte) ([]byte, error)
}

// SchemaSerializerProvider is a factory for creating SchemaSerializer instances.
type SchemaSerializerProvider func(conf *SchemaSerializerConfig) (SchemaSerializer, error)

/*
Standard serializer implementations.
*/

// StandardSchemaSerializerProvider returns a factory for creating a conventional SchemaSerializer, backed by the real client API.
func StandardSchemaSerializerProvider() SchemaSerializerProvider {
	return func(conf *SchemaSerializerConfig) (SchemaSerializer, error) {
		return NewSchemaRegistrySerializer(conf)
	}
}

// SchemaRegistrySerializer implements SchemaSerializer using the Confluent Schema Registry.
type SchemaRegistrySerializer struct {
	serializer *avro.GenericSerializer
}

// NewSchemaRegistrySerializer creates a new SchemaRegistrySerializer.
func NewSchemaRegistrySerializer(conf *SchemaSerializerConfig) (SchemaSerializer, error) {
	// Create a new schema registry client.
	client, err := schemaregistry.NewClient(toSchemaRegistryNativeConfig(conf))
	if err != nil {
		return nil, err
	}

	// Create a new serializer config.
	serConf := avro.NewSerializerConfig()
	serConf.UseLatestVersion = conf.UseLatestVersion

	// Create a new generic serializer.
	genericSer, err := avro.NewGenericSerializer(client, serde.ValueSerde, serConf)
	if err != nil {
		return nil, err
	}

	// Use topic name as subject name strategy.
	genericSer.SubjectNameStrategy = func(topic string, serdeType serde.Type, schema schemaregistry.SchemaInfo) (string, error) {
		return topic, nil
	}

	return &SchemaRegistrySerializer{
		serializer: genericSer,
	}, nil
}

// Serialize serializes the given value using the Confluent Schema Registry.
func (s *SchemaRegistrySerializer) Serialize(topic string, value []byte) ([]byte, error) {
	return s.serializer.Serialize(topic, value)
}

/*
Various helpers.
*/

func toSchemaRegistryNativeConfig(conf *SchemaSerializerConfig) *schemaregistry.Config {
	result := schemaregistry.Config{
		SchemaRegistryURL:              conf.SchemaRegistryURL,
		BasicAuthUserInfo:              conf.BasicAuthUserInfo,
		BasicAuthCredentialsSource:     conf.BasicAuthCredentialsSource,
		SaslMechanism:                  conf.SaslMechanism,
		SaslUsername:                   conf.SaslUsername,
		SaslPassword:                   conf.SaslPassword,
		SslCertificateLocation:         conf.SslCertificateLocation,
		SslKeyLocation:                 conf.SslKeyLocation,
		SslCaLocation:                  conf.SslCaLocation,
		SslDisableEndpointVerification: conf.SslDisableEndpointVerification,
		ConnectionTimeoutMs:            conf.ConnectionTimeoutMs,
		RequestTimeoutMs:               conf.RequestTimeoutMs,
		CacheCapacity:                  conf.CacheCapacity,
	}
	return &result
}
