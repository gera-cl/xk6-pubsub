package pubsub

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib"

	"github.com/mitchellh/mapstructure"
	"google.golang.org/api/option"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/pubsub".
func init() {
	modules.Register("k6/x/pubsub", new(PubSub))
}

// PubSub is the k6 extension for a Google Pub/Sub client.
// See https://cloud.google.com/pubsub/docs/overview
type PubSub struct{}

// publisherConf provides a Pub/Sub publisher client configuration. This configuration
// structure can be used on a client side. All parameters are optional.
type publisherConf struct {
	ProjectID                 string
	Credentials               string
	PublishTimeout            int
	Debug                     bool
	Trace                     bool
	DoNotCreateTopicIfMissing bool
}

// Publisher is the basic wrapper for Google Pub/Sub publisher and uses
// watermill as a client. See https://github.com/ThreeDotsLabs/watermill/
//
// Publisher represents the constructor and creates an instance of
// googlecloud.Publisher with provided projectID and publishTimeout.
// Publisher uses watermill StdLoggerAdapter logger.
func (ps *PubSub) Publisher(config map[string]interface{}) *googlecloud.Publisher {
	cnf := &publisherConf{}
	err := mapstructure.Decode(config, cnf)
	if err != nil {
		log.Fatalf("xk6-pubsub: unable to read publisher config: %v", err)
	}

	if cnf.PublishTimeout < 1 {
		cnf.PublishTimeout = 5
	}

	client, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID:                 cnf.ProjectID,
			Marshaler:                 googlecloud.DefaultMarshalerUnmarshaler{},
			PublishTimeout:            time.Second * time.Duration(cnf.PublishTimeout),
			DoNotCreateTopicIfMissing: cnf.DoNotCreateTopicIfMissing,
			ClientOptions:             withCredentials(cnf.Credentials),
		},
		watermill.NewStdLogger(cnf.Debug, cnf.Trace),
	)

	if err != nil {
		log.Fatalf("xk6-pubsub: unable to init publisher: %v", err)
	}

	return client
}

// Publish publishes a message using the function publishMessage.
// The msg value must be passed as string and will be converted to bytes
// sequence before publishing.
func (ps *PubSub) Publish(ctx context.Context, p *googlecloud.Publisher, topic, msg string) error {
	newMessage := message.NewMessage(watermill.NewShortUUID(), []byte(msg))
	return publishMessage(ctx, p, topic, newMessage)
}

// PublishWithAttributes publishes a message using the function publishMessage.
// The msg value must be passed as string and will be converted to a bytes
// sequence before publishing. The attributes value must be passed as map[string]string
// and will be set as metadata.
func (ps *PubSub) PublishWithAttributes(ctx context.Context, p *googlecloud.Publisher, topic, msg string, attributes map[string]string) error {
	newMessage := createMessage(watermill.NewShortUUID(), []byte(msg), attributes)
	return publishMessage(ctx, p, topic, newMessage)
}

// publishMessage publishes a message to the provided topic using provided
// googlecloud.Publisher. The message value must be passed as Message, a watermill struct.
func publishMessage(ctx context.Context, p *googlecloud.Publisher, topic string, message *message.Message) error {
	state := lib.GetState(ctx)

	if state == nil {
		err := errors.New("xk6-pubsub: state is nil")
		ReportError(err, "cannot determine state")
		return err
	}

	err := p.Publish(
		topic,
		message,
	)

	if err != nil {
		ReportError(err, "xk6-pubsub: unable to publish message")
		return err
	}

	return nil
}

// withCredentials explicitly setup Pub/Sub credentials as option.ClientOption.
func withCredentials(credentials string) []option.ClientOption {
	var opt []option.ClientOption

	if len(credentials) > 0 {
		opt = append(opt, option.WithCredentialsJSON([]byte(credentials)))
	}

	return opt
}

// createMessage function creates a Message object including metadata.
func createMessage(uuid string, payload message.Payload, metadata message.Metadata) *message.Message {
	return &message.Message{
		UUID:     uuid,
		Metadata: metadata,
		Payload:  payload,
	}
}
