package xk6pubsub

import (
	"context"
	"errors"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	vkit "cloud.google.com/go/pubsub/apiv1"
	gax "github.com/googleapis/gax-go/v2"
	"github.com/mitchellh/mapstructure"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib"
	"google.golang.org/api/option"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/pubsub".
func init() {
	modules.Register("k6/x/pubsub", new(RootModule))
}

type RootModule struct{}

// PubSub is the k6 extension for a Google Pub/Sub client.
// See https://cloud.google.com/pubsub/docs/overview
type PubSub struct {
	vu modules.VU
}

var (
	_ modules.Module   = &RootModule{}
	_ modules.Instance = &PubSub{}
)

func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &PubSub{vu: vu}
}

func (ps *PubSub) Exports() modules.Exports {
	return modules.Exports{Default: ps}
}

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

// Publisher is the basic wrapper for Google Pub/Sub publisher.
// Publisher represents the constructor and creates an instance of
// pubsub.PublisherClient with provided projectID.
func (ps *PubSub) Publisher(config map[string]interface{}) *pubsub.Client {
	cnf := &publisherConf{}
	err := mapstructure.Decode(config, cnf)
	if err != nil {
		log.Fatalf("xk6-pubsub: unable to read publisher config: %v", err)
	}

	if cnf.PublishTimeout < 1 {
		cnf.PublishTimeout = 5
	}

	ctx := context.Background()

	// Init Client Config
	clientConfig := &pubsub.ClientConfig{
		PublisherCallOptions: &vkit.PublisherCallOptions{
			Publish: []gax.CallOption{
				gax.WithTimeout(time.Duration(cnf.PublishTimeout) * time.Second),
			},
		},
	}

	// Init Client Options
	var opt []option.ClientOption

	// Add WithCredentialsJSON
	if len(cnf.Credentials) > 0 {
		opt = append(opt, option.WithCredentialsJSON([]byte(cnf.Credentials)))
	}

	client, err := pubsub.NewClientWithConfig(ctx, cnf.ProjectID, clientConfig, opt...)
	if err != nil {
		log.Fatalf("xk6-pubsub: unable to init publisher: %v", err)
	}

	return client
}

// Publish publishes a message using the function publishMessage.
// The msg value must be passed as string and will be converted to bytes
// sequence before publishing.
func (ps *PubSub) Publish(p *pubsub.Client, topic, msg string) error {
	return publishMessage(p, topic, []byte(msg), ps.vu.State())
}

// PublishWithAttributes publishes a message using the function publishMessage.
// The msg value must be passed as string and will be converted to a bytes
// sequence before publishing. The attributes value must be passed as map[string]string
// and will be set as metadata.
func (ps *PubSub) PublishWithAttributes(p *pubsub.Client, topic, msg string, attributes map[string]string) error {
	return publishMessage(p, topic, []byte(msg), ps.vu.State(), attributes)
}

// publishMessage publishes a message to the provided topic using provided
// pubsub.PublisherClient.
func publishMessage(p *pubsub.Client, topic string, data []byte, state *lib.State, attributes ...map[string]string) error {
	if state == nil {
		err := errors.New("xk6-pubsub: state is nil")
		ReportError(err, "cannot determine state")
		return err
	}

	msg := &pubsub.Message{
		Data: data,
	}
	if len(attributes) > 0 {
		msg.Attributes = attributes[0]
	}

	ctx := context.Background()
	res := p.Topic(topic).Publish(ctx, msg)
	_, err := res.Get(ctx)
	if err != nil {
		ReportError(err, "xk6-pubsub: unable to publish message")
		return err
	}

	return nil
}
