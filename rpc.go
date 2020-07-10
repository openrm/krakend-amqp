package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/streadway/amqp"
	"github.com/go-contrib/uuid"

	"github.com/devopsfaith/krakend/config"
	"github.com/devopsfaith/krakend/proxy"
)

const rpcNamespace = "github.com/devopsfaith/krakend-amqp/rpc"
const directReplyTo = "amq.rabbitmq.reply-to"

var errNoRpcCfgDefined = errors.New("no amqp rpc defined")

func getRpcConfig(remote *config.Backend) (*rpcCfg, error) {
	v, ok := remote.ExtraConfig[rpcNamespace]
	if !ok {
		return nil, errNoRpcCfgDefined
	}

	b, _ := json.Marshal(v)
	cfg := &rpcCfg{}
	err := json.Unmarshal(b, cfg)
	return cfg, err
}

type rpcCfg struct {
	queueCfg
	Mandatory     bool   `json:"mandatory"`
	Immediate     bool   `json:"immediate"`
	ExpirationKey string `json:"exp_key"`
	MessageIdKey  string `json:"msg_id_key"`
	PriorityKey   string `json:"priority_key"`
	RoutingKey    string `json:"routing_key"`
}

func (f backendFactory) initRpc(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	if len(remote.Host) < 1 {
		return proxy.NoopProxy, errNoBackendHostDefined
	}
	dns := remote.Host[0]

	cfg, err := getRpcConfig(remote)
	if err != nil {
		f.logger.Debug(fmt.Sprintf("AMQP: %s: %s", dns, err.Error()))
		return proxy.NoopProxy, err
	}

	ch, close, err := f.newChannel(dns)
	if err != nil {
		f.logger.Error(fmt.Sprintf("AMQP: getting the channel for %s/%s: %s", dns, cfg.Name, err.Error()))
		return proxy.NoopProxy, err
	}

	err = ch.ExchangeDeclare(
		cfg.Exchange,     // name
		cfg.ExchangeType, // type
		cfg.Durable,
		cfg.Delete,
		cfg.Exclusive,
		cfg.NoWait,
		nil,
	)
	if err != nil {
		f.logger.Error(fmt.Sprintf("AMQP: declaring the exchange for %s/%s: %s", dns, cfg.Name, err.Error()))
		close()
		return proxy.NoopProxy, err
	}

	replies, err := ch.Consume(directReplyTo, "", true, true, false, false, nil)
	if err != nil {
		f.logger.Error(fmt.Sprintf("AMQP: consuming direct reply queue %s/%s: %s", dns, cfg.Name, err.Error()))
		return proxy.NoopProxy, err
	}

	go func() {
		<-ctx.Done()
		close()
	}()

	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}

		correlationId := uuid.NewV4().String()

		contentType := ""
		headers := amqp.Table{}
		for k, vs := range r.Headers {
			headerValues := make([]interface{}, len(vs))
			for k, v := range vs {
				headerValues[k] = v
			}
			headers[k] = headerValues
		}

		pub := amqp.Publishing{
			Headers:       headers,
			ContentType:   contentType,
			Body:          body,
			Timestamp:     time.Now(),
			Expiration:    r.Params[cfg.ExpirationKey],
			ReplyTo:       directReplyTo,
			MessageId:     r.Params[cfg.MessageIdKey],
			CorrelationId: correlationId,
		}

		if len(r.Headers["Content-Type"]) > 0 {
			pub.ContentType = r.Headers["Content-Type"][0]
		}

		if v, ok := r.Params[cfg.PriorityKey]; ok {
			if i, err := strconv.Atoi(v); err == nil {
				pub.Priority = uint8(i)
			}
		}

		err = ch.Publish(
			cfg.Exchange,
			r.Params[cfg.RoutingKey],
			cfg.Mandatory,
			cfg.Immediate,
			pub,
		)
		if err != nil {
			return nil, err
		}

		for {
			select {
			case reply := <-replies:
				if reply.CorrelationId == correlationId {
					return decodeMessage(remote, reply)
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}, nil
}
