package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/m3db/m3/src/cluster/client"
	etcdcfg "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	routingpolicypb "github.com/m3db/m3/src/msg/generated/proto/routingpolicypb"
	"github.com/m3db/m3/src/x/instrument"
	"gopkg.in/yaml.v3"
)

const (
	coordinatorHealthTimeout = 30 * time.Second
)

type KVConfig struct {
	Zone      string   `yaml:"zone"`
	Env       string   `yaml:"env"`
	Endpoints []string `yaml:"endpoints"`
}

type CoordinatorConfig struct {
	BaseURL     string `yaml:"baseURL"`
	Environment string `yaml:"environment"`
	Zone        string `yaml:"zone"`
}

type ServiceID struct {
	Name        string `json:"name" yaml:"name"`
	Environment string `json:"environment" yaml:"environment"`
	Zone        string `json:"zone" yaml:"zone"`
}

type TopicConsumerConfig struct {
	ServiceID       ServiceID `yaml:"serviceId"`
	ConsumptionType string    `yaml:"consumptionType"`
	MessageTTLNanos int64     `yaml:"messageTtlNanos"`
}

type TopicConfig struct {
	Name           string                `yaml:"name"`
	NumberOfShards int                   `yaml:"numberOfShards"`
	Consumers      []TopicConsumerConfig `yaml:"consumers"`
}

type PlacementInstance struct {
	ID             string `json:"id" yaml:"id"`
	IsolationGroup string `json:"isolation_group" yaml:"isolationGroup"`
	Zone           string `json:"zone" yaml:"zone"`
	Weight         int    `json:"weight" yaml:"weight"`
	Endpoint       string `json:"endpoint" yaml:"endpoint"`
	Hostname       string `json:"hostname" yaml:"hostname"`
	Port           int    `json:"port" yaml:"port"`
}

type PlacementConfig struct {
	Service           string              `yaml:"service"`
	NumShards         int                 `yaml:"numShards"`
	ReplicationFactor int                 `yaml:"replicationFactor"`
	Instances         []PlacementInstance `yaml:"instances"`
}

type RoutingPolicyConfig struct {
	KVNamespace  string            `yaml:"kvNamespace"`
	KVKey        string            `yaml:"kvKey"`
	TrafficTypes map[string]uint64 `yaml:"trafficTypes"`
}

type Config struct {
	KV             KVConfig              `yaml:"kv"`
	Coordinator    CoordinatorConfig     `yaml:"coordinator"`
	Topics         []TopicConfig         `yaml:"topics"`
	Placements     []PlacementConfig     `yaml:"placements"`
	RoutingPolicy  *RoutingPolicyConfig  `yaml:"routingPolicy"`
}

func (c *Config) newCoordinatorClientFromConfig() *coordinatorClient {
	if c.Coordinator.BaseURL == "" {
		fatalf("coordinator.baseURL must be set in config")
	}
	if c.Coordinator.Environment == "" {
		fatalf("coordinator.environment must be set in config")
	}
	return NewCoordinatorClient(c.Coordinator.BaseURL, c.Coordinator.Environment, nil)
}

func (c *Config) newKvClientFromConfig() *kvClient {
	if c.KV.Zone == "" {
		fatalf("kv.zone must be set in config")
	}
	if c.KV.Env == "" {
		fatalf("kv.env must be set in config")
	}
	if len(c.KV.Endpoints) == 0 {
		fatalf("kv.endpoints must include at least one endpoint in config")
	}
	return NewKVClient(c.KV.Env, c.KV.Zone, c.KV.Endpoints)
}

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "./scripts/development/m3_consumer_service/m3msg-bootstrap.yaml", "bootstrap yaml path")
	flag.Parse()

	f, err := os.Open(cfgPath)
	if err != nil {
		fatalf("open config: %v", err)
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		fatalf("decode yaml: %v", err)
	}

	client := cfg.newCoordinatorClientFromConfig()
	ctx := context.Background()
	deadline := time.Now().Add(coordinatorHealthTimeout)
	for {
		if time.Now().After(deadline) {
			fatalf("coordinator health timeout")
		}
		if err := client.HealthCheck(ctx); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	for _, t := range cfg.Topics {
		// init topic if missing
		exists, err := client.TopicExists(ctx, t.Name)
		if err != nil {
			fatalf("check topic exists: %v", err)
		}
		if !exists {
			if err := client.InitTopic(ctx, t.Name, t.NumberOfShards); err != nil {
				fatalf("init topic: %v", err)
			}
		}
		// add consumers
		for _, c := range t.Consumers {
			if err := client.AddConsumer(ctx, t.Name, c.ServiceID, c.ConsumptionType, c.MessageTTLNanos); err != nil {
				fatalf("add consumer: %v", err)
			}
		}
	}

	kvClient := cfg.newKvClientFromConfig()
	for _, p := range cfg.Placements {
		if err := kvClient.SetPlacement(p); err != nil {
			fatalf("create placement: %v", err)
		}
	}

	if cfg.RoutingPolicy != nil {
		if err := kvClient.SetRoutingPolicy(*cfg.RoutingPolicy); err != nil {
			fatalf("create routing policy: %v", err)
		}
	}
}

// coordinatorClient wraps the M3 Coordinator HTTP API interactions.
type coordinatorClient struct {
	BaseURL string
	Env     string
	HTTP    *http.Client
}

// NewCoordinatorClient constructs a coordinator client using the provided baseURL
// and environment.
func NewCoordinatorClient(baseURL, environment string, httpClient *http.Client) *coordinatorClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &coordinatorClient{BaseURL: baseURL, Env: environment, HTTP: httpClient}
}

// HealthCheck checks the coordinator health endpoint and returns an error if not healthy.
func (c *coordinatorClient) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("healthcheck failed: status=%d body=%s", resp.StatusCode, string(b))
	}
	io.Copy(io.Discard, resp.Body)
	return nil
}

// TopicExists checks if a topic exists.
func (c *coordinatorClient) TopicExists(ctx context.Context, topicName string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/api/v1/topic", nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Topic-Name", topicName)
	req.Header.Set("Cluster-Environment-Name", c.Env)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	body, _ := io.ReadAll(resp.Body)
	return false, fmt.Errorf("GET topic failed: status=%d body=%s", resp.StatusCode, string(body))
}

// InitTopic initializes a topic with number of shards.
func (c *coordinatorClient) InitTopic(ctx context.Context, topicName string, numShards int) error {
	body := map[string]any{"numberOfShards": numShards}
	return c.makeReq(ctx, http.MethodPost, c.BaseURL+"/api/v1/topic/init", body, map[string]string{
		"Topic-Name":               topicName,
		"Cluster-Environment-Name": c.Env,
	})
}

// AddConsumer adds a consumer service to a topic. Idempotent 409 is treated as success.
func (c *coordinatorClient) AddConsumer(ctx context.Context, topicName string, serviceID ServiceID, consumptionType string, messageTTLNanos int64) error {
	body := map[string]any{
		"consumerService": map[string]any{
			"serviceId": map[string]any{
				"name":        serviceID.Name,
				"environment": serviceID.Environment,
				"zone":        serviceID.Zone,
			},
			"consumptionType": consumptionType,
			"messageTtlNanos": messageTTLNanos,
		},
	}
    if err := c.makeReq(ctx, http.MethodPost, c.BaseURL+"/api/v1/topic", body, map[string]string{
		"Topic-Name":               topicName,
		"Cluster-Environment-Name": c.Env,
    }); err != nil {
        // Treat already-consuming error as success (idempotent)
        if strings.Contains(err.Error(), "already consuming the topic") {
            return nil
        }
        return err
    }
    return nil
}

func (c *coordinatorClient) makeReq(ctx context.Context, method, url string, body any, headers map[string]string) error {
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("request %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode != http.StatusConflict {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s failed: status=%d body=%s", method, url, resp.StatusCode, string(data))
	}
	return nil
}

type kvClient struct {
	csClient       client.Client
	servicesClient services.Services
	env            string
	zone           string
}

func NewKVClient(env string, zone string, endpoints []string) *kvClient {
	etcdClientCfg := etcdcfg.Configuration{
		Zone:    zone,
		Env:     env,
		Service: "local-consumer-service-bootstrap",
		ETCDClusters: []etcdcfg.ClusterConfig{
			{Zone: zone, Endpoints: endpoints},
		},
	}
	csClient, err := etcdClientCfg.NewClient(instrument.NewOptions())
	if err != nil {
		fatalf("create etcd config service client: %v", err)
	}
	servicesClient, err := csClient.Services(nil)
	if err != nil {
		fatalf("create services client: %v", err)
	}
	return &kvClient{csClient: csClient, servicesClient: servicesClient, env: env, zone: zone}
}

func (c *kvClient) SetPlacement(p PlacementConfig) error {
	sid := services.NewServiceID().
		SetName(p.Service).
		SetEnvironment(c.env).
		SetZone(c.zone)

	popts := placement.NewOptions().SetValidZone(c.zone)

	psvc, err := c.servicesClient.PlacementService(sid, popts)
	if err != nil {
		return fmt.Errorf("placement service for %s: %v", p.Service, err)
	}

	// If a placement already exists, treat as success (idempotent bootstrap)
	if _, err := psvc.Placement(); err == nil {
		return nil
	} else if !errors.Is(err, kv.ErrNotFound) {
		return fmt.Errorf("get existing placement: %v", err)
	}

	// Convert instances
	insts := make([]placement.Instance, 0, len(p.Instances))
	for _, in := range p.Instances {
		pi, err := placement.NewInstance().
			SetID(in.ID).
			SetIsolationGroup(in.IsolationGroup).
			SetZone(in.Zone).
			SetWeight(uint32(in.Weight)).
			SetEndpoint(in.Endpoint).
			SetHostname(in.Hostname).
			SetPort(uint32(in.Port)).Proto()
		if err != nil {
			return fmt.Errorf("build instance proto %s: %v", in.ID, err)
		}
		// Convert back to placement.Instance from proto to ensure consistency
		inst, err := placement.NewInstanceFromProto(pi)
		if err != nil {
			return fmt.Errorf("build instance %s: %v", in.ID, err)
		}
		insts = append(insts, inst)
	}

	_, err = psvc.BuildInitialPlacement(insts, p.NumShards, p.ReplicationFactor)
	if err != nil {
		return err
	}
	return nil
}

func (c *kvClient) SetRoutingPolicy(rp RoutingPolicyConfig) error {
	// Create the routing policy protobuf message
	policyProto := &routingpolicypb.RoutingPolicyConfig{
		TrafficTypes: rp.TrafficTypes,
	}

	// Get or create the store with the specified namespace
	kvOpts := kv.NewOverrideOptions().
		SetEnvironment(c.env).
		SetZone(c.zone).
		SetNamespace(rp.KVNamespace)

	store, err := c.csClient.Store(kvOpts)
	if err != nil {
		return fmt.Errorf("get kv store for routing policy: %v", err)
	}

	// Set the value in etcd
	_, err = store.Set(rp.KVKey, policyProto)
	if err != nil {
		return fmt.Errorf("set routing policy in kv: %v", err)
	}

	fmt.Printf("✓ Routing policy set at %s/%s with traffic types: %v\n",
		rp.KVNamespace, rp.KVKey, rp.TrafficTypes)
	return nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
