/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package runner

import (
	"context"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"io"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/pkg/errors"
	"github.com/tedsuo/ifrit"
)

const (
	CouchbaseDefaultImage = "couchbase:latest"
	CouchbaseUsername     = "Administrator"
	CouchbasePassword     = "Password@123"
)

// Couchbase manages the execution of an instance of a dockerized CounchDB
// for tests.
type Couchbase struct {
	Client        *docker.Client
	Image         string
	HostIP        string
	HostPort      int
	ContainerPort docker.Port
	Name          string
	StartTimeout  time.Duration
	Binds         []string

	ErrorStream  io.Writer
	OutputStream io.Writer

	creator          string
	containerID      string
	hostAddress      string
	containerAddress string
	address          string

	mutex   sync.Mutex
	stopped bool
}

func setupCluster(containerName, connectionString, bucketName string) error {
	// Initialize the cluster
	clusterInitCmd := exec.Command("docker", "exec", "-i", containerName,
		"couchbase-cli", "cluster-init",
		"--cluster", connectionString,
		"--cluster-username", CouchbaseUsername,
		"--cluster-password", CouchbasePassword,
		"--cluster-ramsize", "512",
		"--cluster-index-ramsize", "512",
		"--cluster-fts-ramsize", "512",
		"--services", "data,index,query,fts",
	)
	clusterInitCmd.Stdout = os.Stdout
	clusterInitCmd.Stderr = os.Stderr

	if err := clusterInitCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing Couchbase cluster: %v\n", err)
		// Optionally: return err to fail fast
	}

	// Create the bucket
	bucketCreateCmd := exec.Command("docker", "exec", "-i", containerName,
		"couchbase-cli", "bucket-create",
		"-c", connectionString,
		"--username", CouchbaseUsername,
		"--password", CouchbasePassword,
		"--bucket", bucketName,
		"--bucket-type", "couchbase",
		"--bucket-ramsize", "200",
	)
	bucketCreateCmd.Stdout = os.Stdout
	bucketCreateCmd.Stderr = os.Stderr

	if err := bucketCreateCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating Couchbase bucket: %v\n", err)
		// Optionally: return err to fail fast
	}

	// create the scope
	//scopeCreateCmd := exec.Command("docker", "exec", "-i", containerName,
	//	"couchbase-cli", "collection-manage",
	//	"-c", connectionString,
	//	"--username", username,
	//	"--password", password,
	//	"--bucket", bucketName,
	//	"--create-scope", "fabric-1",
	//	"--bucket-ramsize", "200",
	//)
	//
	//scopeCreateCmd.Stdout = os.Stdout
	//scopeCreateCmd.Stderr = os.Stderr
	//
	//if err := scopeCreateCmd.Run(); err != nil {
	//	fmt.Fprintf(os.Stderr, "Error creating Couchbase scope: %v\n", err)
	//	// Optionally: return err to fail fast
	//}

	// Wait for 10 seconds
	time.Sleep(10 * time.Second)
	return nil
}

// Run runs a Couchbase container. It implements the ifrit.Runner interface
func (c *Couchbase) Run(sigCh <-chan os.Signal, ready chan<- struct{}) error {
	if c.Image == "" {
		c.Image = CouchbaseDefaultImage
	}

	if c.Name == "" {
		c.Name = DefaultNamer()
	}

	if c.HostIP == "" {
		c.HostIP = "127.0.0.1"
	}

	if c.StartTimeout == 0 {
		c.StartTimeout = DefaultStartTimeout
	}

	if c.Client == nil {
		client, err := docker.NewClientFromEnv()
		if err != nil {
			return err
		}
		c.Client = client
	}

	hostPortBindings := map[docker.Port][]docker.PortBinding{
		"8091/tcp":  {{HostIP: c.HostIP, HostPort: "8091"}},
		"8092/tcp":  {{HostIP: c.HostIP, HostPort: "8092"}},
		"8093/tcp":  {{HostIP: c.HostIP, HostPort: "8093"}},
		"8094/tcp":  {{HostIP: c.HostIP, HostPort: "8094"}},
		"8095/tcp":  {{HostIP: c.HostIP, HostPort: "8095"}},
		"8096/tcp":  {{HostIP: c.HostIP, HostPort: "8096"}},
		"11210/tcp": {{HostIP: c.HostIP, HostPort: "11210"}},
		"11211/tcp": {{HostIP: c.HostIP, HostPort: "11211"}},
	}

	hostConfig := &docker.HostConfig{
		AutoRemove:   true,
		PortBindings: hostPortBindings,
		Binds:        c.Binds,
	}

	container, err := c.Client.CreateContainer(
		docker.CreateContainerOptions{
			Name: c.Name,
			Config: &docker.Config{
				Image: c.Image,
				Env: []string{
					fmt.Sprintf("_creator=%s", c.creator),
				},
			},
			HostConfig: hostConfig,
		},
	)
	if err != nil {
		return err
	}
	c.containerID = container.ID

	err = c.Client.StartContainer(container.ID, nil)
	if err != nil {
		return err
	}
	defer c.Stop()

	container, err = c.Client.InspectContainerWithOptions(docker.InspectContainerOptions{ID: container.ID})
	if err != nil {
		return err
	}
	fmt.Printf("Couchbase container started with host IP %+v\n", container.NetworkSettings.Ports)
	fmt.Printf("c.ContainerPort: %+v\n", c.HostIP)

	c.hostAddress = c.HostIP
	c.containerAddress = container.NetworkSettings.IPAddress

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	go c.streamLogs(streamCtx)

	containerExit := c.wait()
	ctx, cancel := context.WithTimeout(context.Background(), c.StartTimeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return errors.Wrapf(ctx.Err(), "database in container %s did not start", c.containerID)
	case <-containerExit:
		return errors.New("container exited before ready")
	case <-c.ready(ctx, c.hostAddress):
		c.address = c.hostAddress
	case <-c.ready(ctx, c.containerAddress):
		c.address = c.containerAddress
	}

	time.Sleep(10 * time.Second)
	err = setupCluster(c.containerID, c.hostAddress, "fabric-1")

	cancel()
	close(ready)

	for {
		select {
		case err := <-containerExit:
			return err
		case <-sigCh:
			if err := c.Stop(); err != nil {
				return err
			}
		}
	}
}

func endpointReadyCouchbase(ctx context.Context, url string) bool {
	return true
}

func (c *Couchbase) ready(ctx context.Context, addr string) <-chan struct{} {
	readyCh := make(chan struct{})
	go func() {
		url := fmt.Sprintf("http://%s:%s@%s/", CouchbaseUsername, CouchbasePassword, addr)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			if endpointReadyCouchbase(ctx, url) {
				close(readyCh)
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	return readyCh
}

func (c *Couchbase) wait() <-chan error {
	exitCh := make(chan error)
	go func() {
		exitCode, err := c.Client.WaitContainer(c.containerID)
		if err == nil {
			err = fmt.Errorf("couchbase: process exited with %d", exitCode)
		}
		exitCh <- err
	}()

	return exitCh
}

func (c *Couchbase) streamLogs(ctx context.Context) {
	if c.ErrorStream == nil && c.OutputStream == nil {
		return
	}

	logOptions := docker.LogsOptions{
		Context:      ctx,
		Container:    c.containerID,
		Follow:       true,
		ErrorStream:  c.ErrorStream,
		OutputStream: c.OutputStream,
		Stderr:       c.ErrorStream != nil,
		Stdout:       c.OutputStream != nil,
	}

	err := c.Client.Logs(logOptions)
	if err != nil {
		fmt.Fprintf(c.ErrorStream, "log stream ended with error: %s", err)
	}
}

// Address returns the address successfully used by the readiness check.
func (c *Couchbase) Address() string {
	return c.address
}

// HostAddress returns the host address where this Couchbase instance is available.
func (c *Couchbase) HostAddress() string {
	return fmt.Sprintf("couchbase://%s", c.hostAddress)
}

// ContainerAddress returns the container address where this Couchbase instance
// is available.
func (c *Couchbase) ContainerAddress() string {
	return c.containerAddress
}

// ContainerID returns the container ID of this Couchbase
func (c *Couchbase) ContainerID() string {
	return c.containerID
}

// Start starts the Couchbase container using an ifrit runner
func (c *Couchbase) Start() error {
	c.creator = string(debug.Stack())
	p := ifrit.Invoke(c)

	select {
	case <-p.Ready():
		return nil
	case err := <-p.Wait():
		return err
	}
}

// Stop stops and removes the Couchbase container
func (c *Couchbase) Stop() error {
	c.mutex.Lock()
	if c.stopped {
		c.mutex.Unlock()
		return errors.Errorf("container %s already stopped", c.containerID)
	}
	c.stopped = true
	c.mutex.Unlock()

	err := c.Client.StopContainer(c.containerID, 0)
	if err != nil {
		return err
	}

	return nil
}

func getAllDatabases(bucket gocb.Bucket) []string {
	var allCollections []string
	scopes, err := bucket.CollectionsV2().GetAllScopes(nil)
	if err != nil {
		return nil
	}
	for _, scope := range scopes {
		for _, collection := range scope.Collections {
			allCollections = append(allCollections, collection.Name)
		}
	}
	return allCollections
}

func deleteAllDatabases(bucket gocb.Bucket) {
	collections := getAllDatabases(bucket)
	for _, collection := range collections {
		if strings.HasPrefix(collection, "_") {
			fmt.Printf("Skipping collection %s\n", collection)
			continue
		}
		fmt.Printf("Deleting collection %s\n", collection)
		err := bucket.CollectionsV2().DropCollection("_default", collection, nil)
		if err != nil {
			fmt.Printf("Error deleting collection %s: %s\n", collection, err)
			return
		}
	}

	return
}

func (c *Couchbase) CleanupCluster() {
	options := gocb.ClusterOptions{
		SecurityConfig: gocb.SecurityConfig{
			TLSSkipVerify: true,
		},
		Authenticator: gocb.PasswordAuthenticator{
			Username: CouchbaseUsername,
			Password: CouchbasePassword,
		},
	}
	cluster, err := gocb.Connect("couchbases://cb.ytjj89q8f6afda87.customsubdomain.nonprod-project-avengers.com", options)
	if err != nil {
		fmt.Println(err)
	}

	bucket := cluster.Bucket("fabric-1")
	err = bucket.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		fmt.Printf("Failed to connect to %s\n", err)
		return
	}

	deleteAllDatabases(*bucket)
	time.Sleep(5 * time.Second)
}
