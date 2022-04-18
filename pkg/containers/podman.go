package containers

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/csv"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"apiserver-benchmarking/pkg/cadvisor"
	"apiserver-benchmarking/pkg/pprof"
	"github.com/sirupsen/logrus"

	kerrors "k8s.io/apimachinery/pkg/util/errors"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/utils/pointer"

	"github.com/containers/podman/v3/libpod/define"
	"github.com/containers/podman/v3/libpod/network/types"
	"github.com/containers/podman/v3/pkg/bindings"
	"github.com/containers/podman/v3/pkg/bindings/containers"
	"github.com/containers/podman/v3/pkg/bindings/images"
	"github.com/containers/podman/v3/pkg/bindings/pods"
	"github.com/containers/podman/v3/pkg/domain/entities"
	"github.com/containers/podman/v3/pkg/specgen"

	"apiserver-benchmarking/pkg/config"
)

func NewManager(ctx context.Context, logger *logrus.Entry) (*manager, error) {
	sock_dir := os.Getenv("XDG_RUNTIME_DIR")
	if sock_dir == "" {
		sock_dir = "/var/run" // TODO: why?
	}
	socket := "unix:" + sock_dir + "/podman/podman.sock"

	ctx, err := bindings.NewConnection(ctx, socket)
	if err != nil {
		return nil, fmt.Errorf("could not create podman connection: %w", err)
	}

	return &manager{
		client:      ctx,
		logger:      logger,
		wg:          &sync.WaitGroup{},
		mappedPorts: map[int32]int32{},
	}, nil
}

type manager struct {
	logger *logrus.Entry
	client context.Context

	wg *sync.WaitGroup

	podId       string
	mappedPorts map[int32]int32

	tempDirs         []string
	cancelMonitoring func()
}

const (
	podName = "benchmark"

	apiServerImage = "k8s.gcr.io/kube-apiserver-amd64"
	crdbImage      = "docker.io/cockroachdb/cockroach:v21.2.8"
	etcdImage      = "quay.io/coreos/etcd:v3.5.1"

	adminToken  = "admin-token"
	adminName   = "admin"
	adminUid    = "f4bf6eb5-aa2f-45db-acde-1c217748b22c"
	adminGroups = `system:masters`

	crdbConsolePort   uint16 = 8080
	databasePprofPort uint16 = 2379
	apiServerPort     uint16 = 6443

	cadvisorHostPort uint16 = 8080
)

func (m *manager) Deploy(cfg *config.Setup) error {
	if cfg.ReloadAPIServerImageFrom != "" {
		m.logger.Info("Removing previous API server images.")
		imageSummary, err := images.List(m.client, &images.ListOptions{
			Filters: map[string][]string{
				"reference": {apiServerImage},
			},
		})
		if err != nil {
			return fmt.Errorf("could not list images: %w", err)
		}
		for _, img := range imageSummary {
			report, err := images.Remove(m.client, []string{img.ID}, &images.RemoveOptions{})
			if err != nil {
				return fmt.Errorf("failed to remove old API server image: %w", err)
			}
			if report.ExitCode != 0 {
				return fmt.Errorf("failed to remove old API server image: exit code %d", report.ExitCode)
			}
			m.logger.WithFields(logrus.Fields{
				"deleted":  report.Deleted,
				"untagged": report.Untagged,
			}).Trace("Removed images.")
		}

		m.logger.Info("Reloading API server image.")
		file, err := os.Open(cfg.ReloadAPIServerImageFrom)
		if err != nil {
			return fmt.Errorf("could not open API server image tarball: %w", err)
		}
		defer func() {
			if err := file.Close(); err != nil && !errors.Is(err, fs.ErrClosed) {
				m.logger.WithError(err).Warn("could not close API server image tarball")
			}
		}()
		img, err := images.Load(m.client, file)
		if err != nil {
			return fmt.Errorf("could not load API server image tarball: %w", err)
		}
		m.logger.WithField("names", img.Names).Trace("Loaded API server image.")
	}

	apiServerImageID, err := m.resolveImage(apiServerImage)
	if err != nil {
		return err
	}
	m.logger.WithField("id", apiServerImageID).Info("Resolved API server image.")

	m.logger.Info("Starting benchmark pod.")
	podSpec := specgen.NewPodSpecGenerator()
	podSpec.Name = podName
	podSpec.PortMappings = []types.PortMapping{
		{ContainerPort: crdbConsolePort},
		{ContainerPort: databasePprofPort},
		{ContainerPort: apiServerPort},
	}
	if err := podSpec.Validate(); err != nil {
		return fmt.Errorf("invalid pod spec: %w", err)
	}

	podCreateReport, err := pods.CreatePodFromSpec(m.client, &entities.PodSpec{PodSpecGen: *podSpec})
	if err != nil {
		return fmt.Errorf("could not create pod: %w", err)
	}
	m.podId = podCreateReport.Id

	podStartReport, err := pods.Start(m.client, podCreateReport.Id, &pods.StartOptions{})
	if err != nil {
		return fmt.Errorf("could not start pod: %w", err)
	}
	if podStartReport.Errs != nil {
		return fmt.Errorf("could not start pod: %v", podStartReport.Errs)
	}

	monitoringCtx, monitoringCancel := context.WithCancel(m.client)
	m.cancelMonitoring = monitoringCancel

	m.logger.Info("Starting database.")
	var dbStartErr error
	var dbServers string
	if cfg.StorageBackend == "crdb" {
		if cfg.SingleNodeStorageTopology {
			dbServers, dbStartErr = m.deploySingleNodeCRDB(monitoringCtx, cfg.OutputDir)
		} else {
			dbServers, dbStartErr = m.deployMultiNodeCRDB(monitoringCtx, cfg.OutputDir)
		}
	} else {
		if cfg.SingleNodeStorageTopology {
			dbServers, dbStartErr = m.deploySingleNodeETCD(monitoringCtx, cfg.OutputDir)
		} else {
			dbServers, dbStartErr = m.deployMultiNodeETCD(monitoringCtx, cfg.OutputDir)
		}
	}
	if dbStartErr != nil {
		return fmt.Errorf("could not deploy database: %w", dbStartErr)
	}

	logrus.Info("Writing a temporary service account signing key.")
	keyDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("could not create temp key dir: %w", err)
	}
	m.tempDirs = append(m.tempDirs, keyDir)

	const rsaKeySize = 2048
	const RSAPrivateKeyBlockType = "RSA PRIVATE KEY"
	const PublicKeyBlockType = "PUBLIC KEY"
	key, err := rsa.GenerateKey(cryptorand.Reader, rsaKeySize)
	if err != nil {
		return fmt.Errorf("failed to create key: %w", err)
	}
	der, err := x509.MarshalPKIXPublicKey(key.Public())
	if err != nil {
		return fmt.Errorf("could not marshal public key: %w", err)
	}
	for file, block := range map[string]*pem.Block{
		"sa.key": {
			Type:  RSAPrivateKeyBlockType,
			Bytes: x509.MarshalPKCS1PrivateKey(key),
		},
		"sa.pub": {
			Type:  PublicKeyBlockType,
			Bytes: der,
		},
	} {
		keyFile, err := os.Create(filepath.Join(keyDir, file))
		if err != nil {
			return fmt.Errorf("failed to create key file: %w", err)
		}
		keyClose := func() {
			if err := keyFile.Close(); err != nil {
				m.logger.WithError(err).Warn("could not close key file")
			}
		}
		if err := pem.Encode(keyFile, block); err != nil {
			keyClose()
			return fmt.Errorf("failed to write key: %w", err)
		}
		keyClose()
	}

	logrus.Info("Writing a temporary token auth file.")
	authDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("could not create temp auth dir: %w", err)
	}
	m.tempDirs = append(m.tempDirs, authDir)

	authFile, err := os.Create(filepath.Join(authDir, "auth-tokens.csv"))
	if err != nil {
		return fmt.Errorf("could not create temp auth CSV file: %w", err)
	}
	authClose := func() {
		if err := authFile.Close(); err != nil {
			m.logger.WithError(err).Warn("could not close temp auth CSV file")
		}
	}
	writer := csv.NewWriter(authFile)
	if err := writer.Write([]string{adminToken, adminName, adminUid, adminGroups}); err != nil {
		authClose()
		return fmt.Errorf("could not write temp auth data: %w", err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		authClose()
		return fmt.Errorf("error writing temp auth data: %w", err)
	}
	authClose()

	m.logger.Info("Starting API server.")
	s := specgen.NewSpecGenerator(apiServerImageID, false)
	s.Name = "apiserver"
	s.Pod = podName
	s.OverlayVolumes = []*specgen.OverlayVolume{
		{
			Source:      keyDir,
			Destination: "/var/lib/kubernetes",
			Options:     []string{"z"},
		},
		{
			Source:      authDir,
			Destination: "/tokens",
			Options:     []string{"z"},
		},
	}
	s.Command = []string{
		"kube-apiserver",
		"--service-account-key-file", "/var/lib/kubernetes/sa.pub",
		"--service-account-signing-key-file", "/var/lib/kubernetes/sa.key",
		"--service-account-issuer", "kubernetes.default.svc.cluster.local",
		"--token-auth-file", "/tokens/auth-tokens.csv",
		"--storage-backend", cfg.StorageBackend,
		"--etcd-servers", dbServers,
	}
	if cfg.WatchCacheEnabled {
		s.Command = append(s.Command, "--watch-cache")
	}
	if err := m.startContainer("apiserver", s); err != nil {
		return err
	}

	if err := m.resolvePortMappings(); err != nil {
		return err
	}

	port, found := m.mappedPorts[int32(apiServerPort)]
	if !found {
		return fmt.Errorf("no host-mapped port found for %d", apiServerPort)
	}
	m.monitorAPIServerContainer(monitoringCtx, port, s.Name, cfg.OutputDir)

	return nil
}

func (m *manager) monitorAPIServerContainer(ctx context.Context, port int32, name, outputDir string) {
	header := map[string]string{"Authorization": "Bearer " + adminToken}
	m.monitor(func() {
		if err := pprof.Fetch(ctx, m.logger, fmt.Sprintf("https://0.0.0.0:%d", port), outputDir, name, "heap", 5*time.Second, nil, header); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.WithError(err).Warnf("Failed to monitor pprof heap for %s.", name)
		}
	})
	m.monitor(func() {
		if err := pprof.Fetch(ctx, m.logger, fmt.Sprintf("https://0.0.0.0:%d", port), outputDir, name, "profile", 30*time.Second, map[string]string{"seconds": "30"}, header); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.WithError(err).Warnf("Failed to monitor pprof profile for %s.", name)
		}
	})
	m.monitor(func() {
		if err := cadvisor.Fetch(ctx, m.logger, fmt.Sprintf("http://0.0.0.0:%d", cadvisorHostPort), outputDir, name, 30*time.Second); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.WithError(err).Warnf("Failed to monitor cadvisor for %s.", name)
		}
	})
}

func (m *manager) resolveImage(reference string) (string, error) {
	imageSummary, err := images.List(m.client, &images.ListOptions{
		Filters: map[string][]string{
			"reference": {reference},
		},
	})
	if err != nil {
		return "", fmt.Errorf("could not list images: %w", err)
	}
	switch len(imageSummary) {
	case 0:
		return "", fmt.Errorf("no images found for reference=%q", reference)
	case 1:
		break // this is expected
	default:
		var names []string
		for _, img := range imageSummary {
			names = append(names, img.Names...)
		}
		return "", fmt.Errorf("ambiguous image reference=%q, found %d matching images: %s", reference, len(imageSummary), strings.Join(names, ", "))
	}
	return imageSummary[0].ID, nil
}

func (m *manager) deploySingleNodeCRDB(ctx context.Context, outputDir string) (string, error) {
	crdbImageId, err := m.resolveImage(crdbImage)
	if err != nil {
		return "", err
	}

	s := specgen.NewSpecGenerator(crdbImageId, false)
	s.Name = "database"
	s.Pod = podName
	s.Labels = map[string]string{}
	s.Command = []string{
		"start-single-node",
		"--sql-addr", "0.0.0.0:2379",
		"--advertise-sql-addr", "0.0.0.0:2379",
		"--insecure",
	}
	if err := m.startContainer("crdb", s); err != nil {
		return "", err
	}

	m.monitorDatabaseContainer(ctx, s.Name, outputDir)

	return crdbConnectionUrl(2379), nil
}

func crdbConnectionUrl(port int) string {
	u := url.URL{
		Scheme: "postgresql",
		User:   url.User("root"),
		Host:   fmt.Sprintf("localhost:%d", port),
		Path:   "defaultdb",
	}
	return u.String()
}

// monitorDatabaseContainerPprof is broken right now - something to do with the podman host->local network port publishing
// breaks these connections ... ?
func (m *manager) monitorDatabaseContainerPprof(ctx context.Context, port int32, name, outputDir string) {
	m.monitor(func() {
		if err := pprof.Fetch(ctx, m.logger, fmt.Sprintf("http://0.0.0.0:%d", port), outputDir, name, "heap", 5*time.Second, nil, nil); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.WithError(err).Warnf("Failed to monitor pprof heap for %s.", name)
		}
	})
	m.monitor(func() {
		if err := pprof.Fetch(ctx, m.logger, fmt.Sprintf("http://0.0.0.0:%d", port), outputDir, name, "profile", 30*time.Second, map[string]string{"seconds": "30"}, nil); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.WithError(err).Warnf("Failed to monitor pprof profile for %s.", name)
		}
	})
}

func (m *manager) monitorDatabaseContainer(ctx context.Context, name, outputDir string) {
	m.monitor(func() {
		if err := cadvisor.Fetch(ctx, m.logger, fmt.Sprintf("http://0.0.0.0:%d", cadvisorHostPort), outputDir, name, 30*time.Second); err != nil && !errors.Is(err, context.Canceled) {
			m.logger.WithError(err).Warnf("Failed to monitor cadvisor for %s.", name)
		}
	})
}

func (m *manager) monitor(f func()) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		f()
	}()
}

func (m *manager) deployMultiNodeCRDB(ctx context.Context, outputDir string) (string, error) {
	members := map[string]int{
		"crdb0": 0,
		"crdb1": 1,
		"crdb2": 2,
	}
	const clusterName = "crdb-cluster"
	const clientPortBase = 26257
	const httpPortBase = 8080
	var join []string
	var ports []int
	for _, idx := range members {
		clientPort := clientPortBase+idx
		join = append(join, fmt.Sprintf("localhost:%d", clientPort))
		ports = append(ports, clientPort)
	}
	for name, idx := range members {
		clientUrl := fmt.Sprintf("localhost:%d", clientPortBase+idx)
		httpUrl := fmt.Sprintf("localhost:%d", httpPortBase+idx)
		s := specgen.NewSpecGenerator(crdbImage, false)
		s.Name = fmt.Sprintf("database%d", idx)
		s.Pod = podName
		s.Command = []string{
			"start",
			"--store", name,
			"--listen-addr", clientUrl,
			"--http-addr", httpUrl,
			"--cluster-name", clusterName,
			"--join", strings.Join(join, ","),
			"--insecure",
		}
		if err := m.startContainer("crdb", s); err != nil {
			return "", err
		}
		m.monitorDatabaseContainer(ctx, s.Name, outputDir)
	}

	s := specgen.NewSpecGenerator(crdbImage, false)
	s.Name = "database-init"
	s.Pod = podName
	s.Command = []string{
		"init",
		"--host", join[0],
		"--cluster-name", clusterName,
		"--insecure",
	}
	if err := m.startContainer("database-init", s); err != nil {
		return "", err
	}
	exitCode, err := containers.Wait(m.client, "database-init", &containers.WaitOptions{
		Condition: []define.ContainerStatus{define.ContainerStateStopped},
	})
	if err != nil {
		return "", fmt.Errorf("%s container did not become running: %w", "crdb-init", err)
	}
	if exitCode != 0 {
		return "", errors.New("crdb init did not finish with a 0 exit code")
	}
	var connections []string
	for _, port := range ports {
		connections = append(connections, crdbConnectionUrl(port))
	}
	return strings.Join(connections, ","), nil
}

func (m *manager) deploySingleNodeETCD(ctx context.Context, outputDir string) (string, error) {
	s := specgen.NewSpecGenerator(etcdImage, false)
	s.Name = "database"
	s.Pod = podName
	s.Command = []string{
		"etcd",
		"--enable-pprof",
		"--quota-backend-bytes", "6710886400",
	}
	if err := m.startContainer("etcd", s); err != nil {
		return "", err
	}
	m.monitorDatabaseContainer(ctx, s.Name, outputDir)
	return "localhost:2379", nil
}

func (m *manager) startContainer(name string, spec *specgen.SpecGenerator) error {
	container, err := containers.CreateWithSpec(m.client, spec, &containers.CreateOptions{})
	if err != nil {
		return fmt.Errorf("could not create %s container: %w", name, err)
	}

	err = containers.Start(m.client, container.ID, &containers.StartOptions{})
	if err != nil {
		return fmt.Errorf("could not start %s container: %w", name, err)
	}

	_, err = containers.Wait(m.client, container.ID, &containers.WaitOptions{
		Condition: []define.ContainerStatus{define.ContainerStateRunning},
	})
	if err != nil {
		return fmt.Errorf("%s container did not become running: %w", name, err)
	}
	m.logger.WithField("id", container.ID).Infof("Started %s container.", name)
	return nil
}

func (m *manager) deployMultiNodeETCD(ctx context.Context, outputDir string) (string, error) {
	members := map[string]int{
		"etcd0": 0,
		"etcd1": 1,
		"etcd2": 2,
	}
	const clusterToken = "etcd-cluster"
	const clientPortBase = 2370
	const peerPortBase = 2380
	var initialCluster []string
	for name, idx := range members {
		initialCluster = append(initialCluster, fmt.Sprintf("%s=http://127.0.0.1:%d", name, peerPortBase+idx))
	}
	var clientUrls []string
	for name, idx := range members {
		clientUrl := fmt.Sprintf("http://127.0.0.1:%d", clientPortBase+idx)
		clientUrls = append(clientUrls, clientUrl)
		peerUrl := fmt.Sprintf("http://127.0.0.1:%d", peerPortBase+idx)
		s := specgen.NewSpecGenerator(etcdImage, false)
		s.Name = fmt.Sprintf("database%d", idx)
		s.Pod = podName
		s.Command = []string{
			"etcd",
			"--name", name,
			"--listen-client-urls", clientUrl,
			"--advertise-client-urls", clientUrl,
			"--listen-peer-urls", peerUrl,
			"--initial-advertise-peer-urls", peerUrl,
			"--initial-cluster-token", clusterToken,
			"--initial-cluster", strings.Join(initialCluster, ","),
			"--initial-cluster-state", "new",
			"--enable-pprof",
			"--quota-backend-bytes", "6710886400",
		}
		if err := m.startContainer("etcd", s); err != nil {
			return "", err
		}
		m.monitorDatabaseContainer(ctx, s.Name, outputDir)
	}
	return strings.Join(clientUrls, ","), nil
}

func (m *manager) resolvePortMappings() error {
	ctrs, err := containers.List(m.client, &containers.ListOptions{
		Filters: map[string][]string{
			"pod": {podName},
		},
	})
	if err != nil {
		return fmt.Errorf("could not list containers: %w", err)
	}

	for _, container := range ctrs {
		for _, mapping := range container.Ports {
			m.mappedPorts[mapping.ContainerPort] = mapping.HostPort
		}
	}
	return nil
}

func (m *manager) APIServerClient() (*clientset.Clientset, error) {
	if len(m.mappedPorts) == 0 {
		if err := m.resolvePortMappings(); err != nil {
			return nil, err
		}
	}

	mappedPort, found := m.mappedPorts[int32(apiServerPort)]
	if !found {
		return nil, fmt.Errorf("could not find any container exposing port %d", apiServerPort)
	}

	apiserverUrl := url.URL{
		Scheme: "https",
		Host:   fmt.Sprintf("localhost:%d", mappedPort),
	}
	clientSet, err := clientset.NewForConfig(&restclient.Config{
		Host:        apiserverUrl.String(),
		BearerToken: adminToken,
		TLSClientConfig: restclient.TLSClientConfig{
			Insecure: true,
		},
		QPS: -1,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create client: %w", err)
	}
	return clientSet, nil
}

func (m *manager) Cleanup(outputDir string) error {
	m.logger.Info("Cancelling monitoring routines.")
	if m.cancelMonitoring != nil {
		m.cancelMonitoring()
	}
	m.wg.Wait()
	containerErr := m.cleanupContainers(outputDir)
	filesErr := m.cleanupFiles()
	return kerrors.NewAggregate(append(filesErr, containerErr))
}

func (m *manager) cleanupContainers(outputDir string) error {
	if m.podId == "" {
		return nil
	}

	m.logger.Info("Cleaning up containers.")
	pod, err := pods.Inspect(m.client, m.podId, &pods.InspectOptions{})
	if err != nil {
		return fmt.Errorf("failed to inspect pod: %w", err)
	}
	wg := sync.WaitGroup{}
	for _, container := range pod.Containers {
		logger := m.logger.WithFields(logrus.Fields{"container": container.Name, "id": container.ID})
		logChan := make(chan string)
		wg.Add(1)
		go func(output chan<- string, name, id string, logger *logrus.Entry) {
			defer wg.Done()
			defer close(output)
			if err := containers.Logs(m.client, container.ID, &containers.LogOptions{
				Stderr:     pointer.Bool(true),
				Stdout:     pointer.Bool(true),
				Timestamps: pointer.Bool(true),
				Until:      pointer.String(time.Now().Format(time.RFC3339)),
			}, logChan, logChan); err != nil {
				logger.WithError(err).Warn("Could not get logs.")
			}
		}(logChan, container.Name, container.ID, logger)
		logFile := filepath.Join(outputDir, container.Name+".log")
		logger.WithField("log", logFile).Info("Writing container logs.")
		output, err := os.Create(logFile)
		if err != nil {
			logger.WithError(err).Warn("Could not create log file.")
			continue
		}

		wg.Add(1)
		go func(input <-chan string, output io.WriteCloser, name, id string, logger *logrus.Entry) {
			defer wg.Done()
			for line := range input {
				if _, err := output.Write([]byte(line + "\n")); err != nil {
					logger.WithError(err).Warn("Could not write to log file.")
					break
				}
			}
			if err := output.Close(); err != nil {
				logger.WithError(err).Warn("Could not close log file.")
			}
			logger.Info("Collected container logs.")
			if err := containers.Stop(m.client, id, &containers.StopOptions{}); err != nil {
				logger.WithError(err).Warn("Could not stop container.")
			}
			logger.Info("Stopped container.")
			if err := containers.Remove(m.client, id, &containers.RemoveOptions{}); err != nil {
				logger.WithError(err).Warn("Could not remove container.")
			}
			logger.Info("Removed container.")
		}(logChan, output, container.Name, container.ID, logger)
	}
	wg.Wait()

	m.logger.Info("Stopping pod.")
	if _, err := pods.Stop(m.client, m.podId, &pods.StopOptions{Timeout: pointer.Int(10)}); err != nil {
		return fmt.Errorf("failed to stop pod: %w", err)
	}

	m.logger.Info("Removing pod.")
	if _, err := pods.Remove(m.client, m.podId, &pods.RemoveOptions{Force: pointer.Bool(true)}); err != nil {
		return fmt.Errorf("failed to remove pod: %w", err)
	}

	m.logger.Info("Finished cleaning up.")
	return nil
}

func (m *manager) cleanupFiles() []error {
	var errs []error
	for _, dir := range m.tempDirs {
		errs = append(errs, os.RemoveAll(dir))
	}
	return errs
}
