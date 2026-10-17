package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/godis/lib/utils"

	"github.com/hdt3213/godis/lib/logger"
)

var (
	ClusterMode    = "cluster"
	StandaloneMode = "standalone"
)

// ServerProperties defines global config properties
type ServerProperties struct {
	// for Public configuration
	RunID             string `cfg:"runid"` // runID always different at every exec.
	Bind              string `cfg:"bind"`
	Port              int    `cfg:"port"`
	Dir               string `cfg:"dir"`
	AnnounceHost      string `cfg:"announce-host"`
	AppendOnly        bool   `cfg:"appendonly"`
	AppendFilename    string `cfg:"appendfilename"`
	AppendFsync       string `cfg:"appendfsync"`
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble"`
	MaxClients        int    `cfg:"maxclients"`
	RequirePass       string `cfg:"requirepass"`
	Databases         int    `cfg:"databases"`
	RDBFilename       string `cfg:"dbfilename"`
	MasterAuth        string `cfg:"masterauth"`
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`
	UseGnet           bool   `cfg:"use-gnet"`

	SlowLogSlowerThan int64 `cfg:"slowlog-log-slower-than"`
	SlowLogMaxLen     int   `cfg:"slowlog-max-len"`

	ClusterEnable     bool   `cfg:"cluster-enable"`
	ClusterAsSeed     bool   `cfg:"cluster-as-seed"`
	ClusterSeed       string `cfg:"cluster-seed"`
	RaftListenAddr    string `cfg:"raft-listen-address"`
	RaftAdvertiseAddr string `cfg:"raft-advertise-address"`
	// If the node join the cluster as a replica of another node,
	// set MasterInCluster as the RedisAdvertiseAddr of it's master node
	MasterInCluster string `cfg:"master-in-cluster"`
}

var configFilePath string

func GetConfigFilePath() string {
	return configFilePath
}

type ServerInfo struct {
	StartUpTime time.Time
}

func (p *ServerProperties) AnnounceAddress() string {
	if p.AnnounceHost != "" {
		return p.AnnounceHost + ":" + strconv.Itoa(p.Port)
	}
	return p.Bind + ":" + strconv.Itoa(p.Port)
}

func (p *ServerProperties) RaftAnnounceAddress() string {
	if p.RaftAdvertiseAddr != "" {
		return p.RaftAdvertiseAddr
	}
	return p.RaftListenAddr
}

// resolvePath resolves a filename relative to the configured working
// directory. Absolute paths and the empty string are returned unchanged.
func (p *ServerProperties) resolvePath(filename string) string {
	if filename == "" || filepath.IsAbs(filename) {
		return filename
	}
	dir := p.Dir
	if dir == "" {
		dir = "."
	}
	return filepath.Join(dir, filename)
}

// AofPath returns the effective path of the append-only file.
func (p *ServerProperties) AofPath() string {
	return p.resolvePath(p.AppendFilename)
}

// RdbPath returns the effective path of the configured RDB file, or "" when
// no RDB filename is configured.
func (p *ServerProperties) RdbPath() string {
	return p.resolvePath(p.RDBFilename)
}

// DumpPath returns the path used by SAVE/BGSAVE, defaulting to dump.rdb.
func (p *ServerProperties) DumpPath() string {
	if p.RDBFilename == "" {
		return p.resolvePath("dump.rdb")
	}
	return p.resolvePath(p.RDBFilename)
}

func defaultProperties() *ServerProperties {
	return &ServerProperties{
		Bind:              "127.0.0.1",
		Port:              6379,
		Databases:         16,
		MaxClients:        1000,
		AppendFilename:    "appendonly.aof",
		AppendFsync:       "everysec",
		SlowLogSlowerThan: 10000,
		SlowLogMaxLen:     128,
		RunID:             utils.RandString(40),
	}
}

// Properties holds global config properties
var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

func init() {
	// A few stats we don't want to reset: server startup time, and peak mem.
	EachTimeServerInfo = &ServerInfo{
		StartUpTime: time.Now(),
	}

	// default config
	Properties = defaultProperties()
}

func parseBool(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "yes", "true", "1", "on":
		return true, true
	case "no", "false", "0", "off":
		return false, true
	default:
		return false, false
	}
}

func validateProperties(p *ServerProperties) error {
	if p.Port < 1 || p.Port > 65535 {
		return fmt.Errorf("invalid port %d: must be between 1 and 65535", p.Port)
	}
	if p.Databases <= 0 {
		return fmt.Errorf("invalid databases %d: must be greater than 0", p.Databases)
	}
	if p.MaxClients < 0 {
		return fmt.Errorf("invalid maxclients %d: must not be negative", p.MaxClients)
	}
	if p.MaxClients > 1<<31-1 {
		return fmt.Errorf("invalid maxclients %d: too large", p.MaxClients)
	}
	if p.ReplTimeout < 0 {
		return fmt.Errorf("invalid repl-timeout %d: must not be negative", p.ReplTimeout)
	}
	if p.SlowLogMaxLen < 0 {
		return fmt.Errorf("invalid slowlog-max-len %d: must not be negative", p.SlowLogMaxLen)
	}
	switch strings.ToLower(p.AppendFsync) {
	case "always", "everysec", "no":
		p.AppendFsync = strings.ToLower(p.AppendFsync)
	default:
		return fmt.Errorf("invalid appendfsync %q: must be always, everysec or no", p.AppendFsync)
	}
	if p.AppendOnly && p.AppendFilename == "" {
		p.AppendFilename = "appendonly.aof"
	}
	if p.Dir == "" {
		p.Dir = "."
	}
	return nil
}

// parse starts from the built-in defaults instead of a zero-valued struct:
// a config file that only overrides one option must not reset bind to "" or
// port to 0 (which made the server listen on ":0" on every interface).
func parse(src io.Reader) (*ServerProperties, error) {
	config := defaultProperties()

	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		pivot := strings.IndexAny(line, " \t")
		if pivot <= 0 || pivot >= len(line)-1 {
			logger.Warn("ignore malformed config line: " + line)
			continue
		}
		key := strings.ToLower(strings.TrimSpace(line[0:pivot]))
		value := strings.TrimSpace(line[pivot+1:])
		if key == "" || value == "" {
			logger.Warn("ignore malformed config line: " + line)
			continue
		}
		rawMap[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	recognized := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}
		key = strings.ToLower(key)
		recognized[key] = struct{}{}
		value, ok := rawMap[key]
		if !ok {
			continue
		}
		switch field.Type.Kind() {
		case reflect.String:
			fieldVal.SetString(value)
		case reflect.Int, reflect.Int64:
			intValue, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				logger.Warn(fmt.Sprintf("invalid value for %s: %q (keeping default %v)", key, value, fieldVal.Interface()))
				continue
			}
			fieldVal.SetInt(intValue)
		case reflect.Bool:
			boolValue, ok := parseBool(value)
			if !ok {
				logger.Warn(fmt.Sprintf("invalid boolean for %s: %q (keeping default %v)", key, value, fieldVal.Interface()))
				continue
			}
			fieldVal.SetBool(boolValue)
		case reflect.Slice:
			if field.Type.Elem().Kind() == reflect.String {
				slice := strings.Split(value, ",")
				fieldVal.Set(reflect.ValueOf(slice))
			}
		}
	}
	for key := range rawMap {
		if _, ok := recognized[key]; !ok {
			logger.Warn("unknown config key: " + key)
		}
	}
	if err := validateProperties(config); err != nil {
		return nil, err
	}
	return config, nil
}

// SetupConfig read config file and store properties into Properties.
// It returns an error instead of panicking so that main can exit with a
// non-zero status and a clear message.
func SetupConfig(configFilename string) error {
	file, err := os.Open(configFilename)
	if err != nil {
		return fmt.Errorf("open config file %q: %w", configFilename, err)
	}
	defer file.Close()
	properties, err := parse(file)
	if err != nil {
		return fmt.Errorf("parse config file %q: %w", configFilename, err)
	}
	properties.RunID = utils.RandString(40)
	Properties = properties
	if abs, err := filepath.Abs(configFilename); err == nil {
		configFilePath = abs
	} else {
		logger.Warn("resolve config path failed: " + err.Error())
	}
	return nil
}

func GetTmpDir() string {
	return filepath.Join(Properties.Dir, "tmp")
}
