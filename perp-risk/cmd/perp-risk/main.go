// Command perp-risk is ADR-0071's global risk coordinator. It owns the global
// insurance fund fold and per-symbol working-capital quotas; perp-counter shards
// remain the position authority and only emit InsuranceDelta / execute
// version-stamped tasks.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-risk/internal/journal"
	"github.com/xargin/opentrade/perp-risk/internal/shardrpc"
	"github.com/xargin/opentrade/perp-risk/internal/snapshot"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/election"
	"github.com/xargin/opentrade/pkg/logx"
	"github.com/xargin/opentrade/pkg/perprisk"
)

type Config struct {
	InstanceID       string
	Brokers          string
	JournalTopic     string
	GroupID          string
	SnapshotPath     string
	SnapshotInterval time.Duration
	QuotaPolicies    quotaFlags
	ShardEndpoints   string
	ShardRPCTimeout  time.Duration
	HTTPAddr         string
	Env              string
	LogLevel         string
	HAMode           string
	EtcdEndpoints    string
	ElectionPath     string
	LeaseTTL         int
	CampaignBackoff  time.Duration
}

type quotaFlags []string

func (q *quotaFlags) String() string { return strings.Join(*q, ",") }
func (q *quotaFlags) Set(v string) error {
	*q = append(*q, v)
	return nil
}

func main() {
	cfg := parseFlags()
	logger, err := logx.New(logx.Config{Service: "perp-risk", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	logx.SetGlobal(logger)

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	etcd := splitCSV(cfg.EtcdEndpoints)
	if cfg.HAMode != "auto" || len(etcd) == 0 {
		runPrimary(rootCtx, cfg, logger)
		return
	}
	runElectionLoop(rootCtx, cfg, etcd, logger)
}

func runPrimary(ctx context.Context, cfg Config, logger *zap.Logger) {
	coord := perprisk.New()
	if cfg.SnapshotPath != "" {
		snap, ok, err := snapshot.Load(cfg.SnapshotPath)
		if err != nil {
			logger.Error("load snapshot", zap.Error(err), zap.String("path", cfg.SnapshotPath))
			return
		}
		if ok {
			coord.Restore(snap.Coordinator)
			logger.Info("restored coordinator snapshot", zap.Int64("ts_unix_ms", snap.TsUnixMs))
		}
	}
	if err := applyQuotaPolicies(coord, cfg.QuotaPolicies); err != nil {
		logger.Error("invalid quota policy", zap.Error(err))
		return
	}
	riskHandler := &handler{
		coord: coord, shards: splitCSV(cfg.ShardEndpoints),
		rpc: shardrpc.New(cfg.ShardRPCTimeout), logger: logger,
	}

	if cfg.SnapshotPath != "" {
		go runSnapshotLoop(ctx, cfg.SnapshotInterval, cfg.SnapshotPath, riskHandler, logger)
	}
	httpSrv := riskHTTPServer(cfg.HTTPAddr, riskHandler, logger)
	if httpSrv != nil {
		go func() {
			logger.Info("perp-risk internal HTTP listening", zap.String("addr", cfg.HTTPAddr))
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("internal HTTP exited", zap.Error(err))
			}
		}()
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := httpSrv.Shutdown(shutdownCtx); err != nil {
				logger.Error("internal HTTP shutdown", zap.Error(err))
			}
		}()
	}

	var consumer *journal.Consumer
	brokers := splitCSV(cfg.Brokers)
	if len(brokers) > 0 {
		var err error
		consumer, err = journal.NewConsumer(journal.ConsumerConfig{
			Brokers: brokers, ClientID: cfg.InstanceID, GroupID: cfg.GroupID,
			Topic: cfg.JournalTopic, InitialOffsets: coord.Offsets(),
		}, riskHandler, logger)
		if err != nil {
			logger.Error("journal consumer", zap.Error(err))
			return
		}
		defer consumer.Close()
	} else {
		logger.Warn("no --brokers: perp-risk will only keep restored state and snapshots")
	}

	logger.Info("perp-risk up (ADR-0071)",
		zap.Strings("brokers", brokers), zap.String("journal_topic", cfg.JournalTopic),
		zap.String("snapshot_path", cfg.SnapshotPath))

	errCh := make(chan error, 1)
	if consumer != nil {
		go func() { errCh <- consumer.Run(ctx) }()
	}
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("journal consumer exited", zap.Error(err))
		}
	}
	if consumer != nil {
		consumer.Close()
	}
	if cfg.SnapshotPath != "" {
		if err := snapshot.Save(cfg.SnapshotPath, riskHandler.Snapshot()); err != nil {
			logger.Error("final snapshot", zap.Error(err))
		}
	}
}

func runElectionLoop(rootCtx context.Context, cfg Config, etcd []string, logger *zap.Logger) {
	elec, err := election.New(election.Config{
		Endpoints: etcd, Path: cfg.ElectionPath, Value: cfg.InstanceID, LeaseTTL: cfg.LeaseTTL,
	})
	if err != nil {
		logger.Fatal("election init", zap.Error(err))
	}
	defer func() { _ = elec.Close() }()

	for {
		if rootCtx.Err() != nil {
			return
		}
		logger.Info("campaigning for perp-risk leadership", zap.String("path", cfg.ElectionPath))
		if err := elec.Campaign(rootCtx); err != nil {
			if rootCtx.Err() != nil {
				return
			}
			logger.Error("campaign failed", zap.Error(err))
			select {
			case <-rootCtx.Done():
				return
			case <-time.After(cfg.CampaignBackoff):
			}
			continue
		}
		logger.Info("became perp-risk primary", zap.String("instance", cfg.InstanceID))

		primaryCtx, cancelPrimary := context.WithCancel(rootCtx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			select {
			case <-elec.LostCh():
				logger.Warn("lost perp-risk leadership")
				cancelPrimary()
			case <-primaryCtx.Done():
			}
		}()

		runPrimary(primaryCtx, cfg, logger)
		cancelPrimary()
		<-watchDone

		if rootCtx.Err() != nil {
			resignCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := elec.Resign(resignCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn("resign failed", zap.Error(err))
			}
			cancel()
			return
		}
		logger.Info("demoted; re-campaigning")
	}
}

func parseFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.InstanceID, "instance-id", "perp-risk-0", "instance id / Kafka client id")
	flag.StringVar(&cfg.Brokers, "brokers", "", "comma-separated Kafka brokers; empty disables journal consumption")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", "perp-journal", "perp-counter WAL topic to fold")
	flag.StringVar(&cfg.GroupID, "group", "perp-risk", "Kafka group for partition assignment")
	flag.StringVar(&cfg.SnapshotPath, "snapshot-path", "./data/perp-risk/snapshot.json", "coordinator snapshot path")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", 60*time.Second, "how often to persist fund/quota state and journal offsets")
	flag.Var(&cfg.QuotaPolicies, "quota", "symbol:fraction:absolute_cap daily working-capital quota; repeatable, zero fields disable that dimension")
	flag.StringVar(&cfg.ShardEndpoints, "perp-counter-shards", "", "comma-separated perp-counter base URLs used for ADR-0071 candidate queries and ADL task dispatch")
	flag.DurationVar(&cfg.ShardRPCTimeout, "shard-rpc-timeout", 3*time.Second, "timeout for internal perp-counter shard RPCs")
	flag.StringVar(&cfg.HTTPAddr, "http-addr", ":8091", "internal HTTP address for health and working-capital repayment; empty disables")
	flag.StringVar(&cfg.Env, "env", "dev", "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "log level")
	flag.StringVar(&cfg.HAMode, "ha-mode", "disabled", "ha mode: disabled | auto (etcd leader election, ADR-0031/0071)")
	flag.StringVar(&cfg.EtcdEndpoints, "etcd", "", "comma-separated etcd endpoints (required for --ha-mode=auto)")
	flag.StringVar(&cfg.ElectionPath, "election-path", "/cex/perp-risk/leader", "etcd election key for the global risk coordinator")
	flag.IntVar(&cfg.LeaseTTL, "lease-ttl", 10, "etcd session TTL seconds")
	flag.DurationVar(&cfg.CampaignBackoff, "campaign-backoff", 2*time.Second, "wait between failed campaigns")
	flag.Parse()
	return cfg
}

func riskHTTPServer(addr string, h *handler, logger *zap.Logger) *http.Server {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc(perprisk.WorkingCapitalRepayPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req perprisk.WorkingCapitalRepayRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		amount, err := dec.Parse(req.Amount)
		if err != nil || req.RefID == "" || amount.Sign() <= 0 {
			http.Error(w, "ref_id and positive amount required", http.StatusBadRequest)
			return
		}
		if err := h.RepayWorkingCapital(req.RefID, amount); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(perprisk.WorkingCapitalRepayResponse{Applied: true})
	})
	return &http.Server{Addr: addr, Handler: mux}
}

type handler struct {
	mu     sync.Mutex
	coord  *perprisk.Coordinator
	shards []string
	rpc    *shardrpc.Client
	logger *zap.Logger
}

func (h *handler) ApplyJournalEventAt(evt *eventpb.PerpJournalEvent, partition int32, offset int64) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delta, applied, err := h.coord.ApplyJournalEventAtResult(evt, partition, offset)
	if err != nil || !applied {
		return applied, err
	}
	if delta.Backstop && delta.TakeoverNotional.Sign() > 0 {
		if _, err := h.coord.BorrowWorkingCapital(perprisk.BorrowRequest{
			Coin: delta.Coin, Symbol: delta.Symbol, Day: time.Now().UTC().Format("2006-01-02"),
			RefID: "takeover:" + delta.RefID, Amount: delta.TakeoverNotional,
		}); err != nil {
			h.logger.Warn("working-capital borrow failed",
				zap.String("symbol", delta.Symbol), zap.String("ref_id", delta.RefID), zap.Error(err))
		}
	}
	if len(h.shards) == 0 || delta.Symbol == "" || delta.Price.Sign() <= 0 {
		return applied, nil
	}
	deficit := h.coord.Fund(delta.Coin).Neg()
	if deficit.Sign() <= 0 {
		return applied, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.planAndDispatchADL(ctx, delta, deficit); err != nil {
		h.logger.Warn("ADL dispatch incomplete",
			zap.String("symbol", delta.Symbol), zap.String("deficit", deficit.String()), zap.Error(err))
	}
	return applied, nil
}

func (h *handler) Snapshot() perprisk.Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.coord.Snapshot()
}

func (h *handler) RepayWorkingCapital(refID string, amount dec.Decimal) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.coord.RepayWorkingCapitalRef(refID, amount)
}

func (h *handler) planAndDispatchADL(ctx context.Context, delta perprisk.InsuranceDelta, deficit dec.Decimal) error {
	var all []perprisk.ADLCandidate
	sources := map[string]string{}
	for _, endpoint := range h.shards {
		candidates, err := h.rpc.Candidates(ctx, endpoint, perprisk.CandidateRequest{
			Symbol: delta.Symbol, AdlPrice: delta.Price.String(), ExcludeUser: delta.UserID,
		})
		if err != nil {
			return err
		}
		for _, cand := range candidates {
			all = append(all, cand)
			sources[taskKey(cand.UserID, cand.Symbol, cand.PosSeq, cand.PositionVersion)] = endpoint
		}
	}
	round := h.coord.ReserveAdlRound()
	tasks := perprisk.PlanADL(deficit, delta.Price, round, all)
	for _, task := range tasks {
		endpoint := sources[taskKey(task.UserID, task.Symbol, task.PosSeq, task.PositionVersion)]
		if endpoint == "" {
			continue
		}
		applied, err := h.rpc.ExecuteTask(ctx, endpoint, task)
		if err != nil {
			return err
		}
		h.logger.Info("ADL task dispatched",
			zap.String("user", task.UserID), zap.String("symbol", task.Symbol),
			zap.Uint64("adl_round", task.AdlRound), zap.Bool("applied", applied))
	}
	return nil
}

func taskKey(user, symbol string, posSeq, positionVersion uint64) string {
	return user + "|" + symbol + "|" + strconv.FormatUint(posSeq, 10) + "|" + strconv.FormatUint(positionVersion, 10)
}

type snapper interface {
	Snapshot() perprisk.Snapshot
}

func runSnapshotLoop(ctx context.Context, interval time.Duration, path string, state snapper, logger *zap.Logger) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := snapshot.Save(path, state.Snapshot()); err != nil {
				logger.Error("periodic snapshot", zap.Error(err))
			}
		}
	}
}

func applyQuotaPolicies(coord *perprisk.Coordinator, raw []string) error {
	for _, item := range raw {
		fields := strings.Split(item, ":")
		if len(fields) != 3 {
			return errors.New("quota must be symbol:fraction:absolute_cap")
		}
		fraction, err := dec.Parse(strings.TrimSpace(fields[1]))
		if err != nil {
			return err
		}
		cap, err := dec.Parse(strings.TrimSpace(fields[2]))
		if err != nil {
			return err
		}
		if err := coord.SetQuotaPolicy(strings.TrimSpace(fields[0]), perprisk.QuotaPolicy{
			Fraction: fraction, AbsoluteCap: cap,
		}); err != nil {
			return err
		}
	}
	return nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
