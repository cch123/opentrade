// Command perp-risk is ADR-0071's global risk coordinator. It owns the global
// insurance fund fold, TakenOverLot lifecycle, and per-symbol working-capital
// quotas; perp-counter shards remain the position authority and only emit user
// execution events / execute version-stamped tasks.
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
	var producer *journal.Producer
	brokers := splitCSV(cfg.Brokers)
	if len(brokers) > 0 {
		var err error
		producer, err = journal.NewProducer(journal.ProducerConfig{
			Brokers: brokers, ClientID: cfg.InstanceID + "-settlement", Topic: cfg.JournalTopic,
		}, logger)
		if err != nil {
			logger.Error("journal producer", zap.Error(err))
			return
		}
		defer producer.Close()
		riskHandler.settlementProducer = producer
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
	mu                 sync.Mutex
	coord              *perprisk.Coordinator
	shards             []string
	rpc                *shardrpc.Client
	settlementProducer settlementProducer
	logger             *zap.Logger
}

type settlementProducer interface {
	EmitRiskPoolSettlement(*eventpb.PerpJournalEvent)
}

func (h *handler) ApplyJournalEventAt(evt *eventpb.PerpJournalEvent, partition int32, offset int64) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	result, err := h.coord.ApplyJournalEventAtResult(evt, partition, offset)
	if err != nil || !result.Applied {
		return result.Applied, err
	}
	if result.Kind == perprisk.JournalKindTakeoverLot && result.BorrowAmount.Sign() > 0 {
		// ADR-0073 records the actual draw on the lot. This value can be lower
		// than the notional request because RiskPool quotas clamp exposure.
		borrow, err := h.coord.BorrowWorkingCapital(perprisk.BorrowRequest{
			Coin: result.Coin, Symbol: result.Symbol, Day: time.Now().UTC().Format("2006-01-02"),
			RefID: result.BorrowRef, Amount: result.BorrowAmount,
		})
		if err != nil {
			h.logger.Warn("working-capital borrow failed",
				zap.String("symbol", result.Symbol), zap.String("lot_id", result.LotID), zap.Error(err))
		} else if err := h.coord.MarkLotWorkingCapital(result.LotID, result.BorrowRef, borrow.Borrowed); err != nil {
			h.logger.Warn("working-capital lot mark failed",
				zap.String("symbol", result.Symbol), zap.String("lot_id", result.LotID), zap.Error(err))
		}
	}
	if result.Kind == perprisk.JournalKindLotADL && result.Lot.LeavesQty.Sign() == 0 {
		settlement, applied, err := h.coord.SettleLot(result.LotID)
		if err != nil {
			return result.Applied, err
		}
		if applied {
			h.emitRiskPoolSettlement(settlement)
		}
	}
	lot, ok := h.coord.Lot(result.LotID)
	if !ok || lot.LeavesQty.Sign() <= 0 || len(h.shards) == 0 || lot.TakeoverPrice.Sign() <= 0 {
		return result.Applied, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.planAndDispatchADL(ctx, lot); err != nil {
		h.logger.Warn("ADL dispatch incomplete",
			zap.String("symbol", lot.Symbol), zap.String("lot_id", lot.LotID), zap.Error(err))
	}
	return result.Applied, nil
}

func (h *handler) emitRiskPoolSettlement(s perprisk.RiskPoolSettlement) {
	if h.settlementProducer == nil {
		return
	}
	h.settlementProducer.EmitRiskPoolSettlement(&eventpb.PerpJournalEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: time.Now().UnixMilli(), ProducerId: "perp-risk"},
		Payload: &eventpb.PerpJournalEvent_RiskPoolSettlement{RiskPoolSettlement: &eventpb.RiskPoolSettlementEvent{
			LotId: s.LotID, Symbol: s.Symbol, Coin: s.Coin, WorkingCapitalRef: s.WorkingCapitalRef,
			TakenOverBalance:  s.TakenOverBalance.String(),
			LiqAdlRealisedPnl: s.LiqAdlRealizedPnL.String(),
			CumFee:            s.CumFee.String(), WorkingCapitalDrawn: s.WorkingCapitalDrawn.String(),
			BorrowedBalance: s.BorrowedBalance.String(), FinalPoolDelta: s.FinalPoolDelta.String(),
			Status: string(perprisk.LotStatusDone),
		}},
	})
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

func (h *handler) planAndDispatchADL(ctx context.Context, lot perprisk.TakenOverLot) error {
	var all []perprisk.ADLCandidate
	sources := map[string]string{}
	for _, endpoint := range h.shards {
		candidates, err := h.rpc.Candidates(ctx, endpoint, perprisk.CandidateRequest{
			Symbol: lot.Symbol, AdlPrice: lot.TakeoverPrice.String(), ExcludeUser: lot.UserID,
		})
		if err != nil {
			return err
		}
		for _, cand := range candidates {
			all = append(all, cand)
			sources[taskKey(cand.UserID, cand.Symbol, cand.PositionIdx, cand.PosSeq, cand.PositionVersion)] = endpoint
		}
	}
	// Reserve one round for the whole plan, not one per shard. The round is the
	// replay guard visible to every owning shard; sharing it lets logs and
	// snapshots reconstruct one lot-consumption attempt across all tasks.
	round := h.coord.ReserveAdlRound()
	planningLot, ok := h.coord.LotForADLPlanning(lot.LotID)
	if !ok || planningLot.LeavesQty.Sign() <= 0 {
		return nil
	}
	tasks := perprisk.PlanADL(planningLot, planningLot.TakeoverPrice, round, all)
	for _, task := range tasks {
		endpoint := sources[taskKey(task.UserID, task.Symbol, task.PositionIdx, task.PosSeq, task.PositionVersion)]
		if endpoint == "" {
			continue
		}
		if err := h.coord.RegisterInFlightADL(task); err != nil {
			return err
		}
		result, err := h.rpc.ExecuteTask(ctx, endpoint, task)
		if err != nil {
			// Keep the task reserved on transport errors. The shard may have
			// accepted it and later emit the ADL journal event; clearing here would
			// allow an over-dispatch before replay proves what happened.
			return err
		}
		if !result.Applied {
			h.coord.CompleteInFlightADL(task.LotID, task.UserID, task.AdlRound)
		}
		h.logger.Info("ADL task dispatched",
			zap.Uint64("user", task.UserID), zap.String("symbol", task.Symbol),
			zap.String("lot_id", task.LotID), zap.Uint64("adl_round", task.AdlRound),
			zap.Bool("applied", result.Applied), zap.String("fact_qty", result.FactQty.String()))
	}
	return nil
}

// taskKey routes a planned task back to the shard that reported the source
// candidate. position_idx is part of the key (ADR-0077 §4): a hedge user's
// two legs are distinct candidates that may even live at the same
// (pos_seq, version) right after a restart.
func taskKey(user uint64, symbol string, positionIdx uint8, posSeq, positionVersion uint64) string {
	return strconv.FormatUint(user, 10) + "|" + symbol + "|" + strconv.Itoa(int(positionIdx)) +
		"|" + strconv.FormatUint(posSeq, 10) + "|" + strconv.FormatUint(positionVersion, 10)
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
