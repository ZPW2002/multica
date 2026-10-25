package lark

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// StreamingReplier mirrors a running agent's text output into a Feishu
// streaming card (cardkit v1), closing the gap between the daemon's 500 ms
// transcript batches and the user's chat: instead of waiting for chat:done
// and posting one assembled reply, the answer appears — and grows — while
// the agent works.
//
// Wire shape (per the cardkit docs; see CardStreamingClient):
//
//   - task:running, or the first thinking/text frame if running was not
//     seen → claim a channel_outbound_card_message row (status pending),
//     then create the card entity and deliver it as a card-ref message in
//     the same reply position a plain reply would use (topic / native
//     reply / chat level). task:running opens on a pending placeholder,
//     which the first frame renders into;
//   - further frames → buffered, and flushed as FULL-content element
//     updates roughly every FlushInterval. The card carries two
//     independently-updated markdown elements: the answer streams
//     append-only into the reply element (Feishu typewriter-renders the
//     appended suffix as long as the old content is a prefix of the new),
//     while the reasoning previews in its own element as a bounded window
//     that follows the latest text — Lark renders a non-append update
//     instantly, so the window reset reads as scrolling, not stutter.
//     Keeping the regions in separate elements is what stops reasoning
//     that arrives AFTER answer text (agentic tool loops) from rewriting
//     the answer region: each element only ever grows on its own.
//     Reasoning appearing on a live single-element card upgrades it to
//     this split layout with one whole-card replace;
//   - chat:done → one whole-card replace settles the card on the
//     authoritative answer alone (the thinking box and divider drop away
//     with no re-animation; the complete reasoning stays in the web
//     transcript), then streaming mode closes; task:failed /
//     task:cancelled annotate the card and close. Row status ends
//     final/error.
//
// Coordination with the Patcher (the legacy whole-reply path): the claimed
// DB row is the ownership signal. It is inserted synchronously on the event
// bus thread at the first activity (task:running or the first thinking/text
// frame), so by the time chat:done arrives the
// Patcher's GetLarkOutboundCardByTask check sees it and skips its own send.
// A task that never produces a text frame never claims a row, and its
// chat:done keeps the legacy path.
//
// Concurrency: bus handlers only do DB reads/writes and in-memory buffer
// appends — every cardkit HTTP call happens on the per-stream worker
// goroutine, so a slow Lark API can never stall the daemon's
// ReportTaskMessages publish path the bus dispatches on. The worker is the
// only writer of card state (card id, sequence, flushed content), which
// keeps the cardkit sequence strictly increasing by construction.
type StreamingReplier struct {
	queries     StreamingQueries
	credentials CredentialsResolver
	client      APIClient
	cards       CardStreamingClient
	cfg         StreamingConfig

	mu      sync.Mutex
	streams map[string]*streamState
	// foreign caches task ids resolved to "not ours" (web tasks, other
	// channels) so the 500 ms transcript cadence does not re-run the
	// delivery lookup for the lifetime of a task.
	foreign map[string]time.Time
}

// StreamingQueries is the narrow DB surface the streaming replier needs.
// *ChannelStore satisfies it directly.
type StreamingQueries interface {
	engine.ChannelProvenanceQueries
	GetAgentTask(ctx context.Context, id pgtype.UUID) (db.AgentTaskQueue, error)
	GetChannelTaskDelivery(ctx context.Context, taskID pgtype.UUID) (db.ChannelTaskDelivery, error)
	GetLarkInstallation(ctx context.Context, id pgtype.UUID) (Installation, error)
	GetLarkOutboundCardByTask(ctx context.Context, taskID pgtype.UUID) (OutboundCardMessage, error)
	CreateLarkOutboundCardMessage(ctx context.Context, arg CreateOutboundCardMessageParams) (OutboundCardMessage, error)
	SetLarkOutboundCardIdentity(ctx context.Context, arg SetOutboundCardIdentityParams) error
	UpdateLarkOutboundCardProgress(ctx context.Context, arg UpdateOutboundCardProgressParams) error
	UpdateLarkOutboundCardStatus(ctx context.Context, arg UpdateOutboundCardStatusParams) error
}

// StreamingConfig tunes the streaming replier; zero values default.
type StreamingConfig struct {
	// FlushInterval is the coalesce window for element content updates.
	// Must stay well under Feishu's cardkit QPS limits (50/s, 1000/min) —
	// one update per interval per active stream.
	FlushInterval time.Duration
	// MaxCardBytes caps the accumulated content we keep pushing into the
	// card. Feishu rejects cards over 30KB; past this bound the card keeps
	// a truncated tail and the full reply rides a legacy message at
	// finalize.
	MaxCardBytes int
	Now          func() time.Time
	Logger       *slog.Logger
}

const (
	// streamElementID addresses the card's reply markdown element and
	// streamThinkElementID the thinking window in cardkit content updates.
	// Lark caps element ids at 20 chars.
	streamElementID      = "reply_text"
	streamThinkElementID = "think_text"

	defaultStreamFlushInterval = 1200 * time.Millisecond
	defaultStreamMaxCardBytes  = 24_000

	// streamOpTimeout bounds one cardkit HTTP call on the worker.
	streamOpTimeout = 8 * time.Second
	// streamCloseGraceCap bounds the typewriter catch-up wait before a card
	// settles; production flush windows stay well under it.
	streamCloseGraceCap = 1500 * time.Millisecond
	// streamIdleTimeout retires a worker whose task never produced a
	// terminal event (lost chat:done); Feishu auto-closes streaming mode
	// server-side after its own inactivity window regardless.
	streamIdleTimeout = 10 * time.Minute
	// streamForeignTTL bounds the not-ours task cache.
	streamForeignTTL = 15 * time.Minute

	streamTextType  = "text"
	streamThinkType = "thinking"

	// The thinking preview is a bounded window over the reasoning buffer:
	// it grows append-only (typewriter) until it passes the high-water
	// mark, then one update resets it to the latest low-water runes —
	// Lark renders a non-append update instantly, so the reset reads as
	// the window scrolling forward instead of the card freezing at a cap.
	// The bounds also keep the split card comfortably inside Feishu's 30KB
	// card cap alongside a full-size reply.
	streamThinkWindowLow  = 600
	streamThinkWindowHigh = 1500
)

// NewStreamingReplier builds the streaming replier. Streaming is disabled
// (Register is a no-op) when the client does not implement
// CardStreamingClient — the stub client and narrow test fakes — mirroring
// how the typing sweep degrades without ReactionLister.
func NewStreamingReplier(queries StreamingQueries, credentials CredentialsResolver, client APIClient, cfg StreamingConfig) *StreamingReplier {
	if cfg.Logger == nil {
		// Default INTO cfg, not a local: every log call in this package goes
		// through cfg.Logger, and a nil one panics the worker goroutine —
		// which takes the whole process down — on the first warning.
		cfg.Logger = slog.Default()
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultStreamFlushInterval
	}
	if cfg.MaxCardBytes <= 0 {
		cfg.MaxCardBytes = defaultStreamMaxCardBytes
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	var cards CardStreamingClient
	if client != nil {
		cards, _ = client.(CardStreamingClient)
	}
	return &StreamingReplier{
		queries:     queries,
		credentials: credentials,
		client:      client,
		cards:       cards,
		cfg:         cfg,
		streams:     make(map[string]*streamState),
		foreign:     make(map[string]time.Time),
	}
}

// Register attaches the streaming replier to the bus before traffic starts.
func (s *StreamingReplier) Register(bus *events.Bus) {
	if s.cards == nil {
		s.cfg.Logger.Debug("lark streaming replier: client cannot stream cards; disabled")
		return
	}
	bus.Subscribe(protocol.EventTaskMessage, s.handleEvent)
	bus.Subscribe(protocol.EventTaskRunning, s.handleEvent)
	bus.Subscribe(protocol.EventChatDone, s.handleEvent)
	bus.Subscribe(protocol.EventTaskFailed, s.handleEvent)
	bus.Subscribe(protocol.EventTaskCancelled, s.handleEvent)
}

func (s *StreamingReplier) handleEvent(e events.Event) {
	taskID, _, ok := taskAndSessionFromEvent(e)
	if !ok {
		return
	}
	if e.Type == protocol.EventTaskMessage {
		s.handleTaskMessage(e, taskID)
		return
	}
	if e.Type == protocol.EventTaskRunning {
		s.handleTaskRunning(taskID)
		return
	}
	s.handleTerminal(e, taskID)
}

// handleTaskRunning opens the streaming card the moment the daemon starts
// executing — before any frame exists — so the user watches a pending
// placeholder instead of silence while the agent warms up. The first
// thinking/text frame replaces it.
func (s *StreamingReplier) handleTaskRunning(taskID pgtype.UUID) {
	key := uuidString(taskID)
	s.mu.Lock()
	_, streaming := s.streams[key]
	_, foreign := s.foreign[key]
	s.mu.Unlock()
	if streaming || foreign {
		return
	}
	s.openStream(taskID, frameNone, "")
}

// ---------------------------------------------------------------- inbound

func (s *StreamingReplier) handleTaskMessage(e events.Event, taskID pgtype.UUID) {
	kind, content := taskMessageFrame(e.Payload)
	if kind == frameOther {
		return
	}
	if kind == frameTool {
		// Tool frames never start a stream, but they seal the current
		// text/thinking run (the daemon seals a pending frame on every type
		// switch), so the next frame opens a new paragraph.
		key := uuidString(taskID)
		s.mu.Lock()
		st := s.streams[key]
		s.mu.Unlock()
		if st != nil {
			st.markBoundary()
		}
		return
	}
	if content == "" {
		return
	}
	key := uuidString(taskID)

	s.mu.Lock()
	st, streaming := s.streams[key]
	_, foreign := s.foreign[key]
	s.mu.Unlock()
	if streaming {
		st.append(kind, content)
		return
	}
	if foreign {
		return
	}

	s.openStream(taskID, kind, content)
}

// openStream resolves a task's channel provenance and claims the outbound
// card row the first time a Feishu-bound task shows activity — a thinking or
// text frame, or the task:running event that precedes them both — then starts
// the worker with the opening frame (if any) already buffered. The worker's
// immediate opening flush delivers the card while it still holds (mostly)
// the opening line, instead of letting a full FlushInterval of frames pile
// up first. kind==frameNone opens with a pending placeholder instead.
func (s *StreamingReplier) openStream(taskID pgtype.UUID, kind frameKind, firstText string) {
	ctx, cancel := context.WithTimeout(context.Background(), streamOpTimeout)
	defer cancel()

	delivery, err := s.queries.GetChannelTaskDelivery(ctx, taskID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.rememberForeign(uuidString(taskID))
			return
		}
		s.cfg.Logger.Warn("lark streaming: delivery lookup failed", "task_id", uuidString(taskID), "error", err)
		return
	}
	if delivery.ChannelType != channelTypeFeishu {
		s.rememberForeign(uuidString(taskID))
		return
	}
	task, err := s.queries.GetAgentTask(ctx, taskID)
	if err != nil {
		s.cfg.Logger.Warn("lark streaming: task lookup failed", "task_id", uuidString(taskID), "error", err)
		return
	}
	if !task.ChatSessionID.Valid {
		s.rememberForeign(uuidString(taskID))
		return
	}
	deliver, err := engine.TaskInputIsChannelIngested(ctx, s.queries, task)
	if err != nil {
		s.cfg.Logger.Warn("lark streaming: provenance classification failed", "task_id", uuidString(taskID), "error", err)
		return
	}
	if !deliver {
		s.rememberForeign(uuidString(taskID))
		return
	}

	binding := bindingFromDelivery(delivery)
	// A topic-isolated session with no frozen trigger cannot receive ANY
	// outbound (see topicSendWithoutTrigger); streaming is no exception.
	if topicSendWithoutTrigger(binding) {
		s.rememberForeign(uuidString(taskID))
		return
	}
	inst, err := s.queries.GetLarkInstallation(ctx, binding.InstallationID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.rememberForeign(uuidString(taskID))
			return
		}
		s.cfg.Logger.Warn("lark streaming: installation lookup failed", "task_id", uuidString(taskID), "error", err)
		return
	}
	if InstallationStatus(inst.Status) != InstallationActive {
		s.rememberForeign(uuidString(taskID))
		return
	}
	secret, err := s.credentials.DecryptAppSecret(inst)
	if err != nil {
		s.cfg.Logger.Warn("lark streaming: credential resolve failed", "task_id", uuidString(taskID), "error", err)
		return
	}
	creds := InstallationCredentials{
		AppID:     inst.AppID,
		AppSecret: secret,
		Region:    RegionOrDefault(inst.Region),
	}
	if inst.TenantKey.Valid {
		creds.TenantKey = inst.TenantKey.String
	}

	// A row already claimed for this task means a previous attempt streamed
	// (task_id carries a unique index). The retry keeps the original card:
	// stand down here and let recoverTerminal update it at the terminal
	// event, rather than fighting the unique constraint every frame.
	if _, err := s.queries.GetLarkOutboundCardByTask(ctx, taskID); err == nil {
		s.rememberForeign(uuidString(taskID))
		return
	} else if !errors.Is(err, pgx.ErrNoRows) {
		s.cfg.Logger.Warn("lark streaming: existing card lookup failed", "task_id", uuidString(taskID), "error", err)
		return
	}

	// Claim the row synchronously on the bus thread: from this moment the
	// Patcher treats this task's chat:done as owned by the streaming card,
	// so the assembled reply is never sent twice.
	row, err := s.queries.CreateLarkOutboundCardMessage(ctx, CreateOutboundCardMessageParams{
		ChatSessionID:        task.ChatSessionID,
		ChannelChatID:        binding.ChannelChatID,
		ChannelCardMessageID: "",
		Status:               string(CardStatusPending),
		TaskID:               taskID,
	})
	if err != nil {
		s.cfg.Logger.Warn("lark streaming: claim card row failed", "task_id", uuidString(taskID), "error", err)
		return
	}

	st := &streamState{
		sr:            s,
		taskID:        taskID,
		cardRowID:     row.ID,
		creds:         creds,
		chatID:        outboundChatID(binding),
		target:        threadReplyTarget(binding),
		mentionOpenID: mentionOpenID(binding),
		opCh:          make(chan streamOp, 1),
		lastActive:    s.cfg.Now(),
	}
	if kind != frameNone && firstText != "" {
		// Buffer the opening frame before the worker exists, so its
		// immediate first flush cannot race an empty buffer.
		st.append(kind, firstText)
	}
	s.mu.Lock()
	s.streams[uuidString(taskID)] = st
	s.mu.Unlock()
	go st.run()
}

// ---------------------------------------------------------------- terminal

func (s *StreamingReplier) handleTerminal(e events.Event, taskID pgtype.UUID) {
	key := uuidString(taskID)
	s.mu.Lock()
	st := s.streams[key]
	delete(s.streams, key)
	delete(s.foreign, key)
	s.mu.Unlock()

	if st != nil {
		st.finish(opFromEvent(e))
		return
	}
	// No in-memory state: either the task never streamed (nothing to do)
	// or this process restarted mid-stream. The claimed row is the durable
	// ownership signal — recover the card and settle it, because the
	// Patcher has already stood down on the strength of that row.
	s.recoverTerminal(e, taskID)
}

func opFromEvent(e events.Event) streamOp {
	switch e.Type {
	case protocol.EventTaskFailed:
		return streamOp{kind: streamOpFail, errMessage: errorMessageFromPayload(e.Payload)}
	case protocol.EventTaskCancelled:
		return streamOp{kind: streamOpCancel}
	default:
		return streamOp{kind: streamOpFinal, content: chatDoneContent(e.Payload)}
	}
}

// recoverTerminal settles a streaming card whose worker died with the
// process. It runs on the bus thread like the Patcher's terminal handling;
// the double streamOpTimeout budget matches the Patcher's tolerance.
func (s *StreamingReplier) recoverTerminal(e events.Event, taskID pgtype.UUID) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*streamOpTimeout)
	defer cancel()

	row, err := s.queries.GetLarkOutboundCardByTask(ctx, taskID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			s.cfg.Logger.Warn("lark streaming: recovery card lookup failed", "task_id", uuidString(taskID), "error", err)
		}
		return
	}
	op := opFromEvent(e)

	delivery, err := s.queries.GetChannelTaskDelivery(ctx, taskID)
	if err != nil {
		s.cfg.Logger.Warn("lark streaming: recovery delivery lookup failed", "task_id", uuidString(taskID), "error", err)
		return
	}
	binding := bindingFromDelivery(delivery)

	if row.CardEntityID == "" {
		// Claimed but the card was never created or delivered. Nothing is
		// on the user's screen; the whole reply rides the legacy path so
		// the ownership claim cannot swallow it.
		if op.kind == streamOpFinal && op.content != "" {
			s.legacySendForBinding(ctx, taskID, binding, op.content)
		}
		_ = s.queries.UpdateLarkOutboundCardStatus(ctx, UpdateOutboundCardStatusParams{ID: row.ID, Status: string(CardStatusError)})
		return
	}

	inst, err := s.queries.GetLarkInstallation(ctx, binding.InstallationID)
	if err != nil {
		s.cfg.Logger.Warn("lark streaming: recovery installation lookup failed", "task_id", uuidString(taskID), "error", err)
		return
	}
	secret, err := s.credentials.DecryptAppSecret(inst)
	if err != nil {
		s.cfg.Logger.Warn("lark streaming: recovery credential resolve failed", "task_id", uuidString(taskID), "error", err)
		return
	}

	st := &streamState{
		sr:        s,
		taskID:    taskID,
		cardRowID: row.ID,
		creds: InstallationCredentials{
			AppID:     inst.AppID,
			AppSecret: secret,
			Region:    RegionOrDefault(inst.Region),
		},
		chatID:        outboundChatID(binding),
		target:        threadReplyTarget(binding),
		mentionOpenID: mentionOpenID(binding),
		cardID:        row.CardEntityID,
		seq:           row.LastSequence,
	}
	switch op.kind {
	case streamOpFail:
		st.fail(ctx, op.errMessage)
	case streamOpCancel:
		st.settle(ctx)
	default:
		st.settleWithContent(ctx, op.content)
	}
}

// ---------------------------------------------------------------- state

// frameKind classifies the task:message frames the streaming replier
// consumes: the answer's text, the agent's thinking, the tool boundaries
// between them. frameNone opens a stream without a frame (task start).
type frameKind int

const (
	frameText frameKind = iota
	frameThinking
	frameTool
	frameOther
	frameNone
)

type streamOpKind int

const (
	streamOpFinal streamOpKind = iota
	streamOpFail
	streamOpCancel
)

type streamOp struct {
	kind       streamOpKind
	content    string
	errMessage string
}

// streamState is one task's streaming card. Bus-thread methods (append,
// finish) touch only mu-guarded fields and the op channel; every cardkit
// HTTP call happens on the run() goroutine, which exclusively owns cardID,
// seq and flushed.
type streamState struct {
	sr        *StreamingReplier
	taskID    pgtype.UUID
	cardRowID pgtype.UUID
	creds     InstallationCredentials
	chatID    ChatID
	target    ReplyTarget
	// mentionOpenID prefixes every content push with a native mention in
	// group chats; see mention.go. Empty sends unmentioned.
	mentionOpenID string

	opCh chan streamOp

	// worker-owned
	cardID string
	seq    int32
	// flushedReply / flushedThink are the last accepted contents of the
	// card's reply and thinking elements.
	flushedReply string
	flushedThink string
	// split records the card carrying the split layout (thinking window +
	// divider + reply element); thinkAnchor is the rune offset the thinking
	// window currently starts at.
	split       bool
	thinkAnchor int
	// placeholderShown records that the reply element still holds a
	// placeholder (the task-start pending marker or the split card's answer
	// slot), so the cancel/fail paths know to replace it.
	placeholderShown bool
	// lastFlushAt stamps the latest accepted cardkit content push; the
	// terminal close waits out the remainder of one flush window so the
	// client typewriter can render the tail before the card settles.
	lastFlushAt time.Time

	mu         sync.Mutex
	buf        strings.Builder
	thinkBuf   strings.Builder
	lastKind   frameKind
	boundary   bool
	dirty      bool
	oversize   bool
	finished   bool
	lastActive time.Time
}

// run is the per-stream worker: it serializes every cardkit operation so
// the sequence strictly increases by construction, and coalesces buffered
// frames into at most one content update per FlushInterval.
func (st *streamState) run() {
	ticker := time.NewTicker(st.sr.cfg.FlushInterval)
	defer ticker.Stop()
	// Paint before the first tick: the opening frame is already buffered, so
	// the card reaches the chat immediately. Waiting a full FlushInterval
	// lets a fast model bury the typewriter under most of the answer before
	// the card even appears.
	ctx, cancel := context.WithTimeout(context.Background(), streamOpTimeout)
	st.flush(ctx)
	cancel()
	for {
		select {
		case op := <-st.opCh:
			ctx, cancel := context.WithTimeout(context.Background(), 2*streamOpTimeout)
			switch op.kind {
			case streamOpFinal:
				st.finalize(ctx, op.content)
			case streamOpFail:
				st.fail(ctx, op.errMessage)
			case streamOpCancel:
				st.settle(ctx)
			}
			cancel()
			return
		case <-ticker.C:
			if st.idle() {
				// No terminal event ever arrived. Feishu closes streaming
				// mode server-side after its own inactivity window
				// regardless; drop the in-memory state so a late terminal
				// recovers the card from the claimed row.
				st.sr.cfg.Logger.Warn("lark streaming: idle stream retiring (terminal event lost?)",
					"task_id", uuidString(st.taskID), "card_id", st.cardID)
				st.sr.forgetStream(st)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), streamOpTimeout)
			st.flush(ctx)
			cancel()
		}
	}
}

// append buffers one frame. Frames of one kind that arrive consecutively are
// chunks of a single continuous block (the daemon's reporter concatenates
// same-type deltas and seals a row only on a type switch or its 500 ms
// flush), so they join with NO separator — inserting one breaks sentences
// mid-word. A paragraph break (\n\n) belongs only where a real boundary sits:
// a different frame kind, or any frame after a tool call sealed the run.
func (st *streamState) append(kind frameKind, text string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.finished || text == "" {
		return
	}
	dst := &st.buf
	if kind == frameThinking {
		dst = &st.thinkBuf
	}
	if dst.Len() > 0 && (st.lastKind != kind || st.boundary) {
		dst.WriteString("\n\n")
	}
	dst.WriteString(text)
	st.lastKind = kind
	st.boundary = false
	st.dirty = true
	st.lastActive = st.sr.cfg.Now()
}

// markBoundary records that a tool frame sealed the current run; the next
// append opens a new paragraph.
func (st *streamState) markBoundary() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.boundary = true
	st.lastActive = st.sr.cfg.Now()
}

func (st *streamState) snapshot() string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.buf.String()
}

// snapshotBoth returns the buffered thinking and answer text.
func (st *streamState) snapshotBoth() (thinking, answer string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.thinkBuf.String(), st.buf.String()
}

// finish delivers the terminal op. Called once from the bus thread after
// the state was removed from the manager's map.
func (st *streamState) finish(op streamOp) {
	st.mu.Lock()
	st.finished = true
	st.mu.Unlock()
	select {
	case st.opCh <- op:
	default:
		// The worker drains exactly one terminal op and the channel is
		// buffered; full means it is already terminating.
		st.sr.cfg.Logger.Warn("lark streaming: terminal op dropped (worker busy)", "task_id", uuidString(st.taskID))
	}
}

func (st *streamState) markActive() {
	st.mu.Lock()
	st.lastActive = st.sr.cfg.Now()
	st.mu.Unlock()
}

// idle reports a stream whose terminal event never arrived. Feishu closes
// streaming mode server-side after its own inactivity window, so retiring
// stops the worker and drops the in-memory state; the row stays as-is for
// operators.
func (st *streamState) idle() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.sr.cfg.Now().Sub(st.lastActive) > streamIdleTimeout
}

// flush pushes the buffered text as full-content element updates, creating
// and delivering the card on first use. Each element grows append-only —
// the reply with the answer, the thinking window with the latest reasoning —
// so Lark's typewriter animates deltas and never re-renders settled text;
// the window resets to the latest runes when it overflows, which Lark
// renders instantly (the preview "scrolls").
func (st *streamState) flush(ctx context.Context) {
	st.mu.Lock()
	thinking, answer := st.thinkBuf.String(), st.buf.String()
	if st.cardID != "" && !st.dirty && (st.split || thinking == "") {
		st.mu.Unlock()
		return
	}
	// Frames may keep arriving while this flush pushes; restoring the flag
	// on any failed push (a plain true-write cannot lose a concurrent
	// append's mark) makes the next tick retry.
	st.dirty = false
	oversize := st.oversize
	st.mu.Unlock()

	if st.cardID == "" {
		st.openCard(ctx, thinking, answer)
		return
	}

	failed := false
	if thinking != "" {
		wantThink, anchor := renderThinkWindow(thinking, st.thinkAnchor)
		switch {
		case !st.split:
			// Reasoning arrived on a live single-element card (text streams
			// first, tools run, then the model reasons): upgrade the card to
			// the split layout so the preview never rewrites the answer.
			if st.ensureSplit(ctx, wantThink) {
				st.thinkAnchor = anchor
			} else {
				failed = true
			}
		case wantThink != st.flushedThink:
			if st.putContent(ctx, streamThinkElementID, wantThink) {
				st.thinkAnchor = anchor
			} else {
				failed = true
			}
		}
	}

	if answer != "" && !oversize {
		wantReply := prependMarkdownMention(st.mentionOpenID, answer)
		switch {
		case wantReply == st.flushedReply:
		case len(wantReply) > st.sr.cfg.MaxCardBytes:
			// Keep buffering (finalize still has the full text), but stop
			// growing the card — Feishu rejects cards over 30KB.
			st.mu.Lock()
			st.oversize = true
			st.mu.Unlock()
		case st.putContent(ctx, streamElementID, wantReply):
			st.placeholderShown = false
		default:
			failed = true
		}
	}

	if failed {
		st.mu.Lock()
		st.dirty = true
		st.mu.Unlock()
	}
}

// openCard creates and delivers the card entity. A stream that already has
// reasoning opens directly on the split layout (thinking window, divider,
// answer slot); everything else opens on the single reply element.
func (st *streamState) openCard(ctx context.Context, thinking, answer string) {
	reply := streamPendingPlaceholder
	var think string
	var anchor int
	if thinking != "" {
		think, anchor = renderThinkWindow(thinking, st.thinkAnchor)
		reply = streamReplySlotPlaceholder
	}
	if answer != "" {
		reply = prependMarkdownMention(st.mentionOpenID, answer)
	}
	if st.ensureCard(ctx, reply, think) {
		st.thinkAnchor = anchor
		st.placeholderShown = answer == ""
	}
}

// ensureCard lazily creates and delivers the card entity on the worker.
// The first buffered text rides inside the created card, so no immediate
// content update is needed after delivery.
func (st *streamState) ensureCard(ctx context.Context, replyContent, thinkContent string) bool {
	if st.cardID != "" {
		return true
	}
	var cardJSON string
	var err error
	if thinkContent != "" {
		cardJSON, err = buildSplitCardJSON(thinkContent, replyContent)
	} else {
		cardJSON, err = buildReplyCardJSON(replyContent)
	}
	if err != nil {
		st.sr.cfg.Logger.Warn("lark streaming: build card failed", "task_id", uuidString(st.taskID), "error", err)
		return false
	}
	cardID, err := st.sr.cards.CreateCardEntity(ctx, CreateCardEntityParams{
		InstallationID: st.creds,
		CardJSON:       cardJSON,
	})
	if err != nil {
		st.sr.cfg.Logger.Warn("lark streaming: create card entity failed", "task_id", uuidString(st.taskID), "error", err)
		return false
	}
	messageID, err := st.sr.cards.SendCardMessage(ctx, SendCardMessageParams{
		InstallationID: st.creds,
		ChatID:         st.chatID,
		CardID:         cardID,
		ReplyTarget:    st.target,
	})
	if err != nil {
		// The entity is created but undelivered; entities are single-send
		// and expire unused, so the next tick rebuilds a fresh one.
		st.sr.cfg.Logger.Warn("lark streaming: deliver card failed", "task_id", uuidString(st.taskID), "error", err)
		return false
	}
	st.cardID = cardID
	st.flushedReply = replyContent
	st.flushedThink = thinkContent
	st.split = thinkContent != ""
	st.lastFlushAt = st.sr.cfg.Now()
	st.markActive()
	if err := st.sr.queries.SetLarkOutboundCardIdentity(ctx, SetOutboundCardIdentityParams{
		ID:                   st.cardRowID,
		ChannelCardMessageID: messageID,
		CardEntityID:         cardID,
	}); err != nil {
		// The card is live; losing the identity write only breaks restart
		// recovery, which degrades to the legacy send. Log and continue.
		st.sr.cfg.Logger.Warn("lark streaming: record card identity failed", "task_id", uuidString(st.taskID), "error", err)
	}
	return true
}

// ensureSplit upgrades a live single-element card to the split layout with
// one whole-card replace: the thinking window (in its own element) sits
// above the divider, the reply element carries over below. Lark renders the
// replace instantly; because the reply content is unchanged, the answer's
// typewriter is not disturbed.
func (st *streamState) ensureSplit(ctx context.Context, thinkContent string) bool {
	reply := st.flushedReply
	if st.placeholderShown {
		// The pending placeholder becomes the quiet answer-slot marker.
		reply = streamReplySlotPlaceholder
	}
	st.typewriterGrace()
	cardJSON, err := buildSplitCardJSON(thinkContent, reply)
	if err != nil {
		st.sr.cfg.Logger.Warn("lark streaming: build split card failed", "task_id", uuidString(st.taskID), "error", err)
		return false
	}
	if !st.replaceCard(ctx, cardJSON) {
		return false
	}
	st.split = true
	st.flushedThink = thinkContent
	st.flushedReply = reply
	return true
}

// putContent replaces one element's content (full text) and persists the
// accepted sequence. Failures leave the retry to the next flush, which
// re-renders from the live buffers — self-healing against transient errors.
func (st *streamState) putContent(ctx context.Context, elementID, content string) bool {
	// Budget guard: an over-cap push would be rejected by Feishu anyway;
	// the oversize branches own delivering the full text elsewhere. Lark
	// also rejects empty content, so nothing to push means no call.
	if content == "" || len(content) > st.sr.cfg.MaxCardBytes {
		return false
	}
	startSeq := st.seq
	st.seq++
	err := st.sr.cards.UpdateCardElementContent(ctx, UpdateCardElementContentParams{
		InstallationID: st.creds,
		CardID:         st.cardID,
		ElementID:      elementID,
		Content:        content,
		Sequence:       st.seq,
	})
	if isCardStreamingClosedError(err) {
		// Feishu auto-closed streaming mode (inactivity timeout). Reopen
		// and retry once, each consuming its own sequence.
		st.seq++
		if reopenErr := st.sr.cards.SetCardStreamingMode(ctx, SetCardStreamingModeParams{
			InstallationID: st.creds,
			CardID:         st.cardID,
			StreamingMode:  true,
			Sequence:       st.seq,
		}); reopenErr == nil {
			st.seq++
			err = st.sr.cards.UpdateCardElementContent(ctx, UpdateCardElementContentParams{
				InstallationID: st.creds,
				CardID:         st.cardID,
				ElementID:      elementID,
				Content:        content,
				Sequence:       st.seq,
			})
		}
	}
	if err != nil {
		// Roll the sequence back to where this attempt started: if the
		// call never landed the retry reuses the number, and if it
		// secretly did, the retry's "sequence not increasing" rejection
		// is indistinguishable from a stale close — the next full-content
		// push heals either way.
		st.seq = startSeq
		st.sr.cfg.Logger.Warn("lark streaming: content update failed",
			"task_id", uuidString(st.taskID), "card_id", st.cardID, "element_id", elementID, "error", err)
		return false
	}
	st.flushedElement(elementID, content)
	st.lastFlushAt = st.sr.cfg.Now()
	st.markActive()
	_ = st.sr.queries.UpdateLarkOutboundCardProgress(ctx, UpdateOutboundCardProgressParams{
		ID:           st.cardRowID,
		LastSequence: st.seq,
		Status:       string(CardStatusStreaming),
	})
	return true
}

// flushedElement records the accepted content of one element.
func (st *streamState) flushedElement(elementID, content string) {
	if elementID == streamThinkElementID {
		st.flushedThink = content
		return
	}
	st.flushedReply = content
}

// finalize settles the card on the authoritative reply alone: the chat:done
// content replaces the streamed approximation — dropping the thinking box,
// whose complete version lives on in the web transcript — then streaming
// mode closes and the row becomes final.
func (st *streamState) finalize(ctx context.Context, authoritative string) {
	buf := st.snapshot()
	st.mu.Lock()
	oversize := st.oversize
	st.mu.Unlock()

	content := authoritative
	if content == "" {
		// Empty authoritative content with streamed text: keep what the
		// user watched appear rather than blanking the card.
		content = buf
	}

	if st.cardID == "" {
		// The card was never created (persistent create failures). The
		// Patcher already skipped its send, so deliver the whole reply on
		// the legacy path rather than losing it.
		if content != "" {
			st.sr.legacySendForState(ctx, st, content)
		}
		_ = st.sr.queries.UpdateLarkOutboundCardStatus(ctx, UpdateOutboundCardStatusParams{ID: st.cardRowID, Status: string(CardStatusError)})
		return
	}

	// Re-check the budget here, not just the flush-time flag: a terminal
	// event can arrive before any tick flushed, and then nothing but this
	// branch knows the reply exceeds the card cap.
	full := prependMarkdownMention(st.mentionOpenID, content)
	if !oversize && len(full) > st.sr.cfg.MaxCardBytes {
		oversize = true
	}

	if oversize && authoritative != "" {
		// The card holds a truncated tail; the full reply follows on the
		// legacy path.
		truncated := prependMarkdownMention(st.mentionOpenID,
			truncateCardTail(authoritative, st.sr.cfg.MaxCardBytes))
		st.settleFinalCard(ctx, truncated)
		st.sr.legacySendForState(ctx, st, authoritative)
		return
	}

	st.settleFinalCard(ctx, full)
}

// settleFinalCard lands the terminal answer-only card. On the split layout
// one whole-card replace drops the thinking box and divider; the reply
// content carries over unchanged, so nothing re-animates. A failed (or
// unbuildable) replace degrades to updating the reply element in place,
// leaving the frozen thinking box visible above the answer.
func (st *streamState) settleFinalCard(ctx context.Context, finalReply string) {
	if finalReply != "" && st.split {
		st.typewriterGrace()
		if cardJSON, err := buildReplyCardJSON(finalReply); err == nil && st.replaceCard(ctx, cardJSON) {
			st.flushedReply = finalReply
			st.closeStreaming(ctx)
			st.persistProgress(ctx, CardStatusFinal)
			return
		}
	}
	if finalReply != "" && finalReply != st.flushedReply {
		st.putContent(ctx, streamElementID, finalReply)
	}
	st.closeStreaming(ctx)
	st.persistProgress(ctx, CardStatusFinal)
}

// replaceCard swaps the card's whole document for cardJSON, consuming one
// sequence like every card operation. It lets the live typewriter finish
// first when the caller graces: the replace re-renders instantly and would
// cut the animation short.
func (st *streamState) replaceCard(ctx context.Context, cardJSON string) bool {
	st.seq++
	err := st.sr.cards.UpdateCardEntity(ctx, UpdateCardEntityParams{
		InstallationID: st.creds,
		CardID:         st.cardID,
		CardJSON:       cardJSON,
		UUID:           uuid.New().String(),
		Sequence:       st.seq,
	})
	if err != nil {
		st.sr.cfg.Logger.Warn("lark streaming: card replace failed",
			"task_id", uuidString(st.taskID), "card_id", st.cardID, "error", err)
		return false
	}
	st.markActive()
	return true
}

// fail surfaces a short failure note on the card after the partial output,
// mirroring the Patcher's error card semantics (visual distinction for a
// failed run) without dropping what already streamed. The note rides the
// reply element; the thinking window (if any) keeps its last state above it.
func (st *streamState) fail(ctx context.Context, errMessage string) {
	partial := st.snapshot()
	if st.cardID == "" {
		if partial != "" {
			st.sr.legacySendForState(ctx, st, partial+streamFailureSuffix(errMessage))
		}
		_ = st.sr.queries.UpdateLarkOutboundCardStatus(ctx, UpdateOutboundCardStatusParams{ID: st.cardRowID, Status: string(CardStatusError)})
		return
	}
	reply := prependMarkdownMention(st.mentionOpenID,
		appendBodySuffix(partial, streamFailureSuffix(errMessage)))
	if reply != st.flushedReply {
		st.putContent(ctx, streamElementID, reply)
	}
	st.closeStreaming(ctx)
	st.persistProgress(ctx, CardStatusError)
}

// settle closes streaming mode — the cancellation path: channel outbounds
// stay silent on cancellation, but a card already on screen must not keep
// showing a live typewriter forever. A cancelled run that never produced an
// answer would otherwise leave the thinking preview reading as in-progress
// work, so the reply slot states the cancellation instead.
func (st *streamState) settle(ctx context.Context) {
	if st.cardID == "" {
		_ = st.sr.queries.UpdateLarkOutboundCardStatus(ctx, UpdateOutboundCardStatusParams{ID: st.cardRowID, Status: string(CardStatusFinal)})
		return
	}
	thinking, answer := st.snapshotBoth()
	if answer == "" && (thinking != "" || st.placeholderShown) {
		cancelled := prependMarkdownMention(st.mentionOpenID, appendBodySuffix("", streamCancelSuffix))
		if cancelled != st.flushedReply {
			st.putContent(ctx, streamElementID, cancelled)
		}
	}
	st.closeStreaming(ctx)
	st.persistProgress(ctx, CardStatusFinal)
}

// settleWithContent is settle preceded by the authoritative content, used by
// the restart-recovery path where nothing else will update the card. The
// recovered worker does not know the card's layout, so it targets the reply
// element — present in both layouts; a split card keeps its frozen thinking
// box, an acceptable degradation for restart recovery.
func (st *streamState) settleWithContent(ctx context.Context, content string) {
	if content != "" {
		st.settleFinalCard(ctx, prependMarkdownMention(st.mentionOpenID, content))
		return
	}
	st.settle(ctx)
}

// typewriterGrace waits out the remainder of one flush window after the
// last accepted push: with the delay strategy the client may still be
// typing that delta out, and settling or replacing the card first would
// cut the animation short. The cap bounds the wait when FlushInterval
// itself is huge.
func (st *streamState) typewriterGrace() {
	if st.lastFlushAt.IsZero() {
		return
	}
	wait := st.sr.cfg.FlushInterval - st.sr.cfg.Now().Sub(st.lastFlushAt)
	if wait <= 0 {
		return
	}
	if wait > streamCloseGraceCap {
		wait = streamCloseGraceCap
	}
	time.Sleep(wait)
}

func (st *streamState) closeStreaming(ctx context.Context) {
	st.typewriterGrace()
	st.seq++
	err := st.sr.cards.SetCardStreamingMode(ctx, SetCardStreamingModeParams{
		InstallationID: st.creds,
		CardID:         st.cardID,
		StreamingMode:  false,
		Sequence:       st.seq,
	})
	if err != nil && !isCardStreamingClosedError(err) {
		// Already-closed is the desired state; anything else leaves the
		// card streaming until Feishu's own timeout closes it.
		st.sr.cfg.Logger.Warn("lark streaming: close streaming failed",
			"task_id", uuidString(st.taskID), "card_id", st.cardID, "error", err)
	}
	st.markActive()
}

func (st *streamState) persistProgress(ctx context.Context, status CardStatus) {
	_ = st.sr.queries.UpdateLarkOutboundCardProgress(ctx, UpdateOutboundCardProgressParams{
		ID:           st.cardRowID,
		LastSequence: st.seq,
		Status:       string(status),
	})
}

// ---------------------------------------------------------------- helpers

// legacySendForState delivers a whole reply on the legacy send path (see
// legacySend) using the routing the stream already resolved.
func (s *StreamingReplier) legacySendForState(ctx context.Context, st *streamState, content string) {
	s.legacySend(ctx, st.creds, st.mentionOpenID, st.chatID, st.target, content)
}

// legacySendForBinding is the fallback whole-reply send for the
// restart-recovery path, re-deriving routing from the delivery binding.
func (s *StreamingReplier) legacySendForBinding(ctx context.Context, taskID pgtype.UUID, binding ChatSessionBinding, content string) {
	if content == "" || s.client == nil {
		return
	}
	creds := s.credsForInstallation(ctx, binding.InstallationID)
	s.legacySend(ctx, creds, mentionOpenID(binding), outboundChatID(binding), threadReplyTarget(binding), content)
}

// legacySend delivers a whole reply outside the streaming card (markdown
// card vs plain text, mention prefix, classified reply fallback).
func (s *StreamingReplier) legacySend(ctx context.Context, creds InstallationCredentials, mention string, chatID ChatID, target ReplyTarget, content string) {
	if content == "" || s.client == nil {
		return
	}
	if containsMarkdown(content) {
		markdown := prependMarkdownMention(mention, content)
		_ = sendWithReplyFallback(s.cfg.Logger, "lark streaming fallback markdown", target, func(t ReplyTarget) error {
			_, err := s.client.SendMarkdownCard(ctx, SendMarkdownCardParams{
				InstallationID: creds,
				ChatID:         chatID,
				Markdown:       markdown,
				ReplyTarget:    t,
			})
			return err
		})
		return
	}
	text := prependTextMention(mention, content)
	_ = sendWithReplyFallback(s.cfg.Logger, "lark streaming fallback text", target, func(t ReplyTarget) error {
		_, err := s.client.SendTextMessage(ctx, SendTextParams{
			InstallationID: creds,
			ChatID:         chatID,
			Text:           text,
			ReplyTarget:    t,
		})
		return err
	})
}

func (s *StreamingReplier) credsForInstallation(ctx context.Context, installationID pgtype.UUID) InstallationCredentials {
	inst, err := s.queries.GetLarkInstallation(ctx, installationID)
	if err != nil {
		return InstallationCredentials{}
	}
	secret, err := s.credentials.DecryptAppSecret(inst)
	if err != nil {
		return InstallationCredentials{}
	}
	creds := InstallationCredentials{AppID: inst.AppID, AppSecret: secret, Region: RegionOrDefault(inst.Region)}
	if inst.TenantKey.Valid {
		creds.TenantKey = inst.TenantKey.String
	}
	return creds
}

// forgetStream drops an idle stream from the manager map so a late terminal
// event recovers the card from the claimed row instead of signaling a dead
// worker.
func (s *StreamingReplier) forgetStream(st *streamState) {
	key := uuidString(st.taskID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams[key] == st {
		delete(s.streams, key)
	}
}

// rememberForeign caches a task as not-ours, bounded by TTL and size.
func (s *StreamingReplier) rememberForeign(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.foreign) > 1024 {
		now := s.cfg.Now()
		for k, at := range s.foreign {
			if now.Sub(at) > streamForeignTTL {
				delete(s.foreign, k)
			}
		}
	}
	s.foreign[key] = s.cfg.Now()
}

func bindingFromDelivery(delivery db.ChannelTaskDelivery) ChatSessionBinding {
	return ChatSessionBinding{
		ID: delivery.BindingID, InstallationID: delivery.InstallationID,
		ChannelChatID: delivery.ChannelChatID, ChatType: delivery.ChatType, Config: delivery.Config,
		LastMessageID: delivery.ChannelMessageID, LastThreadID: delivery.ChannelThreadID,
		LastSenderID: delivery.ChannelSenderID,
	}
}

// taskMessageFrame classifies a task:message event payload. Text frames
// carry the assistant's answer; thinking frames carry its reasoning, which
// the card previews live; everything else (tool_use, tool_result) stays in
// the web transcript only.
func taskMessageFrame(payload any) (frameKind, string) {
	var p protocol.TaskMessagePayload
	switch v := payload.(type) {
	case protocol.TaskMessagePayload:
		p = v
	case map[string]any:
		p.Type, _ = v["type"].(string)
		p.Content, _ = v["content"].(string)
	default:
		return frameOther, ""
	}
	switch p.Type {
	case streamTextType:
		return frameText, p.Content
	case streamThinkType:
		return frameThinking, p.Content
	case "tool_use", "tool_result":
		return frameTool, ""
	default:
		return frameOther, ""
	}
}

// isCardStreamingClosedError reports a cardkit error meaning streaming mode
// is currently off server-side (auto-timeout or already closed).
func isCardStreamingClosedError(err error) bool {
	code := larkErrorCode(err)
	return code == codeCardStreamingClosed || code == codeCardStreamingTimeout
}

// streamFailureSuffix is the failure marker appended to a streamed card;
// appendBodySuffix strips its divider when there is no partial answer to
// follow.
func streamFailureSuffix(errMessage string) string {
	if errMessage == "" {
		return "\n\n---\n\n⚠️ 运行失败。"
	}
	return "\n\n---\n\n⚠️ 运行失败：" + errMessage
}

// truncateCardTail cuts content to fit the card budget and ends on a note
// that the full reply follows in the next message. The cut lands on a rune
// boundary and skips trailing whitespace; the legacy message carries the
// authoritative text, so a mid-markup cut only ever affects the card view.
func truncateCardTail(content string, maxBytes int) string {
	const suffix = "\n\n…（内容超出卡片大小限制，完整回复见下一条消息）"
	budget := maxBytes - len(suffix)
	if budget <= 0 || len(content) <= budget {
		return content
	}
	cut := content[:budget]
	for len(cut) > 0 && !isUTF8Start(cut[len(cut)-1]) {
		cut = cut[:len(cut)-1]
	}
	cut = strings.TrimRight(cut, " \n")
	return cut + suffix
}

func isUTF8Start(b byte) bool { return b&0xC0 != 0x80 }

// streamThinkLabel heads the live thinking preview; streamCancelSuffix marks
// a run cancelled before any answer; streamPendingPlaceholder holds the
// screen between task start and the first frame; streamReplySlotPlaceholder
// marks the split card's answer slot while only reasoning has arrived.
const (
	streamThinkLabel           = "Thinking"
	streamCancelSuffix         = "\n\n---\n\n_已取消_"
	streamPendingPlaceholder   = "Thinking…"
	streamReplySlotPlaceholder = "…"
)

// appendBodySuffix joins a suffix onto a possibly-empty body without leaving
// a leading divider.
func appendBodySuffix(body, suffix string) string {
	if body == "" {
		return strings.TrimPrefix(suffix, "\n\n---\n\n")
	}
	return body + suffix
}

// quoteBlock turns text into a markdown quote, one "> " per line.
func quoteBlock(text string) string {
	return "> " + strings.ReplaceAll(text, "\n", "\n> ")
}

// renderThinkWindow renders the thinking preview and the rune anchor its
// content starts from. The window grows append-only until it passes the
// high-water mark; then the anchor jumps so the render shows the latest
// low-water runes under an ellipsis marker. A growth render always extends
// the previous one — the precondition for Feishu's typewriter to animate
// only the delta — while a reset render changes the head on purpose: Lark
// puts non-append updates on screen instantly, which reads as the window
// scrolling forward. The anchor only ever advances past committed pushes.
func renderThinkWindow(thinking string, anchor int) (content string, next int) {
	runes := []rune(thinking)
	if len(runes)-anchor > streamThinkWindowHigh {
		anchor = len(runes) - streamThinkWindowLow
	}
	window := string(runes[anchor:])
	if anchor > 0 {
		window = "…\n\n" + window
	}
	return "**" + streamThinkLabel + "**\n\n" + quoteBlock(window), anchor
}

// buildReplyCardJSON renders the single-element answer card: the shape a
// stream opens on without reasoning and the shape every finalize settles
// into.
func buildReplyCardJSON(content string) (string, error) {
	return buildStreamingCardJSON([]any{
		map[string]any{
			"tag":        "markdown",
			"content":    content,
			"element_id": streamElementID,
		},
	})
}

// buildSplitCardJSON renders the live reasoning layout: the thinking window,
// a divider, and the reply element below it. Both markdown elements carry
// their own element_id so content updates address them independently — the
// precondition for each region growing without rewriting the other.
func buildSplitCardJSON(thinkContent, replyContent string) (string, error) {
	return buildStreamingCardJSON([]any{
		map[string]any{
			"tag":        "markdown",
			"content":    thinkContent,
			"element_id": streamThinkElementID,
		},
		map[string]any{"tag": "hr"},
		map[string]any{
			"tag":        "markdown",
			"content":    replyContent,
			"element_id": streamElementID,
		},
	})
}

// buildStreamingCardJSON renders the cardkit card document: schema 2.0,
// streaming mode on, and the given body elements.
//
// Typewriter pacing is tuned to generation speed, not to Feishu's 70 ms x 1
// default (~14 chars/s): agent models emit hundreds of characters a second,
// and pacing below the generation rate makes the card fall behind from the
// first flush. With the delay strategy the client renders every update in
// arrival order at a fixed pace — a faster-than-generation pace keeps the
// typewriter continuously busy and the card tracks the agent in near real
// time. (The fast strategy instead dumps any un-rendered backlog the moment
// a new update lands, which reads as bursts interrupting the typing.)
func buildStreamingCardJSON(elements []any) (string, error) {
	doc := map[string]any{
		"schema": "2.0",
		"config": map[string]any{
			"streaming_mode": true,
			"streaming_config": map[string]any{
				// 30 ms x 10 chars = ~333 chars/s: at or above typical agent
				// output speed, so the typewriter never lags the buffer. The
				// frequency stays inside the range of Feishu's own examples.
				"print_frequency_ms": map[string]int{"default": 30, "android": 30, "ios": 30, "pc": 30},
				"print_step":         map[string]int{"default": 10, "android": 10, "ios": 10, "pc": 10},
				"print_strategy":     "delay",
			},
		},
		"body": map[string]any{
			"elements": elements,
		},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
