package lark

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

var (
	streamTaskID    = mustUUID("11111111-1111-1111-1111-111111111111")
	streamSessionID = mustUUID("22222222-2222-2222-2222-222222222222")
	streamInstID    = mustUUID("44444444-4444-4444-4444-444444444444")
	streamCardRowID = mustUUID("55555555-5555-5555-5555-555555555555")
)

// fakeStreamQueries is the StreamingQueries seam for unit tests. The happy
// path resolves one Feishu-bound chat task; per-field errors force the
// degradation branches.
type fakeStreamQueries struct {
	mu              sync.Mutex
	task            db.AgentTaskQueue
	taskErr         error
	delivery        db.ChannelTaskDelivery
	deliveryErr     error
	installation    Installation
	installationErr error
	channelIngested bool

	created    []CreateOutboundCardMessageParams
	identities []SetOutboundCardIdentityParams
	progress   []UpdateOutboundCardProgressParams
	statuses   []UpdateOutboundCardStatusParams

	// cardRow simulates the durable claim recoverTerminal reads back.
	cardRow OutboundCardMessage
	cardErr error
}

func (f *fakeStreamQueries) GetAgentTask(ctx context.Context, id pgtype.UUID) (db.AgentTaskQueue, error) {
	return f.task, f.taskErr
}
func (f *fakeStreamQueries) TaskHasChannelIngestedMessages(ctx context.Context, taskID pgtype.UUID) (bool, error) {
	return f.channelIngested, nil
}
func (f *fakeStreamQueries) GetChannelTaskDelivery(ctx context.Context, taskID pgtype.UUID) (db.ChannelTaskDelivery, error) {
	return f.delivery, f.deliveryErr
}
func (f *fakeStreamQueries) GetLarkInstallation(ctx context.Context, id pgtype.UUID) (Installation, error) {
	return f.installation, f.installationErr
}
func (f *fakeStreamQueries) GetLarkOutboundCardByTask(ctx context.Context, taskID pgtype.UUID) (OutboundCardMessage, error) {
	if f.cardErr != nil {
		return OutboundCardMessage{}, f.cardErr
	}
	// Mirror Postgres: no configured row means ErrNoRows, not a zero row —
	// openStream distinguishes "previous attempt claimed the card" from
	// "nothing claimed yet" by exactly this error.
	if !f.cardRow.ID.Valid {
		return OutboundCardMessage{}, pgx.ErrNoRows
	}
	return f.cardRow, nil
}
func (f *fakeStreamQueries) CreateLarkOutboundCardMessage(ctx context.Context, arg CreateOutboundCardMessageParams) (OutboundCardMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, arg)
	return OutboundCardMessage{ID: streamCardRowID, Status: arg.Status}, nil
}
func (f *fakeStreamQueries) SetLarkOutboundCardIdentity(ctx context.Context, arg SetOutboundCardIdentityParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.identities = append(f.identities, arg)
	return nil
}
func (f *fakeStreamQueries) UpdateLarkOutboundCardProgress(ctx context.Context, arg UpdateOutboundCardProgressParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.progress = append(f.progress, arg)
	return nil
}
func (f *fakeStreamQueries) UpdateLarkOutboundCardStatus(ctx context.Context, arg UpdateOutboundCardStatusParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, arg)
	return nil
}

// fakeStreamingAPIClient implements CardStreamingClient and, through the
// embedded APIClient, the two legacy sends the fallback path uses. Any other
// APIClient method hits the nil embedded interface and fails the test.
type fakeStreamingAPIClient struct {
	APIClient
	mu sync.Mutex

	cardsCreated  []CreateCardEntityParams
	cardMessages  []SendCardMessageParams
	contentCalls  []UpdateCardElementContentParams
	settingsCalls []SetCardStreamingModeParams
	cardReplaces  []UpdateCardEntityParams
	mdCards       []SendMarkdownCardParams
	texts         []SendTextParams

	createErr   error
	sendErr     error
	contentErrs []error // consumed per content call, nil once exhausted
	replaceErr  error
}

func (f *fakeStreamingAPIClient) CreateCardEntity(ctx context.Context, p CreateCardEntityParams) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cardsCreated = append(f.cardsCreated, p)
	if f.createErr != nil {
		return "", f.createErr
	}
	return "card_1", nil
}

func (f *fakeStreamingAPIClient) SendCardMessage(ctx context.Context, p SendCardMessageParams) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cardMessages = append(f.cardMessages, p)
	if f.sendErr != nil {
		return "", f.sendErr
	}
	return "om_card", nil
}

func (f *fakeStreamingAPIClient) UpdateCardElementContent(ctx context.Context, p UpdateCardElementContentParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.contentCalls = append(f.contentCalls, p)
	var err error
	if len(f.contentErrs) > 0 {
		err, f.contentErrs = f.contentErrs[0], f.contentErrs[1:]
	}
	return err
}

func (f *fakeStreamingAPIClient) SetCardStreamingMode(ctx context.Context, p SetCardStreamingModeParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settingsCalls = append(f.settingsCalls, p)
	return nil
}

func (f *fakeStreamingAPIClient) UpdateCardEntity(ctx context.Context, p UpdateCardEntityParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cardReplaces = append(f.cardReplaces, p)
	return f.replaceErr
}

func (f *fakeStreamingAPIClient) SendMarkdownCard(ctx context.Context, p SendMarkdownCardParams) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mdCards = append(f.mdCards, p)
	return "om_md", nil
}

func (f *fakeStreamingAPIClient) SendTextMessage(ctx context.Context, p SendTextParams) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.texts = append(f.texts, p)
	return "om_text", nil
}

func (f *fakeStreamingAPIClient) lastContent() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastContentLocked()
}

func (f *fakeStreamingAPIClient) lastContentLocked() string {
	if len(f.contentCalls) == 0 {
		return ""
	}
	return f.contentCalls[len(f.contentCalls)-1].Content
}

// lastContentFor returns the latest pushed content of one element.
func (f *fakeStreamingAPIClient) lastContentFor(elementID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.contentCalls) - 1; i >= 0; i-- {
		if f.contentCalls[i].ElementID == elementID {
			return f.contentCalls[i].Content
		}
	}
	return ""
}

func (f *fakeStreamingAPIClient) settled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.settledLocked()
}

func (f *fakeStreamingAPIClient) settledLocked() bool {
	return len(f.settingsCalls) > 0 &&
		!f.settingsCalls[len(f.settingsCalls)-1].StreamingMode
}

// newStreamEnv wires a streaming replier against a Feishu-bound p2p task.
func newStreamEnv(t *testing.T, mutate func(q *fakeStreamQueries, api *fakeStreamingAPIClient)) (*StreamingReplier, *fakeStreamingAPIClient, *fakeStreamQueries) {
	t.Helper()
	q := &fakeStreamQueries{
		task: db.AgentTaskQueue{
			ID:            streamTaskID,
			ChatSessionID: streamSessionID,
		},
		delivery: db.ChannelTaskDelivery{
			BindingID:        mustUUID("33333333-3333-3333-3333-333333333333"),
			InstallationID:   streamInstID,
			ChannelType:      channelTypeFeishu,
			ChannelChatID:    "oc_chat",
			ChatType:         string(ChatTypeP2P),
			ChannelMessageID: pgtype.Text{String: "om_trigger", Valid: true},
		},
		installation: Installation{
			ID:     streamInstID,
			Status: string(InstallationActive),
			AppID:  "cli_test",
		},
	}
	api := &fakeStreamingAPIClient{}
	if mutate != nil {
		mutate(q, api)
	}
	s := NewStreamingReplier(q, fakeCredentials{"secret"}, api, StreamingConfig{
		FlushInterval: 5 * time.Millisecond,
		Logger:        newDiscardLogger(),
	})
	return s, api, q
}

func textEvent(content string) events.Event {
	return events.Event{
		Type:    protocol.EventTaskMessage,
		TaskID:  uuidString(streamTaskID),
		Payload: protocol.TaskMessagePayload{TaskID: uuidString(streamTaskID), Type: "text", Content: content},
	}
}

func thinkingEvent(content string) events.Event {
	return events.Event{
		Type:    protocol.EventTaskMessage,
		TaskID:  uuidString(streamTaskID),
		Payload: protocol.TaskMessagePayload{TaskID: uuidString(streamTaskID), Type: "thinking", Content: content},
	}
}

func doneEvent(content string) events.Event {
	return events.Event{
		Type:   protocol.EventChatDone,
		TaskID: uuidString(streamTaskID),
		Payload: protocol.ChatDonePayload{
			TaskID:        uuidString(streamTaskID),
			ChatSessionID: uuidString(streamSessionID),
			Content:       content,
		},
	}
}

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// waitForCard blocks until the stream's card entity exists. Tests gate on
// this milestone before sending more events, because the FIRST frame rides
// inside the created card (no separate content update) and a terminal event
// that wins the race against card creation takes the legacy fallback path.
func waitForCard(t *testing.T, api *fakeStreamingAPIClient) {
	t.Helper()
	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.cardsCreated) == 1
	}, "streaming card created and delivered")
}

func TestStreamingHappyPathCreatesCardAndFinalizes(t *testing.T) {
	s, api, q := newStreamEnv(t, nil)

	s.handleEvent(textEvent("Hello"))
	waitForCard(t, api)
	s.handleEvent(textEvent(" world"))

	eventually(t, func() bool { return api.lastContent() == "Hello world" },
		"consecutive same-kind frames join as one continuous block")

	api.mu.Lock()
	if !strings.Contains(api.cardsCreated[0].CardJSON, "Hello") {
		t.Fatalf("card should embed the first frame: %s", api.cardsCreated[0].CardJSON)
	}
	// p2p chats send at the chat level — no reply threading (a 1:1 has no
	// ambiguity for a quote to resolve; see threadReplyTarget).
	if len(api.cardMessages) != 1 || api.cardMessages[0].ChatID != "oc_chat" || api.cardMessages[0].ReplyTarget.IsSet() {
		t.Fatalf("card delivered once at chat level: %+v", api.cardMessages)
	}
	api.mu.Unlock()

	s.handleEvent(doneEvent("**Done** final answer"))

	eventually(t, func() bool { return api.settled() && api.lastContent() == "**Done** final answer" },
		"final update with the authoritative reply and streaming closed")

	api.mu.Lock()
	defer api.mu.Unlock()
	// Sequences strictly increase across every accepted cardkit call.
	last := int32(0)
	for _, c := range api.contentCalls {
		if c.Sequence <= last {
			t.Fatalf("content sequence not increasing: %d after %d", c.Sequence, last)
		}
		last = c.Sequence
	}
	for _, st := range api.settingsCalls {
		if st.Sequence <= last {
			t.Fatalf("settings sequence not increasing: %d after %d", st.Sequence, last)
		}
		last = st.Sequence
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.identities) != 1 || q.identities[0].CardEntityID != "card_1" {
		t.Fatalf("card identity not recorded: %+v", q.identities)
	}
	if len(q.progress) == 0 || q.progress[len(q.progress)-1].Status != string(CardStatusFinal) {
		t.Fatalf("row not finalized: %+v", q.progress)
	}
}

// TestStreamingDeliversCardWithoutWaitingForFlushInterval pins the fast
// first paint: the worker flushes the moment it starts, so the card goes out
// carrying the opening frame — no flush tick (let alone a full FlushInterval
// of buffered frames) may sit between the first text and the card appearing.
func TestStreamingDeliversCardWithoutWaitingForFlushInterval(t *testing.T) {
	s, api, _ := newStreamEnv(t, nil)
	s.cfg.FlushInterval = time.Hour // no tick can fire during the test

	s.handleEvent(textEvent("opening"))

	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.cardsCreated) == 1 && strings.Contains(api.cardsCreated[0].CardJSON, "opening")
	}, "card created from the opening frame with zero flush ticks")

	// The terminal op rides the op channel, so the lifecycle completes
	// without any tick ever firing.
	s.handleEvent(doneEvent("opening\n\nrest"))

	eventually(t, func() bool { return api.settled() && api.lastContent() == "opening\n\nrest" },
		"finalize settles the card without a flush tick")
}

// TestStreamingThinkingPreviewDropsAtFinalize pins the thinking display:
// the card opens on the first thinking frame in the split layout (window,
// divider, answer slot), the answer streams into the reply element, and
// finalize replaces the card with the answer alone — no thinking header,
// window, or divider remains.
func TestStreamingThinkingPreviewDropsAtFinalize(t *testing.T) {
	s, api, _ := newStreamEnv(t, nil)

	s.handleEvent(thinkingEvent("先分析问题"))
	waitForCard(t, api)

	api.mu.Lock()
	embedded := api.cardsCreated[0].CardJSON
	api.mu.Unlock()
	if !strings.Contains(embedded, "Thinking") || !strings.Contains(embedded, "先分析问题") {
		t.Fatalf("card should open with the thinking preview: %s", embedded)
	}
	if !strings.Contains(embedded, streamReplySlotPlaceholder) {
		t.Fatalf("split card should carry the answer-slot placeholder: %s", embedded)
	}

	s.handleEvent(textEvent("答案"))
	eventually(t, func() bool { return api.lastContentFor(streamElementID) == "答案" },
		"answer streams into the reply element")

	s.handleEvent(doneEvent("答案（完整）"))

	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return api.settledLocked() && len(api.cardReplaces) == 1
	}, "finalize replaces the card with the answer-only layout")

	api.mu.Lock()
	defer api.mu.Unlock()
	final := replyContentFromCardJSON(t, api.cardReplaces[0].CardJSON)
	if final != "答案（完整）" {
		t.Fatalf("final card must carry the authoritative answer, got %q", final)
	}
	if strings.Contains(api.cardReplaces[0].CardJSON, streamThinkElementID) || strings.Contains(api.cardReplaces[0].CardJSON, "先分析问题") {
		t.Fatalf("final card must drop the thinking window: %s", api.cardReplaces[0].CardJSON)
	}
}

// TestStreamingThinkingAfterAnswerNeverRewritesAnswer pins the fix for the
// interleave twitch: reasoning that arrives AFTER answer text has streamed
// must never rewrite the answer region. Lark renders any non-append content
// update instantly, which used to re-render the whole single-element card on
// every flush; with the split layout the thinking window lives in its own
// element and the reply element only ever receives prefix-extensions.
func TestStreamingThinkingAfterAnswerNeverRewritesAnswer(t *testing.T) {
	s, api, _ := newStreamEnv(t, nil)

	s.handleEvent(textEvent("先说结论"))
	waitForCard(t, api)
	s.handleEvent(thinkingEvent("现在开始思考细节"))
	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.cardReplaces) == 1
	}, "card upgrades to the split layout when reasoning follows text")

	api.mu.Lock()
	upgrade := api.cardReplaces[0].CardJSON
	api.mu.Unlock()
	if !strings.Contains(upgrade, streamThinkElementID) || !strings.Contains(upgrade, "先说结论") {
		t.Fatalf("upgrade must install the thinking window and carry the answer over: %s", upgrade)
	}

	s.handleEvent(thinkingEvent("，继续思考"))
	s.handleEvent(textEvent("，然后补充"))
	s.handleEvent(textEvent("更多"))
	s.handleEvent(doneEvent("先说结论，然后补充更多"))

	eventually(t, func() bool { return api.settled() }, "finalized")

	api.mu.Lock()
	defer api.mu.Unlock()
	var last string
	for _, c := range api.contentCalls {
		switch c.ElementID {
		case streamElementID:
			if last != "" && !strings.HasPrefix(c.Content, last) {
				t.Fatalf("reply element rewritten mid-stream (twitch):\n  %q\n→ %q", last, c.Content)
			}
			last = c.Content
			if strings.Contains(c.Content, "思考") || strings.Contains(c.Content, streamThinkLabel) {
				t.Fatalf("reply element must never carry thinking: %q", c.Content)
			}
		case streamThinkElementID:
			if strings.Contains(c.Content, "先说结论") {
				t.Fatalf("thinking element must never carry the answer: %q", c.Content)
			}
		}
	}
	if len(api.cardReplaces) != 2 {
		t.Fatalf("want upgrade + final replaces, got %d", len(api.cardReplaces))
	}
	final := api.cardReplaces[len(api.cardReplaces)-1].CardJSON
	if !strings.Contains(final, "先说结论，然后补充更多") || strings.Contains(final, streamThinkElementID) {
		t.Fatalf("final card must be the answer alone: %s", final)
	}
}

// TestStreamingThinkingWindowScrolls pins the liveness contract: past the
// high-water mark the preview keeps moving — the window resets to the latest
// runes instead of freezing at a cap.
func TestStreamingThinkingWindowScrolls(t *testing.T) {
	s, api, _ := newStreamEnv(t, nil)

	s.handleEvent(thinkingEvent(strings.Repeat("H", streamThinkWindowHigh)))
	waitForCard(t, api)
	s.handleEvent(thinkingEvent(strings.Repeat("T", streamThinkWindowHigh) + "尾"))

	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		for _, c := range api.contentCalls {
			if c.ElementID == streamThinkElementID &&
				strings.Contains(c.Content, "尾") &&
				strings.Contains(c.Content, "…") &&
				!strings.Contains(c.Content, "H") {
				return true
			}
		}
		return false
	}, "window scrolls to the latest reasoning after overflow")
}

// TestStreamingCancelDuringThinkingMarksCard: a run cancelled before any
// answer must not leave the thinking preview reading as in-progress work.
func TestStreamingCancelDuringThinkingMarksCard(t *testing.T) {
	s, api, _ := newStreamEnv(t, nil)

	s.handleEvent(thinkingEvent("正在想"))
	waitForCard(t, api)
	s.handleEvent(events.Event{Type: protocol.EventTaskCancelled, TaskID: uuidString(streamTaskID)})

	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return api.settledLocked() && strings.Contains(api.lastContentLocked(), "已取消")
	}, "cancel marker on the thinking-only card")
}

// TestStreamingOpensCardAtTaskRunning pins the fast first paint: the card
// opens on task:running with a pending placeholder, before any frame exists,
// so the user is not left in silence while the agent warms up.
func TestStreamingOpensCardAtTaskRunning(t *testing.T) {
	s, api, _ := newStreamEnv(t, nil)

	s.handleEvent(events.Event{Type: protocol.EventTaskRunning, TaskID: uuidString(streamTaskID)})

	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.cardsCreated) == 1 && strings.Contains(api.cardsCreated[0].CardJSON, streamPendingPlaceholder)
	}, "card opens at task start with the pending placeholder")

	s.handleEvent(thinkingEvent("开始想"))
	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.cardReplaces) == 1 && strings.Contains(api.cardReplaces[0].CardJSON, "开始想")
	}, "first thinking frame upgrades the placeholder card to the split layout")
}

// TestStreamingFramesJoinByDaemonSealing mirrors the daemon reporter's frame
// semantics: consecutive same-type frames are chunks of one continuous block
// (no separator — a separator breaks sentences mid-word), while a frame after
// a tool boundary opens a new paragraph.
func TestStreamingFramesJoinByDaemonSealing(t *testing.T) {
	s, api, _ := newStreamEnv(t, nil)

	s.handleEvent(textEvent("The"))
	waitForCard(t, api)
	s.handleEvent(textEvent(" user asks"))
	eventually(t, func() bool { return api.lastContent() == "The user asks" },
		"same-kind frames concatenate directly")

	s.handleEvent(events.Event{
		Type:    protocol.EventTaskMessage,
		TaskID:  uuidString(streamTaskID),
		Payload: protocol.TaskMessagePayload{TaskID: uuidString(streamTaskID), Type: "tool_result"},
	})
	s.handleEvent(textEvent("New paragraph."))
	eventually(t, func() bool { return api.lastContent() == "The user asks\n\nNew paragraph." },
		"post-tool frame opens a new paragraph")
}

func TestStreamingGroupMentionPrefixesContent(t *testing.T) {
	s, api, _ := newStreamEnv(t, func(q *fakeStreamQueries, _ *fakeStreamingAPIClient) {
		q.delivery.ChatType = string(ChatTypeGroup)
		q.delivery.ChannelSenderID = pgtype.Text{String: "ou_sender", Valid: true}
	})

	s.handleEvent(textEvent("answer"))
	waitForCard(t, api)
	s.handleEvent(doneEvent("answer **complete**"))

	eventually(t, func() bool {
		return api.settled() && api.lastContent() == `<at id=ou_sender></at> answer **complete**`
	}, "group content carries the mention prefix")
}

func TestStreamingTaskFailedAppendsFailureNote(t *testing.T) {
	s, api, q := newStreamEnv(t, nil)

	s.handleEvent(textEvent("partial"))
	waitForCard(t, api)
	s.handleEvent(events.Event{
		Type:    protocol.EventTaskFailed,
		TaskID:  uuidString(streamTaskID),
		Payload: map[string]any{"task_id": uuidString(streamTaskID), "error": "boom"},
	})

	eventually(t, func() bool {
		return api.settled() && strings.Contains(api.lastContent(), "运行失败：boom")
	}, "failure note on the card")

	q.mu.Lock()
	defer q.mu.Unlock()
	if got := q.progress[len(q.progress)-1].Status; got != string(CardStatusError) {
		t.Fatalf("row status should be error, got %q", got)
	}
}

func TestStreamingCancelSettlesWithoutContentChange(t *testing.T) {
	s, api, _ := newStreamEnv(t, nil)

	s.handleEvent(textEvent("partial"))
	waitForCard(t, api)
	s.handleEvent(textEvent(" more"))
	eventually(t, func() bool { return api.lastContent() == "partial more" }, "second frame flushed")

	s.handleEvent(events.Event{Type: protocol.EventTaskCancelled, TaskID: uuidString(streamTaskID)})

	eventually(t, func() bool { return api.settled() }, "streaming closed on cancel")
	if got := api.lastContent(); got != "partial more" {
		t.Fatalf("cancel must not alter content, got %q", got)
	}
}

func TestStreamingOversizeTruncatesCardAndSendsLegacy(t *testing.T) {
	long := "**加粗** " + strings.Repeat("字", 400) // markdown + 1200+ bytes
	s, api, q := newStreamEnv(t, nil)
	s.cfg.MaxCardBytes = 500

	s.handleEvent(textEvent("start")) // creates the card
	waitForCard(t, api)

	s.handleEvent(textEvent(long)) // pushes past the cap
	s.handleEvent(doneEvent("start\n\n" + long))

	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.mdCards) == 1
	}, "legacy markdown fallback sent")

	api.mu.Lock()
	if !strings.Contains(api.mdCards[0].Markdown, long) {
		api.mu.Unlock()
		t.Fatal("legacy fallback should carry the full reply")
	}
	api.mu.Unlock()

	eventually(t, func() bool {
		last := api.lastContent()
		return strings.Contains(last, "完整回复见下一条消息") && len(last) <= 500
	}, "card holds a truncated tail with a pointer within the byte cap")

	q.mu.Lock()
	defer q.mu.Unlock()
	if got := q.progress[len(q.progress)-1].Status; got != string(CardStatusFinal) {
		t.Fatalf("oversize finalize should end final, got %q", got)
	}
}

// TestStreamingOversizeWithThinkingDropsPreview pins the oversize finalize
// path with thinking on screen: the whole-card replace lands a truncated,
// answer-only reply that fits MaxCardBytes (thinking drops away like any
// finalize), and the full reply rides the legacy path.
func TestStreamingOversizeWithThinkingDropsPreview(t *testing.T) {
	s, api, _ := newStreamEnv(t, nil)
	s.cfg.MaxCardBytes = 2000

	s.handleEvent(thinkingEvent(strings.Repeat("思", 100)))
	waitForCard(t, api)
	s.handleEvent(doneEvent(strings.Repeat("字", 1000)))

	// The plain-字 reply carries no markdown, so the legacy fallback sends
	// it as text; waiting on the sends themselves (not settled(), which
	// lands around them) keeps the assertions race-free.
	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return api.settledLocked() && len(api.texts) == 1 && len(api.cardReplaces) == 1
	}, "oversize finalize settles, replaces the card, and sends the legacy reply")

	api.mu.Lock()
	defer api.mu.Unlock()
	final := replyContentFromCardJSON(t, api.cardReplaces[0].CardJSON)
	if len(final) > s.cfg.MaxCardBytes {
		t.Fatalf("truncated card exceeds cap: %d bytes", len(final))
	}
	if !strings.Contains(final, "完整回复见下一条消息") {
		t.Fatalf("truncated answer should carry the pointer suffix: %q", final[:min(60, len(final))])
	}
	if strings.Contains(final, "Thinking") || strings.Contains(final, "思") {
		t.Fatalf("oversize finalize must drop the thinking window, got %q", final[:min(60, len(final))])
	}
}

func TestStreamingCreateFailureFallsBackToLegacyReply(t *testing.T) {
	s, api, q := newStreamEnv(t, func(_ *fakeStreamQueries, api *fakeStreamingAPIClient) {
		api.createErr = errors.New("cardkit down")
	})

	s.handleEvent(textEvent("**final** answer"))
	s.handleEvent(doneEvent("**final** answer"))

	eventually(t, func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.mdCards) == 1 && strings.Contains(api.mdCards[0].Markdown, "**final** answer")
	}, "legacy markdown reply after create failure")

	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.statuses) == 0 || q.statuses[len(q.statuses)-1].Status != string(CardStatusError) {
		t.Fatalf("row should end error, got %+v", q.statuses)
	}
}

func TestStreamingIgnoresNonTextFrames(t *testing.T) {
	s, api, q := newStreamEnv(t, nil)

	for _, kind := range []string{"tool_use", "tool_result"} {
		s.handleEvent(events.Event{
			Type:    protocol.EventTaskMessage,
			TaskID:  uuidString(streamTaskID),
			Payload: protocol.TaskMessagePayload{TaskID: uuidString(streamTaskID), Type: kind, Content: "x"},
		})
	}

	api.mu.Lock()
	if len(api.cardsCreated) != 0 {
		api.mu.Unlock()
		t.Fatalf("tool frames must not start a stream, %d cards", len(api.cardsCreated))
	}
	api.mu.Unlock()
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.created) != 0 {
		t.Fatalf("tool frames must not claim a row, %d rows", len(q.created))
	}
}

func TestStreamingForeignTaskIsCached(t *testing.T) {
	s, api, q := newStreamEnv(t, func(q *fakeStreamQueries, _ *fakeStreamingAPIClient) {
		q.deliveryErr = pgx.ErrNoRows
	})

	s.handleEvent(textEvent("hi"))
	s.handleEvent(textEvent("again"))

	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.created) != 0 {
		t.Fatalf("foreign task must not claim rows, got %d", len(q.created))
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.cardsCreated) != 0 {
		t.Fatalf("foreign task must not create cards, got %d", len(api.cardsCreated))
	}
}

func TestStreamingReopensAutoClosedCard(t *testing.T) {
	s, api, _ := newStreamEnv(t, func(_ *fakeStreamQueries, api *fakeStreamingAPIClient) {
		api.contentErrs = []error{&APIError{Op: "update card element content", Code: codeCardStreamingClosed}}
	})

	s.handleEvent(textEvent("recoverable"))
	waitForCard(t, api)
	s.handleEvent(doneEvent("recoverable now"))

	eventually(t, func() bool { return api.settled() && api.lastContent() == "recoverable now" },
		"finalized after reopen")

	api.mu.Lock()
	defer api.mu.Unlock()
	var reopened bool
	for _, st := range api.settingsCalls {
		if st.StreamingMode {
			reopened = true
		}
	}
	if !reopened {
		t.Fatal("auto-closed card should be reopened with streaming_mode=true")
	}
	if len(api.contentCalls) < 2 {
		t.Fatalf("content update should retry after reopen, got %d calls", len(api.contentCalls))
	}
}

func TestStreamingTopicWithoutTriggerNeverStarts(t *testing.T) {
	s, api, q := newStreamEnv(t, func(q *fakeStreamQueries, _ *fakeStreamingAPIClient) {
		// Composite "chat:thread" key with the real chat id in config marks
		// the binding topic-isolated; no frozen trigger → no outbound at all.
		q.delivery.ChannelChatID = "oc_chat:om_thread"
		q.delivery.Config = []byte(`{"chat_id":"oc_chat"}`)
		q.delivery.ChannelMessageID = pgtype.Text{}
	})

	s.handleEvent(textEvent("hi"))

	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.created) != 0 {
		t.Fatal("topic-isolated session without a trigger must not claim a row")
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.cardsCreated) != 0 {
		t.Fatal("topic-isolated session without a trigger must not create a card")
	}
}

func TestPatcherSkipsWhenStreamingCardClaimed(t *testing.T) {
	q := &fakePatcherQueries{
		task: db.AgentTaskQueue{ID: streamTaskID, ChatSessionID: streamSessionID},
		delivery: db.ChannelTaskDelivery{
			BindingID: streamInstID, InstallationID: streamInstID, ChannelType: channelTypeFeishu,
			ChannelChatID: "oc_chat", ChatType: string(ChatTypeP2P),
		},
		installation:        Installation{ID: streamInstID, Status: string(InstallationActive), AppID: "cli_test"},
		taskChannelIngested: true,
		card:                OutboundCardMessage{ID: streamCardRowID, CardEntityID: "card_1"},
	}
	api := &fakeAPIClient{sendReturn: "om_x"}
	p := NewPatcher(q, fakeCredentials{"secret"}, api, PatcherConfig{Logger: newDiscardLogger()})

	err := p.processEvent(context.Background(), events.Event{
		Type:          protocol.EventChatDone,
		TaskID:        uuidString(streamTaskID),
		ChatSessionID: uuidString(streamSessionID),
		Payload:       protocol.ChatDonePayload{TaskID: uuidString(streamTaskID), Content: "the reply"},
	})
	if err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.textSent) != 0 || len(api.mdCardSent) != 0 {
		t.Fatalf("patcher must not send when a streaming card owns the reply: texts=%d cards=%d",
			len(api.textSent), len(api.mdCardSent))
	}
}

func TestRenderThinkWindow(t *testing.T) {
	// Small buffer: the full text quoted under the header.
	got, anchor := renderThinkWindow("想", 0)
	if anchor != 0 || !strings.HasPrefix(got, "**Thinking**\n\n> 想") {
		t.Fatalf("window malformed: %q anchor=%d", got, anchor)
	}
	// Growth keeps the head: the render extends the previous one, so the
	// typewriter animates only the delta.
	grown, _ := renderThinkWindow("想一二三", 0)
	if !strings.HasPrefix(grown, got) {
		t.Fatalf("growth must extend the previous render:\n%q\n→ %q", got, grown)
	}
	// Past the high-water mark the window resets to the latest runes under
	// an ellipsis marker. The head changes on purpose: Lark renders
	// non-append updates instantly, which reads as the window scrolling
	// forward rather than the card freezing at a cap.
	base := "头" + strings.Repeat("H", streamThinkWindowHigh)
	over := base + strings.Repeat("T", 700) + "尾"
	reset, next := renderThinkWindow(over, 0)
	if want := len([]rune(over)) - streamThinkWindowLow; next != want {
		t.Fatalf("reset anchor: got %d want %d", next, want)
	}
	if !strings.Contains(reset, "…") || !strings.Contains(reset, "尾") {
		t.Fatalf("reset window should carry the marker and latest runes: %q", reset[:min(80, len(reset))])
	}
	if strings.Contains(reset, "头") || strings.Contains(reset, "H") {
		t.Fatal("reset window must drop the overflowed head")
	}
	// After a reset, growth extends the reset render — the typewriter
	// resumes from the window's head.
	resume, _ := renderThinkWindow(over+"加", next)
	if !strings.HasPrefix(resume, reset) {
		t.Fatalf("post-reset growth must extend the reset render:\n%q\n→ %q", reset, resume)
	}
}

func TestBuildStreamingCardJSON(t *testing.T) {
	raw, err := buildReplyCardJSON("seed")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, want := range []string{
		`"schema":"2.0"`,
		`"streaming_mode":true`,
		`"element_id":"reply_text"`,
		`"content":"seed"`,
		// The delay strategy never dumps un-rendered backlog mid-stream;
		// pacing must stay at or above agent generation speed so the
		// typewriter does not lag the buffer (14 chars/s defaults do).
		`"print_strategy":"delay"`,
		`"print_frequency_ms":{"android":30,"default":30,"ios":30,"pc":30}`,
		`"print_step":{"android":10,"default":10,"ios":10,"pc":10}`,
	} {
		if !strings.Contains(raw, want) {
			t.Fatalf("card JSON missing %s: %s", want, raw)
		}
	}

	// The split layout puts the thinking window, a divider, and the reply
	// element in the same card, each markdown addressable by its own id.
	split, err := buildSplitCardJSON("**Thinking**\n\n> 想", streamReplySlotPlaceholder)
	if err != nil {
		t.Fatalf("build split: %v", err)
	}
	for _, want := range []string{
		`"element_id":"think_text"`,
		`"tag":"hr"`,
		`"element_id":"reply_text"`,
		// json.Marshal HTML-escapes ">" as \u003e; Lark parses the escape.
		`"content":"**Thinking**\n\n\u003e 想"`,
		`"content":"…"`,
	} {
		if !strings.Contains(split, want) {
			t.Fatalf("split card JSON missing %s: %s", want, split)
		}
	}
}

// replyContentFromCardJSON extracts the reply element's content from a
// serialized card document.
func replyContentFromCardJSON(t *testing.T, raw string) string {
	t.Helper()
	var doc struct {
		Body struct {
			Elements []struct {
				Tag       string `json:"tag"`
				ElementID string `json:"element_id"`
				Content   string `json:"content"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("parse card json: %v (raw=%s)", err, raw)
	}
	for _, el := range doc.Body.Elements {
		if el.ElementID == streamElementID {
			return el.Content
		}
	}
	t.Fatalf("reply element missing from card: %s", raw)
	return ""
}

func TestTruncateCardTail(t *testing.T) {
	// Under budget: untouched.
	if got := truncateCardTail("short", 100); got != "short" {
		t.Fatalf("short content altered: %q", got)
	}
	// Over budget: cut on a rune boundary with the pointer suffix, within
	// the byte cap.
	content := strings.Repeat("飞", 100) // 300 bytes
	got := truncateCardTail(content, 100)
	if len(got) > 100 {
		t.Fatalf("truncated content exceeds budget: %d bytes", len(got))
	}
	if !strings.Contains(got, "完整回复见下一条消息") {
		t.Fatalf("truncated content missing pointer suffix: %q", got)
	}
}
