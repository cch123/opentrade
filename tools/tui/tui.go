package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/coder/websocket"
	zone "github.com/lrstanley/bubblezone"
)

// ---------------------------------------------------------------------------
// Form plumbing (text inputs only — toggles live on the Model directly now)
// ---------------------------------------------------------------------------

type textField struct {
	key   string
	label string
	input textinput.Model
}

func newTextField(key, label, placeholder string, width int) *textField {
	ti := textinput.New()
	ti.Placeholder = placeholder
	ti.CharLimit = 64
	ti.Width = width
	return &textField{key: key, label: label, input: ti}
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

type wsStateMsg struct {
	state string // "connecting" / "open" / "closed"
	err   string
}
type wsFrameMsg struct{ frame *WSFrame }
type apiResultMsg struct {
	ok      bool
	message string
}
type pollMsg struct{}
type pollResultMsg struct {
	balances []Balance
	orders   []OrderRow
	trades   []TradeRow
	err      string
}

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

type model struct {
	client *Client
	host   string
	wsURL  string

	user  string
	users []string

	// trading view state
	symbol    string
	side      string // "buy" / "sell"
	orderType string // "limit" / "market"
	tif       string // "gtc" / "ioc" / "fok" / "post_only"

	priceField *textField
	qtyField   *textField
	quoteField *textField // market buy: quote budget

	focus int // 0 price, 1 qty, 2 quote

	// orderbook
	book      *orderBook
	lastTrade string // last public trade price, for center marker

	// balances + open orders + trade stream
	balances []Balance
	orders   []OrderRow
	myTrades []TradeRow
	acctErr  string
	acctAt   time.Time

	stream []streamLine // interleaved public + user events

	// transfer modal
	showTransfer   bool
	transferType   int // 0..3
	transferAsset  *textField
	transferAmount *textField
	transferFocus  int // 0 type, 1 asset, 2 amount

	// ws state
	wsState  string
	wsErr    string
	wsConn   *websocket.Conn
	wsCancel context.CancelFunc
	wsCh     chan tea.Msg

	// status bar
	status string
	isErr  bool

	width, height int
}

type streamLine struct {
	ts   time.Time
	kind string // "pub" / "me" / "ord" / "xfer" / "ctrl" / "info"
	text string
}

var transferTypes = []string{"deposit", "withdraw", "freeze", "unfreeze"}

func initialModel(host, wsURL string, users []string) *model {
	zone.NewGlobal() // enable bubblezone click tracking
	m := &model{
		client:    NewClient(host),
		host:      host,
		wsURL:     wsURL,
		users:     users,
		user:      users[0],
		symbol:    "BTC-USDT",
		side:      "buy",
		orderType: "limit",
		tif:       "gtc",
		book:      newOrderBook("BTC-USDT"),
		wsState:   "idle",
		wsCh:      make(chan tea.Msg, 64),
	}
	m.priceField = newTextField("price", "price", "50000", 14)
	m.qtyField = newTextField("qty", "qty", "0.01", 14)
	m.quoteField = newTextField("quote_qty", "quote", "(market buy budget)", 14)
	m.priceField.input.Focus()

	m.transferAsset = newTextField("asset", "asset", "USDT", 10)
	m.transferAmount = newTextField("amount", "amount", "100.00", 14)

	return m
}

func (m *model) Init() tea.Cmd {
	return tea.Batch(m.pollCmd(), m.connectWSCmd())
}

// ---------------------------------------------------------------------------
// WS pump
// ---------------------------------------------------------------------------

func (m *model) connectWSCmd() tea.Cmd {
	return func() tea.Msg {
		go m.runWS()
		return wsStateMsg{state: "connecting"}
	}
}

func (m *model) runWS() {
	if m.wsCancel != nil {
		m.wsCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.wsCancel = cancel
	defer cancel()

	dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
	conn, err := DialWS(dialCtx, m.wsURL, m.user)
	dialCancel()
	if err != nil {
		m.wsCh <- wsStateMsg{state: "closed", err: err.Error()}
		return
	}
	m.wsConn = conn
	m.wsCh <- wsStateMsg{state: "open"}

	_ = Subscribe(ctx, conn, []string{
		"publictrade@" + m.symbol,
		"depth@" + m.symbol,
		"kline@" + m.symbol + ":1m",
	})

	for {
		readCtx, rcancel := context.WithCancel(ctx)
		frame, err := ReadFrame(readCtx, conn)
		rcancel()
		if err != nil {
			select {
			case <-ctx.Done():
				_ = conn.Close(websocket.StatusNormalClosure, "bye")
				return
			default:
			}
			m.wsCh <- wsStateMsg{state: "closed", err: err.Error()}
			_ = conn.Close(websocket.StatusInternalError, "read error")
			return
		}
		m.wsCh <- wsFrameMsg{frame: frame}
	}
}

func (m *model) waitForWSCmd() tea.Cmd {
	return func() tea.Msg {
		return <-m.wsCh
	}
}

// ---------------------------------------------------------------------------
// Poll
// ---------------------------------------------------------------------------

func (m *model) pollCmd() tea.Cmd {
	return tea.Tick(2*time.Second, func(time.Time) tea.Msg { return pollMsg{} })
}

func (m *model) runPoll() tea.Cmd {
	user, client, sym := m.user, m.client, m.symbol
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		out := pollResultMsg{}
		if bal, err := client.Balances(ctx, user); err == nil {
			out.balances = bal.Balances
		} else {
			out.err = "balances: " + err.Error()
		}
		if ords, err := client.OpenOrders(ctx, user, sym); err == nil {
			out.orders = ords.Orders
		}
		if tr, err := client.RecentTrades(ctx, user, sym); err == nil {
			out.trades = tr.Trades
		}
		return out
	}
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(msg)
	case tea.MouseMsg:
		return m.handleMouse(msg)
	case wsStateMsg:
		m.wsState = msg.state
		m.wsErr = msg.err
		if msg.state == "closed" && msg.err != "" {
			m.pushStream("ctrl", "ws closed: "+msg.err)
		}
		return m, m.waitForWSCmd()
	case wsFrameMsg:
		m.handleFrame(msg.frame)
		return m, m.waitForWSCmd()
	case apiResultMsg:
		m.status = msg.message
		m.isErr = !msg.ok
		return m, m.runPoll()
	case pollMsg:
		return m, tea.Batch(m.runPoll(), m.pollCmd())
	case pollResultMsg:
		m.balances = msg.balances
		m.orders = msg.orders
		m.myTrades = msg.trades
		m.acctErr = msg.err
		m.acctAt = time.Now()
		return m, nil
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// Key handling
// ---------------------------------------------------------------------------

func (m *model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.showTransfer {
		return m.handleTransferKey(msg)
	}

	s := msg.String()
	switch s {
	case "ctrl+c", "ctrl+q":
		return m, tea.Quit
	case "ctrl+u":
		m.cycleUser()
		return m, tea.Batch(m.connectWSCmd(), m.runPoll())
	case "ctrl+r":
		return m, m.connectWSCmd()
	case "tab":
		m.focusNext(+1)
		return m, nil
	case "shift+tab":
		m.focusNext(-1)
		return m, nil
	case "enter":
		return m, m.submitOrder()
	}

	// Non-typing shortcuts: only when focus is NOT on a text input that
	// actually cares about the character (e.g. digits belong to price).
	if !m.focused().input.Focused() {
		switch s {
		case "b":
			m.side = "buy"
			return m, nil
		case "s":
			m.side = "sell"
			return m, nil
		case "l":
			m.orderType = "limit"
			return m, nil
		case "m":
			m.orderType = "market"
			return m, nil
		case "g":
			m.tif = "gtc"
			return m, nil
		case "i":
			m.tif = "ioc"
			return m, nil
		case "f":
			m.tif = "fok"
			return m, nil
		case "p":
			m.tif = "post_only"
			return m, nil
		case "t":
			m.showTransfer = true
			m.transferAsset.input.Focus()
			return m, nil
		case "r":
			return m, m.runPoll()
		}
	} else {
		// Shortcuts that always work (don't conflict with digit/decimal input).
		switch s {
		case "ctrl+t":
			m.showTransfer = true
			m.transferAsset.input.Focus()
			return m, nil
		}
	}

	// Forward remaining to the focused input.
	var cmd tea.Cmd
	fl := m.focused()
	fl.input, cmd = fl.input.Update(msg)
	return m, cmd
}

func (m *model) handleTransferKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+c":
		m.closeTransfer()
		return m, nil
	case "tab":
		m.transferFocusNext(+1)
		return m, nil
	case "shift+tab":
		m.transferFocusNext(-1)
		return m, nil
	case "left":
		if m.transferFocus == 0 {
			m.transferType = (m.transferType - 1 + len(transferTypes)) % len(transferTypes)
		}
		return m, nil
	case "right":
		if m.transferFocus == 0 {
			m.transferType = (m.transferType + 1) % len(transferTypes)
		}
		return m, nil
	case "enter":
		return m, m.submitTransfer()
	}
	// forward to focused input
	var cmd tea.Cmd
	switch m.transferFocus {
	case 1:
		m.transferAsset.input, cmd = m.transferAsset.input.Update(msg)
	case 2:
		m.transferAmount.input, cmd = m.transferAmount.input.Update(msg)
	}
	return m, cmd
}

func (m *model) focused() *textField {
	switch m.focus {
	case 1:
		return m.qtyField
	case 2:
		return m.quoteField
	}
	return m.priceField
}

func (m *model) focusNext(delta int) {
	fields := []*textField{m.priceField, m.qtyField, m.quoteField}
	fields[m.focus].input.Blur()
	n := len(fields)
	m.focus = ((m.focus+delta)%n + n) % n
	fields[m.focus].input.Focus()
}

func (m *model) transferFocusNext(delta int) {
	// sequence: 0 type, 1 asset, 2 amount
	switch m.transferFocus {
	case 1:
		m.transferAsset.input.Blur()
	case 2:
		m.transferAmount.input.Blur()
	}
	m.transferFocus = ((m.transferFocus+delta)%3 + 3) % 3
	switch m.transferFocus {
	case 1:
		m.transferAsset.input.Focus()
	case 2:
		m.transferAmount.input.Focus()
	}
}

func (m *model) cycleUser() {
	for i, u := range m.users {
		if u == m.user {
			m.user = m.users[(i+1)%len(m.users)]
			m.status = "switched user → " + m.user
			m.isErr = false
			return
		}
	}
	m.user = m.users[0]
}

func (m *model) closeTransfer() {
	m.showTransfer = false
	m.transferAsset.input.Blur()
	m.transferAmount.input.Blur()
	m.transferFocus = 0
}

// ---------------------------------------------------------------------------
// Mouse handling
// ---------------------------------------------------------------------------

func (m *model) handleMouse(msg tea.MouseMsg) (tea.Model, tea.Cmd) {
	if msg.Action != tea.MouseActionPress || msg.Button != tea.MouseButtonLeft {
		return m, nil
	}

	if m.showTransfer {
		if zone.Get("xfer-cancel").InBounds(msg) {
			m.closeTransfer()
			return m, nil
		}
		if zone.Get("xfer-confirm").InBounds(msg) {
			return m, m.submitTransfer()
		}
		for i, t := range transferTypes {
			if zone.Get("xfer-type-" + t).InBounds(msg) {
				m.transferType = i
				return m, nil
			}
		}
		return m, nil
	}

	// Main view zones.
	if zone.Get("user-chip").InBounds(msg) {
		m.cycleUser()
		return m, tea.Batch(m.connectWSCmd(), m.runPoll())
	}
	if zone.Get("btn-transfer").InBounds(msg) {
		m.showTransfer = true
		m.transferAsset.input.Focus()
		return m, nil
	}
	if zone.Get("btn-submit").InBounds(msg) {
		return m, m.submitOrder()
	}
	if zone.Get("btn-reconnect").InBounds(msg) {
		return m, m.connectWSCmd()
	}

	// side / type / tif chips
	for _, opt := range []string{"buy", "sell"} {
		if zone.Get("side-" + opt).InBounds(msg) {
			m.side = opt
			return m, nil
		}
	}
	for _, opt := range []string{"limit", "market"} {
		if zone.Get("type-" + opt).InBounds(msg) {
			m.orderType = opt
			return m, nil
		}
	}
	for _, opt := range []string{"gtc", "ioc", "fok", "post_only"} {
		if zone.Get("tif-" + opt).InBounds(msg) {
			m.tif = opt
			return m, nil
		}
	}

	// Orderbook price click → copy to price field.
	for _, l := range m.book.topAsks(20) {
		if zone.Get("ask-" + l.price).InBounds(msg) {
			m.priceField.input.SetValue(l.price)
			return m, nil
		}
	}
	for _, l := range m.book.topBids(20) {
		if zone.Get("bid-" + l.price).InBounds(msg) {
			m.priceField.input.SetValue(l.price)
			return m, nil
		}
	}

	// Cancel buttons per open order row.
	for _, o := range m.orders {
		if zone.Get(fmt.Sprintf("cancel-%d", o.OrderID)).InBounds(msg) {
			return m, m.submitCancel(o.OrderID)
		}
	}

	// Focus text input on click.
	if zone.Get("input-price").InBounds(msg) {
		m.focus = 0
		m.priceField.input.Focus()
		m.qtyField.input.Blur()
		m.quoteField.input.Blur()
	}
	if zone.Get("input-qty").InBounds(msg) {
		m.focus = 1
		m.qtyField.input.Focus()
		m.priceField.input.Blur()
		m.quoteField.input.Blur()
	}
	if zone.Get("input-quote").InBounds(msg) {
		m.focus = 2
		m.quoteField.input.Focus()
		m.priceField.input.Blur()
		m.qtyField.input.Blur()
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// WS frame dispatch
// ---------------------------------------------------------------------------

func (m *model) handleFrame(fr *WSFrame) {
	if fr.Control != "" {
		m.pushStream("ctrl", fr.Control+" "+strings.TrimSpace(string(fr.Raw)))
		return
	}
	switch {
	case fr.Stream == "user":
		m.pushStream("me", summarizeUserEvent(fr.Data))
	case strings.HasPrefix(fr.Stream, "publictrade@"):
		m.pushStream("pub", summarizePublicTrade(fr.Data))
		m.lastTrade = extractTradePrice(fr.Data)
	case strings.HasPrefix(fr.Stream, "depth@"):
		m.book.applyDepthJSON(fr.Data)
	case strings.HasPrefix(fr.Stream, "kline@"):
		// skip in stream log; we don't render klines in this layout.
	}
}

func (m *model) pushStream(kind, text string) {
	m.stream = append(m.stream, streamLine{ts: time.Now(), kind: kind, text: text})
	if len(m.stream) > 500 {
		m.stream = m.stream[len(m.stream)-500:]
	}
}

func extractTradePrice(data json.RawMessage) string {
	var s struct {
		Price string `json:"price"`
	}
	if json.Unmarshal(data, &s) == nil {
		return s.Price
	}
	return ""
}

// ---------------------------------------------------------------------------
// Summarizers (shared with api.go is not imported there — keep here)
// ---------------------------------------------------------------------------

func summarizeUserEvent(data json.RawMessage) string {
	var env map[string]json.RawMessage
	if err := json.Unmarshal(data, &env); err != nil {
		return string(data)
	}
	for k, v := range env {
		switch k {
		case "orderStatus":
			var s struct {
				OrderID   string `json:"orderId"`
				NewStatus string `json:"newStatus"`
				Symbol    string `json:"symbol"`
				FilledQty string `json:"filledQty"`
			}
			if json.Unmarshal(v, &s) == nil {
				return fmt.Sprintf("order %s %s %s filled=%s", s.OrderID, s.Symbol, s.NewStatus, s.FilledQty)
			}
		case "trade":
			var s struct {
				TradeID string `json:"tradeId"`
				Symbol  string `json:"symbol"`
				Price   string `json:"price"`
				Qty     string `json:"qty"`
				Role    string `json:"role"`
				OrderID string `json:"orderId"`
			}
			if json.Unmarshal(v, &s) == nil {
				return fmt.Sprintf("trade %s %s×%s role=%s order=%s",
					s.Symbol, s.Price, s.Qty, s.Role, s.OrderID)
			}
		case "transfer":
			var s struct {
				TransferID string `json:"transferId"`
				Asset      string `json:"asset"`
				Amount     string `json:"amount"`
				Type       string `json:"type"`
			}
			if json.Unmarshal(v, &s) == nil {
				return fmt.Sprintf("transfer %s %s %s", s.Type, s.Asset, s.Amount)
			}
		case "freeze", "unfreeze", "settlement", "cancelReq":
			return k + " " + string(v)
		}
	}
	return string(data)
}

func summarizePublicTrade(data json.RawMessage) string {
	var s struct {
		Symbol    string `json:"symbol"`
		Price     string `json:"price"`
		Qty       string `json:"qty"`
		TakerSide string `json:"takerSide"`
	}
	if json.Unmarshal(data, &s) != nil {
		return string(data)
	}
	side := strings.ToLower(strings.TrimPrefix(s.TakerSide, "SIDE_"))
	return fmt.Sprintf("%s %s×%s %s", s.Symbol, s.Price, s.Qty, side)
}

// ---------------------------------------------------------------------------
// Submit handlers
// ---------------------------------------------------------------------------

func (m *model) submitOrder() tea.Cmd {
	symbol := m.symbol
	side := m.side
	ot := m.orderType
	tif := m.tif
	price := strings.TrimSpace(m.priceField.input.Value())
	qty := strings.TrimSpace(m.qtyField.input.Value())
	qq := strings.TrimSpace(m.quoteField.input.Value())

	req := PlaceOrderReq{
		ClientOrderID: "tui-" + RandID(),
		Symbol:        symbol,
		Side:          side,
		OrderType:     ot,
		TIF:           tif,
	}
	switch ot {
	case "limit":
		if price == "" || qty == "" {
			return resultCmd(false, "limit needs price + qty")
		}
		req.Price = price
		req.Qty = qty
	case "market":
		if side == "buy" {
			if qq == "" {
				return resultCmd(false, "market buy needs quote_qty")
			}
			req.QuoteQty = qq
		} else {
			if qty == "" {
				return resultCmd(false, "market sell needs qty")
			}
			req.Qty = qty
		}
	}
	user := m.user
	client := m.client
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := client.PlaceOrder(ctx, user, req)
		if err != nil {
			return apiResultMsg{ok: false, message: "order failed: " + err.Error()}
		}
		return apiResultMsg{
			ok:      resp.Accepted,
			message: fmt.Sprintf("order %d accepted=%v status=%s", resp.OrderID, resp.Accepted, resp.Status),
		}
	}
}

func (m *model) submitCancel(orderID uint64) tea.Cmd {
	user := m.user
	client := m.client
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := client.CancelOrder(ctx, user, orderID)
		if err != nil {
			return apiResultMsg{ok: false, message: "cancel failed: " + err.Error()}
		}
		return apiResultMsg{ok: resp.Accepted, message: fmt.Sprintf("cancel %d accepted=%v", resp.OrderID, resp.Accepted)}
	}
}

func (m *model) submitTransfer() tea.Cmd {
	tt := transferTypes[m.transferType]
	asset := strings.ToUpper(strings.TrimSpace(m.transferAsset.input.Value()))
	amount := strings.TrimSpace(m.transferAmount.input.Value())
	if asset == "" || amount == "" {
		return resultCmd(false, "asset and amount are required")
	}
	user := m.user
	client := m.client
	req := TransferReq{
		TransferID: tt + "-" + user + "-" + RandID(),
		Asset:      asset,
		Amount:     amount,
		Type:       tt,
	}
	m.closeTransfer()
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := client.Transfer(ctx, user, req)
		if err != nil {
			return apiResultMsg{ok: false, message: "transfer failed: " + err.Error()}
		}
		return apiResultMsg{
			ok:      resp.Status == "confirmed" || resp.Status == "duplicated",
			message: fmt.Sprintf("transfer %s %s %s → %s avail=%s frozen=%s", tt, amount, asset, resp.Status, resp.AvailableAfter, resp.FrozenAfter),
		}
	}
}

func resultCmd(ok bool, msg string) tea.Cmd {
	return func() tea.Msg { return apiResultMsg{ok: ok, message: msg} }
}

// ---------------------------------------------------------------------------
// View
// ---------------------------------------------------------------------------

func (m *model) View() string {
	if m.width == 0 {
		return "initializing…"
	}
	header := m.renderHeader()
	ticker := m.renderTicker()

	// Main row: orderbook | (order form / balances)
	orderbookW := 36
	rightW := m.width - orderbookW - 4
	if rightW < 40 {
		rightW = 40
	}
	ob := m.renderOrderbook(orderbookW)
	form := m.renderOrderForm(rightW)
	bal := m.renderBalances(rightW)
	rightCol := lipgloss.JoinVertical(lipgloss.Left, form, bal)
	mid := lipgloss.JoinHorizontal(lipgloss.Top, ob, rightCol)

	orders := m.renderOpenOrders(m.width - 2)
	streamBox := m.renderStream(m.width - 2)
	footer := m.renderFooter()

	view := lipgloss.JoinVertical(lipgloss.Left, header, ticker, mid, orders, streamBox, footer)

	if m.showTransfer {
		modal := m.renderTransferModal()
		view = overlay(view, modal, m.width, m.height)
	}
	return zone.Scan(view)
}

// overlay places `top` centered over `bg`. lipgloss.Place centers within the
// given W×H, padding with the bg color — we want a true overlay, so we render
// the modal centered on an empty canvas and let the terminal redraw it on
// top of the view. A simpler (good-enough) approach: just show the modal and
// skip the main view while the modal is open. Users already saw the state
// in the footer status after closing.
func overlay(bg, top string, w, h int) string {
	return lipgloss.Place(w, h, lipgloss.Center, lipgloss.Center, top)
}

// ---------------------------------------------------------------------------
// Render components
// ---------------------------------------------------------------------------

func (m *model) renderHeader() string {
	title := styleHeader.Render("OpenTrade TUI")
	user := zone.Mark("user-chip", styleChipOn.Render("user: "+m.user))
	ws := m.renderWSBadge()
	reconnect := zone.Mark("btn-reconnect", styleBtnGhost.Render("[reconnect]"))
	xfer := zone.Mark("btn-transfer", styleBtnGhost.Render("[transfer]"))
	right := lipgloss.JoinHorizontal(lipgloss.Top, user, "  ", xfer, "  ", reconnect, "  ", ws)
	gap := max(1, m.width-lipgloss.Width(title)-lipgloss.Width(right))
	return lipgloss.JoinHorizontal(lipgloss.Top, title, strings.Repeat(" ", gap), right)
}

func (m *model) renderWSBadge() string {
	switch m.wsState {
	case "open":
		return styleOK.Render("● ws open")
	case "connecting":
		return styleWarn.Render("◌ connecting")
	case "closed":
		msg := "● closed"
		if m.wsErr != "" {
			msg += " (" + truncate(m.wsErr, 30) + ")"
		}
		return styleErr.Render(msg)
	}
	return styleDim.Render("○ idle")
}

func (m *model) renderTicker() string {
	last := m.lastTrade
	if last == "" {
		if ba, ok := m.book.bestAsk(); ok {
			last = ba.price
		}
	}
	if last == "" {
		last = "—"
	}
	bidQty := "—"
	askQty := "—"
	if bb, ok := m.book.bestBid(); ok {
		bidQty = bb.price + "  " + bb.qty
	}
	if ba, ok := m.book.bestAsk(); ok {
		askQty = ba.price + "  " + ba.qty
	}
	parts := []string{
		styleKey.Render(m.symbol),
		styleLabel.Render(" last: ") + styleOK.Render(last),
		styleLabel.Render(" best bid: ") + styleBuy.Render(bidQty),
		styleLabel.Render(" best ask: ") + styleSell.Render(askQty),
	}
	return lipgloss.NewStyle().Padding(0, 1).Render(strings.Join(parts, "  "))
}

// ---------------------------------------------------------------------------
// Orderbook
// ---------------------------------------------------------------------------

func (m *model) renderOrderbook(width int) string {
	rows := []string{styleTitle.Render("Orderbook — " + m.symbol)}

	askLevels := m.book.topAsks(8)
	bidLevels := m.book.topBids(8)

	// asks: render highest → lowest (so lowest sits just above the mid line)
	for i := len(askLevels) - 1; i >= 0; i-- {
		l := askLevels[i]
		row := renderLevelRow(l, width-4, colSell)
		rows = append(rows, zone.Mark("ask-"+l.price, row))
	}
	if len(askLevels) == 0 {
		rows = append(rows, styleDim.Render("  (asks empty — waiting on depth)"))
	}

	last := m.lastTrade
	if last == "" {
		last = "—"
	}
	midLine := styleDim.Render(strings.Repeat("─", 4) + " last ") + styleOK.Render(last) + styleDim.Render(" "+strings.Repeat("─", 4))
	rows = append(rows, midLine)

	// bids: highest → lowest (top-down)
	for _, l := range bidLevels {
		row := renderLevelRow(l, width-4, colBuy)
		rows = append(rows, zone.Mark("bid-"+l.price, row))
	}
	if len(bidLevels) == 0 {
		rows = append(rows, styleDim.Render("  (bids empty — waiting on depth)"))
	}

	if !m.book.at.IsZero() {
		rows = append(rows, "", styleDim.Render("updated "+m.book.at.Format("15:04:05")+" · click price to copy"))
	} else {
		rows = append(rows, "", styleDim.Render("waiting for depth snapshot…"))
	}

	return styleBox.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

func renderLevelRow(l level, width int, col lipgloss.Color) string {
	priceStr := lipgloss.NewStyle().Foreground(col).Render(fmt.Sprintf("%14s", l.price))
	qty := fmt.Sprintf("%14s", l.qty)
	return priceStr + "  " + qty
}

// ---------------------------------------------------------------------------
// Order form
// ---------------------------------------------------------------------------

func (m *model) renderOrderForm(width int) string {
	sideRow := lipgloss.JoinHorizontal(lipgloss.Top,
		zone.Mark("side-buy", chip(m.side == "buy", "buy")),
		" ",
		zone.Mark("side-sell", chip(m.side == "sell", "sell")),
	)
	typeRow := lipgloss.JoinHorizontal(lipgloss.Top,
		zone.Mark("type-limit", chip(m.orderType == "limit", "limit")),
		" ",
		zone.Mark("type-market", chip(m.orderType == "market", "market")),
	)
	tifRow := lipgloss.JoinHorizontal(lipgloss.Top,
		zone.Mark("tif-gtc", chip(m.tif == "gtc", "gtc")),
		" ",
		zone.Mark("tif-ioc", chip(m.tif == "ioc", "ioc")),
		" ",
		zone.Mark("tif-fok", chip(m.tif == "fok", "fok")),
		" ",
		zone.Mark("tif-post_only", chip(m.tif == "post_only", "post")),
	)

	var priceRow, qtyRow, quoteRow string
	if m.orderType == "limit" {
		priceRow = styleLabel.Render(" price  ") + zone.Mark("input-price", m.priceField.input.View())
		qtyRow = styleLabel.Render(" qty    ") + zone.Mark("input-qty", m.qtyField.input.View())
	} else {
		if m.side == "buy" {
			quoteRow = styleLabel.Render(" quote  ") + zone.Mark("input-quote", m.quoteField.input.View())
		} else {
			qtyRow = styleLabel.Render(" qty    ") + zone.Mark("input-qty", m.qtyField.input.View())
		}
	}

	// Primary action button — colored by side.
	btnLabel := fmt.Sprintf("  Place %s %s %s  ", strings.ToUpper(m.side), m.orderType, m.symbol)
	var btn string
	if m.side == "buy" {
		btn = styleBtnBuy.Render(btnLabel)
	} else {
		btn = styleBtnSell.Render(btnLabel)
	}
	btn = zone.Mark("btn-submit", btn)

	est := m.estimateCost()

	rows := []string{
		styleTitle.Render("Order"),
		"",
		styleLabel.Render(" side  ") + sideRow,
		styleLabel.Render(" type  ") + typeRow,
	}
	if m.orderType == "limit" {
		rows = append(rows, styleLabel.Render(" tif   ")+tifRow)
	}
	if priceRow != "" {
		rows = append(rows, priceRow)
	}
	if qtyRow != "" {
		rows = append(rows, qtyRow)
	}
	if quoteRow != "" {
		rows = append(rows, quoteRow)
	}
	if est != "" {
		rows = append(rows, styleDim.Render(" "+est))
	}
	rows = append(rows, "", btn)

	return styleBox.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

func chip(on bool, label string) string {
	if on {
		return styleChipOn.Render(label)
	}
	return styleChipOff.Render(label)
}

func (m *model) estimateCost() string {
	if m.orderType != "limit" {
		return ""
	}
	p, err1 := strconv.ParseFloat(strings.TrimSpace(m.priceField.input.Value()), 64)
	q, err2 := strconv.ParseFloat(strings.TrimSpace(m.qtyField.input.Value()), 64)
	if err1 != nil || err2 != nil || p <= 0 || q <= 0 {
		return ""
	}
	return fmt.Sprintf("≈ notional %.4f (%s)", p*q, quoteAsset(m.symbol))
}

func quoteAsset(symbol string) string {
	if i := strings.LastIndex(symbol, "-"); i >= 0 {
		return symbol[i+1:]
	}
	return ""
}

// ---------------------------------------------------------------------------
// Balances
// ---------------------------------------------------------------------------

func (m *model) renderBalances(width int) string {
	rows := []string{styleTitle.Render("Balances")}
	if len(m.balances) == 0 {
		rows = append(rows, styleDim.Render(" (loading / empty)"))
	} else {
		rows = append(rows, styleLabel.Render(fmt.Sprintf(" %-6s %14s %14s", "asset", "available", "frozen")))
		for _, b := range m.balances {
			rows = append(rows, fmt.Sprintf(" %-6s %14s %14s", b.Asset, b.Available, b.Frozen))
		}
	}
	if m.acctErr != "" {
		rows = append(rows, styleErr.Render(" "+m.acctErr))
	} else if !m.acctAt.IsZero() {
		rows = append(rows, styleDim.Render(" updated "+m.acctAt.Format("15:04:05")+" · r refresh"))
	}
	return styleBox.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

// ---------------------------------------------------------------------------
// Open orders
// ---------------------------------------------------------------------------

func (m *model) renderOpenOrders(width int) string {
	rows := []string{styleTitle.Render("Open Orders")}
	if len(m.orders) == 0 {
		rows = append(rows, styleDim.Render(" (none)"))
	} else {
		rows = append(rows, styleLabel.Render(fmt.Sprintf(" %-11s %-4s %-7s %-12s %-10s %-10s %-10s %s",
			"order_id", "side", "type", "price", "qty", "filled", "status", "cancel")))
		for _, o := range m.orders {
			ss := styleBuy
			if o.Side == "sell" {
				ss = styleSell
			}
			row := fmt.Sprintf(" %-11d %s %-7s %-12s %-10s %-10s %-10s %s",
				o.OrderID, ss.Render(fmt.Sprintf("%-4s", o.Side)),
				o.OrderType, o.Price, o.Qty, o.FilledQty, o.Status,
				zone.Mark(fmt.Sprintf("cancel-%d", o.OrderID), styleBtnGhost.Render("[X]")))
			rows = append(rows, row)
		}
	}
	return styleBox.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

// ---------------------------------------------------------------------------
// Stream (recent public + user events)
// ---------------------------------------------------------------------------

func (m *model) renderStream(width int) string {
	rows := []string{styleTitle.Render("Stream (public + me)")}
	maxRows := 8
	if m.height >= 44 {
		maxRows = m.height - 36
		if maxRows < 4 {
			maxRows = 4
		}
	}
	start := 0
	if len(m.stream) > maxRows {
		start = len(m.stream) - maxRows
	}
	if start == len(m.stream) {
		rows = append(rows, styleDim.Render(" (no events yet — subscribed to user / publictrade / depth)"))
	}
	for _, l := range m.stream[start:] {
		ts := styleDim.Render(l.ts.Format("15:04:05"))
		var tag string
		switch l.kind {
		case "pub":
			tag = styleOK.Render(" pub ")
		case "me":
			tag = styleKey.Render(" me  ")
		case "ctrl":
			tag = styleWarn.Render(" ctl ")
		default:
			tag = styleDim.Render(" " + l.kind + " ")
		}
		rows = append(rows, " "+ts+" "+tag+" "+l.text)
	}
	return styleBox.Width(width).Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------

func (m *model) renderFooter() string {
	keys := styleDim.Render("b/s side · l/m type · g/i/f/p tif · tab field · enter submit · t transfer · ctrl+u user · ctrl+r ws · q quit")
	status := m.status
	if status == "" {
		status = styleDim.Render("idle")
	} else if m.isErr {
		status = styleErr.Render(status)
	} else {
		status = styleOK.Render(status)
	}
	return lipgloss.JoinVertical(lipgloss.Left, keys, status)
}

// ---------------------------------------------------------------------------
// Transfer modal
// ---------------------------------------------------------------------------

func (m *model) renderTransferModal() string {
	rows := []string{styleTitle.Render("Transfer  —  press esc to cancel")}
	rows = append(rows, "")

	// type chips
	var typeChips []string
	for i, t := range transferTypes {
		c := chip(m.transferType == i, t)
		typeChips = append(typeChips, zone.Mark("xfer-type-"+t, c))
	}
	rows = append(rows, styleLabel.Render(" type   ")+lipgloss.JoinHorizontal(lipgloss.Top, typeChips...))

	rows = append(rows, styleLabel.Render(" asset  ")+m.transferAsset.input.View())
	rows = append(rows, styleLabel.Render(" amount ")+m.transferAmount.input.View())
	rows = append(rows, "")

	confirm := zone.Mark("xfer-confirm", styleBtnNeutral.Render("  Confirm  "))
	cancel := zone.Mark("xfer-cancel", styleBtnGhost.Render("  Cancel  "))
	rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top, confirm, "  ", cancel))

	return styleModalBg.Render(lipgloss.JoinVertical(lipgloss.Left, rows...))
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
