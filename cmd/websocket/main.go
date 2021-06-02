package main

import (
	"flag"
	"fmt"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	quickfix "github.com/cryptogarageinc/quickfix-go"
	"github.com/cryptogarageinc/quickfix-go/field"
	fix44quote "github.com/cryptogarageinc/quickfix-go/fix44/quote"
	fix44qr "github.com/cryptogarageinc/quickfix-go/fix44/quoterequest"
	"github.com/cryptogarageinc/quickfix-go/tag"

	// "github.com/shopspring/decimal"

	"github.com/cryptogarageinc/quickfix-go-examples/internal/pkg/websocket"
)

var srv http.Server
var addr = flag.String("addr", ":8080", "http service address")

type SubscribeMessage struct {
	setting   *quickfix.Settings
	sessionID quickfix.SessionID
}

func (m *SubscribeMessage) SetSession(sessionID quickfix.SessionID) {
	m.sessionID = sessionID
}

// newQuoteRequestByFix44 This function create QuoteRequest message.
func (m *SubscribeMessage) newQuoteRequestByFix44(quoteReqID, symbol, account string) *quickfix.Message {
	order := fix44qr.New(field.NewQuoteReqID(quoteReqID))
	// order.Set(field.NewAccount(account))
	// order.Set(field.NewSymbol(symbol))
	// order.Set(field.NewQuoteRequestType(enum.QuoteRequestType_AUTOMATIC))
	// order.Set(field.NewQuoteType(enum.QuoteType_RESTRICTED_TRADEABLE))
	// order.Set(field.NewOrdType(enum.OrdType_FOREX_MARKET))
	// FIXME
	// order.Set(field.NewOrderQty(decimal.New(0, 0), 0))
	group := fix44qr.NewNoRelatedSymRepeatingGroup()
	groupData := group.Add().Group
	groupData.FieldMap.SetString(tag.Account, account)
	groupData.FieldMap.SetString(tag.Symbol, symbol)
	groupData.FieldMap.SetInt(tag.OrderQty, 0)
	order.SetNoRelatedSym(group)
	// order.Set(field.NewNoRelatedSym(1))

	order.Header.SetTargetCompID(m.sessionID.TargetCompID)
	order.Header.SetSenderCompID(m.sessionID.SenderCompID)
	return order.ToMessage()
}

type PriceLogger struct {
	Enable   bool
	FileName string
	Asset    string
	Exchange string
	Handle   *os.File
	Count    uint64
}

// NewPriceLogger This function create PriceLogger.
func NewPriceLogger(settings *quickfix.Settings) *PriceLogger {
	globalSetting := settings.GlobalSettings()
	enable, err := globalSetting.BoolSetting("LoggingPrice")
	if err != nil {
		enable = false
	}
	asset, err := globalSetting.Setting("LoggingAsset")
	if err != nil {
		asset = "BTC/JPY"
	}
	exchange, err := globalSetting.Setting("LoggingExchangeName")
	if err != nil {
		exchange = ""
	}
	filename, err := globalSetting.Setting("LoggingFileName")
	if err != nil || len(filename) == 0 {
		filename = "price_{asset}_{time}.csv"
	} else if strings.Contains(filename, ".csv") == false {
		filename = filename + ".csv"
	}
	assetName := strings.Replace(asset, "/", "_", -1)
	timeString := time.Now().UTC().Format("20060102150405")
	filename = strings.Replace(filename, "{asset}", assetName, -1)
	filename = strings.Replace(filename, "{time}", timeString, -1)
	return &PriceLogger{
		Enable:   enable,
		FileName: filename,
		Asset:    asset,
		Exchange: exchange,
	}
}

// Open This function open logging file.
func (obj *PriceLogger) Open() error {
	if obj.Enable == false {
		return nil
	}
	file, err := os.OpenFile(obj.FileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	_, err = file.WriteString(",time,exchange,qty,sellprice,buyprice\n")
	if err != nil {
		file.Close()
		return err
	}
	obj.Handle = file
	return nil
}

// Open This function close logging file.
func (obj *PriceLogger) Close() error {
	if obj.Handle == nil {
		return nil
	}
	return obj.Handle.Close()
}

// WriteQuote This function write quote message.
func (obj *PriceLogger) WriteQuoteMessage(quote *fix44quote.Quote) error {
	if obj.Handle == nil {
		return nil
	}
	asset, err := quote.GetSymbol()
	if err != nil {
		return err
	} else if obj.Asset != asset {
		return nil
	}
	count := obj.Count
	if count == uint64(0xffffffffffffffff) {
		obj.Count = 0
	} else {
		obj.Count = obj.Count + 1
	}
	timeData, err := quote.GetTransactTime()
	if err != nil {
		// if field is not found, use receive time.
		timeData = time.Now().UTC()
	}
	timeString := timeData.Format("2006-01-02 15:04:05.000000")
	qty, err := quote.GetBidSize()
	if err != nil {
		return err
	}
	bid, err := quote.GetBidPx() // sell
	if err != nil {
		return err
	}
	offer, err := quote.GetOfferPx() // ask
	if err != nil {
		return err
	}
	floatQty, _ := qty.Float64()
	floatBid, _ := bid.Float64()
	floatOffer, _ := offer.Float64()

	logStr := fmt.Sprintf("%d,%s,%s,%g,%f,%f\n", count, timeString, obj.Exchange, floatQty, floatBid, floatOffer)
	_, osError := obj.Handle.WriteString(logStr)
	return osError
}

type PriceMessage struct {
	QuoteId      string  `json:"quote_id"`
	TransactTime string  `json:"transact_time"`
	Symbol       string  `json:"symbol"`
	Size         float64 `json:"size"`
	Bid          float64 `json:"bid"`
	Offer        float64 `json:"offer"`
}

func NewPriceMessageFromQuote(quote *fix44quote.Quote) *PriceMessage {
	quoteId, err := quote.GetQuoteID()
	if err != nil {
		fmt.Printf("GetQuoteID error: '%s'\n", err)
		return nil
	}
	asset, err := quote.GetSymbol()
	if err != nil {
		fmt.Printf("GetSymbol error: '%s'\n", err)
		return nil
	}
	timeData, err := quote.GetTransactTime()
	if err != nil {
		// if field is not found, use receive time.
		timeData = time.Now().UTC()
	}
	timeString := timeData.Format("2006-01-02 15:04:05.000000")
	qty, err := quote.GetBidSize()
	if err != nil {
		fmt.Printf("GetBidSize error: '%s'\n", err)
		return nil
	}
	bid, err := quote.GetBidPx() // sell
	if err != nil {
		fmt.Printf("GetBidPx error: '%s'\n", err)
		return nil
	}
	offer, err := quote.GetOfferPx() // ask
	if err != nil {
		fmt.Printf("GetOfferPx error: '%s'\n", err)
		return nil
	}
	floatQty, _ := qty.Float64()
	floatBid, _ := bid.Float64()
	floatOffer, _ := offer.Float64()

	return &PriceMessage{
		QuoteId: quoteId,
		TransactTime: timeString,
		Symbol: asset,
		Size: floatQty,
		Bid: floatBid,
		Offer: floatOffer,
	}
}

type QuoteRequestData struct {
	QuoteReqID, Symbol, Account string
	Index                       int
}

func GetQuoteRequestDatas(settings *quickfix.Settings) []QuoteRequestData {
	list := make([]QuoteRequestData, 0, 8)
	datas := settings.GlobalSettings()
	for i := 1; i < 9; i++ {
		numStr := strconv.Itoa(i)
		quoteReqId, err := datas.Setting("QuoteReqId" + numStr)
		if err != nil {
			continue
		}
		symbol, err := datas.Setting("Symbol" + numStr)
		if err != nil {
			continue
		}
		account, err := datas.Setting("Account" + numStr)
		if err != nil {
			continue
		}
		list = append(list, QuoteRequestData{
			QuoteReqID: quoteReqId,
			Symbol:     symbol,
			Account:    account,
			Index:      i,
		})
	}
	return list
}

//Subscriber implements the quickfix.Application interface
type Subscriber struct {
	data       *SubscribeMessage
	isDebug    bool
	logger     *PriceLogger
	isStartHub bool
	hub        *websocket.Hub
}

//OnCreate implemented as part of Application interface
func (e Subscriber) OnCreate(sessionID quickfix.SessionID) {
	e.data.sessionID = sessionID
}

//OnLogon implemented as part of Application interface
func (e Subscriber) OnLogon(sessionID quickfix.SessionID) {
	fmt.Println("Logon.")
}

//OnLogout implemented as part of Application interface
func (e Subscriber) OnLogout(sessionID quickfix.SessionID) {
	fmt.Println("Logout.")
}

//FromAdmin implemented as part of Application interface
func (e Subscriber) FromAdmin(msg *quickfix.Message, sessionID quickfix.SessionID) (reject quickfix.MessageRejectError) {
	msgType, err := msg.MsgType()
	if err != nil {
		fmt.Printf("Receive Invalid adminMsg: %s\n", msg.String())
	} else if msgType == "A" {
		fmt.Printf("Recv Logon: %s\n", msg.String())
	} else if msgType == "5" {
		fmt.Printf("Recv Logout: %s\n", msg.String())
	} else if msgType == "3" {
		fmt.Printf("Recv Reject: %s\n", msg.String())
	} else if msgType != "0" {
		fmt.Printf("Recv: %s\n", msg.String())
	} else if e.isDebug {
		fmt.Println("Recv heartbeat.")
	}
	return
}

//ToAdmin implemented as part of Application interface
func (e Subscriber) ToAdmin(msg *quickfix.Message, sessionID quickfix.SessionID) {
	msgType, err := msg.MsgType()
	if err != nil {
		fmt.Printf("Receive Invalid adminMsg: %s\n", msg.String())
	} else if e.isDebug == false {
		// do nothing
	} else if msgType == "A" {
		fmt.Printf("Send Logon: %s\n", msg)
	} else if msgType == "5" {
		fmt.Printf("Send Logout: %s\n", msg)
	} else if msgType != "0" {
		fmt.Printf("Send: %s\n", msg)
	}
}

//ToApp implemented as part of Application interface
func (e Subscriber) ToApp(msg *quickfix.Message, sessionID quickfix.SessionID) (err error) {
	fmt.Printf("Sending %s\n", msg)
	return
}

//FromApp implemented as part of Application interface. This is the callback for all Application level messages from the counter party.
func (e Subscriber) FromApp(msg *quickfix.Message, sessionID quickfix.SessionID) (reject quickfix.MessageRejectError) {
	msgType, err := msg.MsgType()
	if err != nil {
		fmt.Printf("Receive Invalid msg: %s\n", msg.String())
	} else if msgType == "S" {
		quoteData := fix44quote.FromMessage(msg)
		fmt.Printf("Quote: %s, size=%d\n", msg.String(), quoteData.Body.Len())
		tmpErr := e.logger.WriteQuoteMessage(&quoteData)
		if tmpErr != nil {
			fmt.Printf("Logging error: %s\n", tmpErr)
		}
		priceMsg := NewPriceMessageFromQuote(&quoteData)
		if priceMsg != nil {
			priceJson, err := json.Marshal(priceMsg)
			if err != nil {
				fmt.Printf("Marshaling error: %s\n", err)
				return
			}
			
//			fmt.Printf("price json: '%s'\n", priceJson)
			e.hub.Broadcast <- priceJson

		}
	} else if msgType == "j" {
		fmt.Printf("BusinessMessageReject: %s\n", msg.String())
	} else {
		fmt.Printf("Receive: %s\n", msg.String())
	}
	return
}

// queryQuoteRequestOrder This function send QuoteRequest message.
func (e Subscriber) queryQuoteRequestOrder(quoteReqID, symbol, account string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()

	order := e.data.newQuoteRequestByFix44(quoteReqID, symbol, account)
	return quickfix.Send(order)
}

func main() {
	flag.Parse()
	
	cfgFileName := path.Join("config", "subscriber.cfg")
	if flag.NArg() > 0 {
		cfgFileName = flag.Arg(0)
	}

	cfg, err := os.Open(cfgFileName)
	if err != nil {
		fmt.Printf("Error opening %v, %v\n", cfgFileName, err)
		return
	}

	appSettings, err := quickfix.ParseSettings(cfg)
	if err != nil {
		fmt.Println("Error reading cfg,", err)
		return
	}
	isDebug := false
	if appSettings.GlobalSettings().HasSetting("Debug") {
		isDebug, _ = appSettings.GlobalSettings().BoolSetting("Debug")
	}

	// create a message hub
	fmt.Printf("listen '%s'\n", *addr)
	hub := websocket.NewHub()

	appData := SubscribeMessage{setting: appSettings}
	logger := NewPriceLogger(appSettings)
	app := Subscriber{data: &appData, isDebug: isDebug, logger: logger, hub: hub}
	fileLogFactory, err := quickfix.NewFileLogFactory(appSettings)

	if err != nil {
		fmt.Println("Error creating file log factory,", err)
		return
	}

	initiator, err := quickfix.NewInitiator(app, quickfix.NewMemoryStoreFactory(), appSettings, fileLogFactory)
	if err != nil {
		fmt.Printf("Unable to create Initiator: %s\n", err)
		return
	}

	err = logger.Open()
	if err != nil {
		fmt.Printf("Error opening log file: %s\n", err)
		return
	}
	defer logger.Close()

	err = initiator.Start()
	if err != nil {
		fmt.Printf("Failed to Start: %s\n", err)
		return
	}

	go app.hub.Run()
	app.isStartHub = true

	// config websocket handler
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		websocket.ServeWs(hub, w, r)
	})

	err = http.ListenAndServe(*addr, nil)
	if err != nil {
		panic(err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM) // os.Kill
	<-interrupt

	initiator.Stop()
}
