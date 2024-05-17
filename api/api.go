package api

import (
	"context"
	"fmt"
	"log"
	"market-data-api/logging"
	"net/http"
	"strconv"
	"strings"
	"time"

	apipb "code.vegaprotocol.io/vega/protos/data-node/api/v2"
	"code.vegaprotocol.io/vega/protos/vega"
	"github.com/gin-gonic/gin"
	"github.com/sasha-s/go-deadlock"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DataNode TODO - better if we don't use a hardcoded data node
const DataNode = "vega-data.nodes.guru:3007"

func fromBigNumber(number string, decimals uint64) float64 {
	bigNumber, _ := decimal.NewFromString(number)
	modifier := decimal.NewFromInt(10).Pow(decimal.NewFromInt(int64(decimals)))
	return bigNumber.Div(modifier).InexactFloat64()
}

type Market struct {
	TickerId                 string  `json:"ticker_id"`
	BaseCurrency             string  `json:"base_currency"`
	TargetCurrency           string  `json:"target_currency"`
	LastPrice                float64 `json:"last_price"`
	BaseVolume               float64 `json:"base_volume"`
	TargetVolume             float64 `json:"target_volume"`
	Bid                      float64 `json:"bid"`
	Ask                      float64 `json:"ask"`
	High                     float64 `json:"high"`
	Low                      float64 `json:"low"`
	ProductType              string  `json:"product_type"`
	OpenInterest             float64 `json:"open_interest"`
	OpenInterestUsd          float64 `json:"open_interest_usd"`
	IndexPrice               float64 `json:"index_price"`
	IndexName                string  `json:"index_name"`
	IndexCurrency            string  `json:"index_currency"`
	CreationTimestamp        int64   `json:"creation_timestamp"`
	StartTimestamp           int64   `json:"start_timestamp"`
	ExpiryTimestamp          int64   `json:"expiry_timestamp"`
	EndTimestamp             int64   `json:"end_timestamp"`
	FundingRate              float64 `json:"funding_rate"`
	NextFundingRate          float64 `json:"next_funding_rate"`
	NextFundingRateTimestamp int64   `json:"next_funding_rate_timestamp"`
	ContractType             string  `json:"contract_type"`
	ContractPrice            float64 `json:"contract_price"`
	ContractPriceCurrency    string  `json:"contract_price_currency"`
}

func getSecondsToFunding(market *vega.Market, vegaTime int64) int64 {
	perp := market.TradableInstrument.Instrument.GetPerpetual()
	timeTrigger := perp.DataSourceSpecForSettlementSchedule.Data.GetInternal().GetTimeTrigger().Triggers[0]
	vegaTime = vegaTime / 1000000000
	return timeTrigger.Every - ((vegaTime - timeTrigger.GetInitial()) % timeTrigger.Every)
}

func NewMarket(
	market *vega.Market,
	marketData *vega.MarketData,
	candles []*apipb.Candle,
	asset *vega.Asset,
	vegaTime int64,
) *Market {
	dp := market.DecimalPlaces
	pdp := uint64(market.PositionDecimalPlaces)
	var baseCurrency string
	var targetCurrency string
	var enactmentTimestamp string
	var settlementTimestamp string
	for _, tag := range market.TradableInstrument.Instrument.Metadata.Tags {
		if strings.Contains(tag, "base:") {
			baseCurrency = strings.ReplaceAll(tag, "base:", "")
		} else if strings.Contains(tag, "quote:") {
			targetCurrency = strings.ReplaceAll(tag, "quote:", "")
		} else if strings.Contains(tag, "enactment:") {
			enactmentTimestamp = strings.ReplaceAll(tag, "enactment:", "")
		} else if strings.Contains(tag, "settlement:") {
			settlementTimestamp = strings.ReplaceAll(tag, "settlement:", "")
		}
	}
	high := 0.0
	low := 0.0
	baseVolume := decimal.Zero
	targetVolume := decimal.Zero
	for _, candle := range candles {
		h := fromBigNumber(candle.High, dp)
		l := fromBigNumber(candle.Low, dp)
		v := candle.Volume
		nv := candle.Notional
		if high == 0 || h > high {
			high = h
		}
		if low == 0 || l < low {
			low = l
		}
		baseVolume = baseVolume.Add(decimal.NewFromInt(int64(v)))
		targetVolume = targetVolume.Add(decimal.NewFromInt(int64(nv)))
	}
	future := market.TradableInstrument.Instrument.GetFuture()
	spot := market.TradableInstrument.Instrument.GetSpot()
	perp := market.TradableInstrument.Instrument.GetPerpetual()
	productType := ""
	startTimestamp := int64(0)
	endTimestamp := int64(0)
	fundingRate := 0.0
	nextFundingRate := 0.0
	nextFundingRateTimestamp := int64(0)
	indexPrice := 0.0
	var indexName string
	var indexCurrency string
	if future != nil {
		productType = "Futures"
		startTime, _ := time.Parse(time.RFC3339, enactmentTimestamp)
		endTime, _ := time.Parse(time.RFC3339, settlementTimestamp)
		startTimestamp = startTime.UnixMilli()
		endTimestamp = endTime.UnixMilli()
	} else if spot != nil {
		productType = "Spot"
	} else if perp != nil {
		productType = "Perpetual"
		indexPrice = fromBigNumber(marketData.ProductData.GetPerpetualData().ExternalTwap, asset.GetDetails().Decimals)
		fundingRate, _ = strconv.ParseFloat(marketData.ProductData.GetPerpetualData().FundingRate, 64)
		nextFundingRate = fundingRate
		indexCurrency = targetCurrency
		indexName = baseCurrency
		startTime, _ := time.Parse(time.RFC3339, enactmentTimestamp)
		startTimestamp = startTime.UnixMilli()
		secondsToFunding := getSecondsToFunding(market, vegaTime)
		nextFundingRateTimestamp = ((vegaTime + (secondsToFunding * 1000000000)) / 1000000000) * 1000
		endTimestamp = nextFundingRateTimestamp
	}
	lastPrice := fromBigNumber(marketData.LastTradedPrice, dp)
	code := strings.ReplaceAll(market.TradableInstrument.Instrument.Code, "/", "")
	openInterest := fromBigNumber(fmt.Sprintf("%d", marketData.OpenInterest), pdp)
	return &Market{
		TickerId:                 code,
		BaseCurrency:             baseCurrency,
		TargetCurrency:           targetCurrency,
		LastPrice:                lastPrice,
		BaseVolume:               fromBigNumber(baseVolume.String(), pdp),
		TargetVolume:             fromBigNumber(targetVolume.String(), dp+pdp),
		Bid:                      fromBigNumber(marketData.BestBidPrice, dp),
		Ask:                      fromBigNumber(marketData.BestOfferPrice, dp),
		High:                     high,
		Low:                      low,
		ProductType:              productType,
		OpenInterest:             openInterest,
		OpenInterestUsd:          openInterest * lastPrice,
		IndexPrice:               indexPrice,
		IndexName:                indexName,
		IndexCurrency:            indexCurrency,
		StartTimestamp:           startTimestamp,
		CreationTimestamp:        startTimestamp,
		EndTimestamp:             endTimestamp,
		ExpiryTimestamp:          endTimestamp,
		FundingRate:              fundingRate,
		NextFundingRate:          nextFundingRate,
		NextFundingRateTimestamp: nextFundingRateTimestamp,
		ContractType:             "Vanilla",
		ContractPrice:            lastPrice,
		ContractPriceCurrency:    targetCurrency,
	}
}

type CoinGeckoOrderBook struct {
	TickerId string      `json:"ticker_id"`
	Bids     [][]float64 `json:"bids"`
	Asks     [][]float64 `json:"asks"`
}

type Api struct {
	markets        []*Market
	orderBooks     map[string]*CoinGeckoOrderBook
	marketsLock    deadlock.RWMutex
	orderBooksLock deadlock.RWMutex
}

func NewApi() *Api {
	return &Api{
		markets:    []*Market{},
		orderBooks: map[string]*CoinGeckoOrderBook{},
	}
}

func (a *Api) GetOrderBook(id string) ([]*vega.PriceLevel, []*vega.PriceLevel) {
	dataNode, _ := grpc.Dial(DataNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	service := apipb.NewTradingDataServiceClient(dataNode)
	resp, err := service.GetLatestMarketDepth(context.Background(), &apipb.GetLatestMarketDepthRequest{MarketId: id})
	dataNode.Close()
	if err != nil {
		fmt.Println(err)
		return nil, nil
	}
	return resp.Buy, resp.Sell
}

func (a *Api) GetCandles(id string, from int64, to int64) []*apipb.Candle {
	dataNode, _ := grpc.Dial(DataNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	service := apipb.NewTradingDataServiceClient(dataNode)
	resp, err := service.ListCandleData(context.Background(), &apipb.ListCandleDataRequest{
		CandleId:      fmt.Sprintf("trades_candle_5_minutes_%s", id),
		FromTimestamp: from,
		ToTimestamp:   to,
	})
	dataNode.Close()
	candles := make([]*apipb.Candle, 0)
	if err != nil {
		fmt.Println(err)
		return candles
	}
	for _, edge := range resp.GetCandles().Edges {
		node := edge.GetNode()
		candles = append(candles, node)
	}
	return candles
}

func (a *Api) GetMarkets() []*vega.Market {
	dataNode, _ := grpc.Dial(DataNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	service := apipb.NewTradingDataServiceClient(dataNode)
	resp, err := service.ListMarkets(context.Background(), &apipb.ListMarketsRequest{})
	dataNode.Close()
	markets := make([]*vega.Market, 0)
	if err != nil {
		fmt.Println(err)
		return markets
	}
	for _, edge := range resp.GetMarkets().GetEdges() {
		node := edge.GetNode()
		if node.State == vega.Market_STATE_ACTIVE {
			markets = append(markets, node)
		}
	}
	return markets
}

func (a *Api) GetAsset(id string) *vega.Asset {
	dataNode, _ := grpc.Dial(DataNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	service := apipb.NewTradingDataServiceClient(dataNode)
	resp, err := service.GetAsset(context.Background(), &apipb.GetAssetRequest{AssetId: id})
	dataNode.Close()
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return resp.GetAsset()
}

func (a *Api) GetMarketData(id string) *vega.MarketData {
	dataNode, _ := grpc.Dial(DataNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	service := apipb.NewTradingDataServiceClient(dataNode)
	resp, err := service.GetLatestMarketData(context.Background(), &apipb.GetLatestMarketDataRequest{MarketId: id})
	dataNode.Close()
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return resp.GetMarketData()
}

func (a *Api) GetVegaTime() int64 {
	dataNode, _ := grpc.Dial(DataNode, grpc.WithTransportCredentials(insecure.NewCredentials()))
	service := apipb.NewTradingDataServiceClient(dataNode)
	resp, err := service.GetVegaTime(context.Background(), &apipb.GetVegaTimeRequest{})
	dataNode.Close()
	if err != nil {
		fmt.Println(err)
		return 0
	}
	return resp.Timestamp
}

func (a *Api) UpdateCache() {
	logging.Logger.Info("updating cache...")
	vegaMarkets := a.GetMarkets()
	markets := make([]*Market, 0)
	endTime := time.Now()
	startTime := endTime.Add(time.Hour * -24)
	vegaTime := a.GetVegaTime()
	for _, market := range vegaMarkets {
		code := strings.ReplaceAll(market.TradableInstrument.Instrument.Code, "/", "")
		logging.Logger.Infof("updating %s", code)
		marketData := a.GetMarketData(market.Id)
		var assetId string
		instrument := market.TradableInstrument.Instrument
		if instrument.GetFuture() != nil {
			assetId = instrument.GetFuture().SettlementAsset
		} else if market.TradableInstrument.Instrument.GetSpot() != nil {
			assetId = instrument.GetSpot().QuoteAsset
		} else if market.TradableInstrument.Instrument.GetPerpetual() != nil {
			assetId = instrument.GetPerpetual().SettlementAsset
		}
		asset := a.GetAsset(assetId)
		if len(assetId) == 0 || asset == nil {
			logging.Logger.Warnf("cannot find settlement asset for market = %s", market.Id)
		} else {
			candles := a.GetCandles(market.Id, startTime.UnixMilli()*1000000, endTime.UnixMilli()*1000000)
			buys, sells := a.GetOrderBook(market.Id)
			var buysArr [][]float64
			var sellsArr [][]float64
			for _, b := range buys {
				qty := fromBigNumber(fmt.Sprintf("%d", b.Volume), uint64(market.PositionDecimalPlaces))
				price := fromBigNumber(b.Price, market.DecimalPlaces)
				buysArr = append(buysArr, []float64{qty, price})
			}
			for _, s := range sells {
				qty := fromBigNumber(fmt.Sprintf("%d", s.Volume), uint64(market.PositionDecimalPlaces))
				price := fromBigNumber(s.Price, market.DecimalPlaces)
				sellsArr = append(sellsArr, []float64{qty, price})
			}
			a.orderBooksLock.Lock()
			a.orderBooks[code] = &CoinGeckoOrderBook{
				TickerId: code,
				Bids:     buysArr,
				Asks:     sellsArr,
			}
			a.orderBooksLock.Unlock()
			markets = append(markets, NewMarket(market, marketData, candles, asset, vegaTime))
		}
	}
	a.marketsLock.Lock()
	a.markets = markets
	a.marketsLock.Unlock()
	logging.Logger.Info("cache updated!")
}

func (a *Api) Start() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.GET("/contracts", func(c *gin.Context) {
		a.marketsLock.RLock()
		c.JSON(http.StatusOK, a.markets)
		a.marketsLock.RUnlock()
	})
	r.GET("/orderbook/:ticker", func(c *gin.Context) {
		a.orderBooksLock.RLock()
		c.JSON(http.StatusOK, a.orderBooks[c.Param("ticker")])
		a.orderBooksLock.RUnlock()
	})
	// TODO - I guess we don't want to hardcode the port
	logging.Logger.Info("rest api available at http://localhost:9999")
	err := r.Run(":9999")
	if err != nil {
		log.Fatal(err)
	}
}

func (a *Api) Init() {
	a.UpdateCache()
	go func() {
		for range time.NewTicker(time.Minute * 5).C {
			a.UpdateCache()
		}
	}()
	a.Start()
}
