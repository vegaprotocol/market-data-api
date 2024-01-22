# Market Data API

This service fetches data from Mainnet and exposes it in the format required by CoinGecko and CoinMarketCap to qualify for listing as an exchange.

The service exposes two endpoints:

* `/contracts` displays a list of active markets
* `/orderbook/{ticker_id}` displays the market depth for the specified ticker

## Build & Run

`go build && ./market-data-api`

The service will be available at http://localhost:9999

## Caching

The service periodically fetches the data from a hardcoded Mainnet data node and stores it in memory. It currently uses the Nodes Guru data node and updates the cache once every 5 minutes.
