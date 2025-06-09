import time

import pymongo, requests


client_market_trading_tracker = pymongo.MongoClient('mongodb://marketTrading_writer:marketTradingWriter_5jUsnwXtxGz87T3H@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017/')

collection_accounts = client_market_trading_tracker['MarketTradingTracker']['gmx_accounts']
collection_opening_positions = client_market_trading_tracker['MarketTradingTracker']['gmx_opening_positions']
collection_closed_positions = client_market_trading_tracker['MarketTradingTracker']['gmx_closed_positions']
collection_market = client_market_trading_tracker['MarketTradingTracker']['gmx_market']
docs_closed_positions = collection_closed_positions.find({}, {'positionKey': 1, 'logs': 1})


closed_positions_logs = {}


def get_price():
    current_prices = {}
    response = requests.get("https://arbitrum-api.gmxinfra.io/prices/tickers")
    if response.status_code == 200:
        data = response.json()
        symbols = [token['tokenSymbol'] for token in data]
        name_variants = []

        for symbol in symbols:
            name_variants.extend([symbol, 'k'+symbol,'t'+symbol, 'm'+symbol])
        market_infos = collection_market.find({"name": {"$in": name_variants}})
        symbol_decimals = {}

        for info in market_infos:
            for prefix in ['', 't', 'k', 'm']:
                if info['name'].startswith(prefix):
                    symbol_decimals[info['name'].lstrip('ktm')] = info['decimals']

        for token in data:
            price = (float(token['minPrice']) + float(token['maxPrice'])) / 2
            symbol = token['tokenSymbol']
            decimals = symbol_decimals.get(symbol)
            if decimals is None:
                continue
            current_prices[symbol] = price / (10 ** (30-decimals))

    return current_prices

print(get_price())