import argparse
import time

import pymongo
import requests

client = None
collection_accounts = None
collection_opening_positions = None
collection_closed_positions = None
collection_market = None


def normalize_token(token):
    if token and token[0].islower():
        return token[1:]
    return token


def get_price():
    current_prices = {}
    response = requests.get("https://arbitrum-api.gmxinfra.io/prices/tickers")
    if response.status_code == 200:
        data = response.json()
        symbols = [token['tokenSymbol'] for token in data]
        name_variants = []

        for symbol in symbols:
            name_variants.extend([symbol, 'k' + symbol, 't' + symbol, 'm' + symbol])

        market_infos = collection_market.find({"name": {"$in": name_variants}})
        symbol_decimals = {}

        for info in market_infos:
            for prefix in ['', 'k', 't', 'm']:
                if info['name'].startswith(prefix):
                    key = info['name'].lstrip('ktm')
                    symbol_decimals[key] = info['decimals']

        for token in data:
            price = (float(token['minPrice']) + float(token['maxPrice'])) / 2
            symbol = token['tokenSymbol']
            decimals = symbol_decimals.get(symbol)
            if decimals is None:
                continue
            current_prices[symbol] = price / (10 ** (30 - decimals))

    return current_prices


def update_account_detail():
    global closed_positions_logs
    closed_positions_logs = {}

    current_prices = get_price()

    docs_closed_positions = collection_closed_positions.find({}, {'positionKey': 1, 'logs': 1})
    bulks_closed_positions = []
    for doc in docs_closed_positions:
        positionKey = doc['positionKey']
        logs = doc.get('logs', [])
        closed_positions_logs[positionKey] = logs

        lastClosedAt = 0
        for item in logs:
            lastClosedAt = max(lastClosedAt, item.get('timestamp', 0))

        bulks_closed_positions.append(pymongo.UpdateOne(
            {'_id': positionKey},
            {'$set': {'lastClosedAt': lastClosedAt}}
        ))

    if bulks_closed_positions:
        collection_closed_positions.bulk_write(bulks_closed_positions)

    result = {}          
    no_update_ROI = []   

    bulks_accounts = []
    bulks_opening_positions = []

    cursor_opening = collection_opening_positions.find({})
    for doc in cursor_opening:
        positionKey = doc['positionKey']
        sizeUsd = doc.get('sizeUsd', 0)
        entryPrice = doc.get('entryPrice', 0)
        ownerAccount = doc['ownerAccount']
        asset = doc['asset']
        side = doc['side']
        logs = doc.get('logs', [])

        if ownerAccount not in result:
            result[ownerAccount] = {
                'openingSizeUsd': 0,
                'unrealizedPnl': 0,
                'openingPositionCount': 0
            }

        current_price = current_prices.get(normalize_token(asset))
        if current_price is None:
            continue

        total_size_usd = 0
        firstOpenedAt = float('inf')
        for item in logs:
            total_size_usd += item.get('sizeUsd', 0)
            firstOpenedAt = min(firstOpenedAt, item.get('timestamp', float('inf')))
        if firstOpenedAt == float('inf'):
            firstOpenedAt = 1735689600

        if sizeUsd > total_size_usd:
            no_update_ROI.append(ownerAccount)

        if side == "Long":
            unrealized_pnl = sizeUsd * ((current_price - entryPrice) / entryPrice)
        else:  
            unrealized_pnl = sizeUsd * ((entryPrice - current_price) / entryPrice)

        bulks_opening_positions.append(pymongo.UpdateOne(
            {'_id': positionKey},
            {'$set': {
                'firstOpenedAt': firstOpenedAt,
                'unrealizedPnl': unrealized_pnl
            }}
        ))

        result[ownerAccount]['openingSizeUsd'] += sizeUsd
        result[ownerAccount]['openingPositionCount'] += 1
        result[ownerAccount]['unrealizedPnl'] += unrealized_pnl

    if bulks_opening_positions:
        collection_opening_positions.bulk_write(bulks_opening_positions)

    for account, vals in result.items():
        bulks_accounts.append(pymongo.UpdateOne(
            {'_id': account},
            {'$set': vals}
        ))
    if bulks_accounts:
        collection_accounts.bulk_write(bulks_accounts)

    bulks_final = []
    cursor_account = collection_accounts.find({})
    for doc in cursor_account:
        account = doc['account']
        profitedPositionCount = doc.get('profitedPositionCount', 0)
        closedPositionCount = doc.get('closedPositionCount', 0)
        realizedPnl = doc.get('realizedPnl', 0)
        collateralUsd = doc.get('collateralUsd', 0)
        unrealizedPnl = doc.get('unrealizedPnl', 0)

        if account not in result:
            update_data = {
                'openingSizeUsd': 0,
                'unrealizedPnl': 0,
                'openingPositionCount': 0,
                'PNL': realizedPnl
            }
            if collateralUsd > 0 and account not in no_update_ROI:
                update_data['ROI'] = (realizedPnl / collateralUsd) * 100
            if closedPositionCount > 0:
                update_data['profitableRatio'] = profitedPositionCount / closedPositionCount
        else:
            update_data = {
                'PNL': realizedPnl + unrealizedPnl
            }
            if closedPositionCount > 0:
                update_data['profitableRatio'] = profitedPositionCount / closedPositionCount
            if collateralUsd > 0 and account not in no_update_ROI:
                update_data['ROI'] = ((realizedPnl + unrealizedPnl) / collateralUsd) * 100

        bulks_final.append(pymongo.UpdateOne(
            {'_id': account},
            {'$set': update_data}
        ))

    if bulks_final:
        collection_accounts.bulk_write(bulks_final)


def parse_args():
    parser = argparse.ArgumentParser(description='Update GMX Account Details')
    parser.add_argument('--uri', required=True, help='MongoDB connection URI')
    parser.add_argument('--db', required=True, help='Database name')
    parser.add_argument('--accounts', default='gmx_accounts')
    parser.add_argument('--opening', default='gmx_opening_positions')
    parser.add_argument('--closed', default='gmx_closed_positions')
    parser.add_argument('--markets', default='gmx_market')
    parser.add_argument('--interval', type=int, default=30)
    return parser.parse_args()


def main():
    global client
    global collection_accounts, collection_opening_positions, collection_closed_positions, collection_market

    args = parse_args()

    client = pymongo.MongoClient(args.uri)
    db = client[args.db]
    collection_accounts = db[args.accounts]
    collection_opening_positions = db[args.opening]
    collection_closed_positions = db[args.closed]
    collection_market = db[args.markets]

    while True:
        print('Updating...')
        try:
            update_account_detail()
        except Exception as e:
            print("Error in update_account_detail():", e)
        print('Done')
        print(f'Sleeping for {args.interval}s...')
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
