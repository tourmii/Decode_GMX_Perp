import argparse
import json
import math
import time
import pymongo


client = None
collection_configs = None
collection_gmx_log = None
collection_accounts = None
collection_opening_positions = None
collection_closed_positions = None


def process_increase_event(doc):
    positionKey = doc['positionKey']
    owner = doc['account']
    sizeUsdDelta = doc['sizeDeltaUsd']
    collateralUsdDelta = doc['collateralDeltaAmount']
    positionSizeUsd = doc['sizeInUsd']
    price = doc['executionPrice']
    positionSide = 'Long' if doc['isLong'] else 'Short'
    timestamp = doc['timestamp']
    transaction_hash = doc['transactionHash']
    asset = doc['indexTokenName']

    doc_account = collection_accounts.find_one({'_id': owner})

    if doc_account is not None:
        positionKeys = doc_account.get('positionKeys', [])
        collateralUsd = doc_account.get('collateralUsd', 0)

        if positionKey not in positionKeys:
            positionKeys.append(positionKey)

        collection_accounts.update_one(
            {'_id': owner},
            {'$set': {
                'positionKeys': positionKeys,
                'collateralUsd': collateralUsd + collateralUsdDelta,
            }}
        )
    else:
        new_account = {
            '_id': owner,
            'account': owner,
            'positionKeys': [positionKey],
            'openingSizeUsd': 0,
            'collateralUsd': collateralUsdDelta,
            'realizedPnl': 0,
            'unrealizedPnl': 0,
            'openingPositionCount': 0,
            'closedPositionCount': 0,
            'profitedPositionCount': 0,
            'profitableRatio': 0,
            'PNL': 0,
            'ROI': 0
        }
        collection_accounts.insert_one(new_account)

    doc_opening_position = collection_opening_positions.find_one({'_id': positionKey})
    leverage = math.ceil(sizeUsdDelta / collateralUsdDelta * 10) / 10 if collateralUsdDelta > 0 else 0
    new_log = {
        'timestamp': timestamp,
        'action': 'Open',
        'collateralUsd': collateralUsdDelta,
        'leverage': leverage,
        'sizeUsd': sizeUsdDelta,
        'price': price,
        'transaction_hash': transaction_hash
    }

    if doc_opening_position is not None:
        logs = doc_opening_position.get('logs', [])
        logs.append(new_log)
        old_entryPrice = doc_opening_position['entryPrice']
        old_sizeUsd = doc_opening_position.get('sizeUsd', 0)
        new_entryPrice = (old_entryPrice * old_sizeUsd + price * sizeUsdDelta) / (old_sizeUsd + sizeUsdDelta)

        collection_opening_positions.update_one(
            {'_id': positionKey},
            {'$set': {
                'logs': logs,
                'entryPrice': new_entryPrice,
                'sizeUsd': positionSizeUsd
            }}
        )
    else:
        new_position = {
            '_id': positionKey,
            'positionKey': positionKey,
            'ownerAccount': owner,
            'asset': asset,
            'side': positionSide,
            'sizeUsd': positionSizeUsd,
            'entryPrice': price,
            'unrealizedPnl': 0,
            'logs': [new_log]
        }
        collection_opening_positions.insert_one(new_position)


def process_decrease_event(doc):
    if 'account' not in doc:
        return

    positionKey = doc['positionKey']
    owner = doc['account']
    price = doc['executionPrice']
    positionSide = 'Long' if doc['isLong'] else 'Short'
    positionSizeUsd = doc['sizeInUsd']
    timestamp = doc['timestamp']
    transaction_hash = doc['transactionHash']

    if 'sizeDeltaUsd' not in doc:
        sizeUsdDelta = positionSizeUsd
        positionSizeUsd = 0
    else:
        sizeUsdDelta = doc.get('sizeDeltaUsd')

    asset = doc['indexTokenName']
    pnlDelta = doc['basePnlUsd']

    doc_account = collection_accounts.find_one({'_id': owner})
    doc_opening_position = collection_opening_positions.find_one({'_id': positionKey})
    doc_closed_order = collection_closed_positions.find_one({'_id': positionKey})

    if doc_account is None:
        new_account = {
            '_id': owner,
            'account': owner,
            'positionKeys': [positionKey],
            'openingSizeUsd': 0,
            'collateralUsd': 0,
            'realizedPnl': pnlDelta,
            'unrealizedPnl': 0,
            'openingPositionCount': 0,
            'closedPositionCount': 1,
            'profitedPositionCount': 1 if pnlDelta > 0 else 0,
            'profitableRatio': 0,
            'PNL': 0,
            'ROI': 0
        }
        collection_accounts.insert_one(new_account)
    else:
        positionKeys = doc_account.get('positionKeys', [])
        if positionKey not in positionKeys:
            positionKeys.append(positionKey)

        realizedPnl = doc_account.get('realizedPnl', 0)
        new_realizedPnl = realizedPnl + pnlDelta

        closedPositionCount = doc_account.get('closedPositionCount', 0) + 1
        profitedPositionCount = doc_account.get('profitedPositionCount', 0) + (1 if pnlDelta > 0 else 0)

        collection_accounts.update_one(
            {'_id': owner},
            {'$set': {
                'positionKeys': positionKeys,
                'realizedPnl': new_realizedPnl,
                'closedPositionCount': closedPositionCount,
                'profitedPositionCount': profitedPositionCount
            }}
        )

    if sizeUsdDelta <= 0 and positionSizeUsd <= 0:
        percentageClosed = 100
    else:
        percentageClosed = round(sizeUsdDelta / (sizeUsdDelta + positionSizeUsd) * 100)

    if doc.get('orderType') == 7:
        type_close = 'Liquidate'
    else:
        type_close = 'Close'

    new_close_log = {
        'timestamp': timestamp,
        'action': type_close,
        'realizedPnl': pnlDelta,
        'sizeUsd': sizeUsdDelta,
        'percentageClosed': percentageClosed,
        'price': price,
        'transaction_hash': transaction_hash
    }

    if doc_closed_order is None:
        new_closed_position_log = {
            '_id': positionKey,
            'positionKey': positionKey,
            'ownerAccount': owner,
            'asset': asset,
            'side': positionSide,
            'realizedPnl': pnlDelta,
            'logs': [new_close_log]
        }
        collection_closed_positions.insert_one(new_closed_position_log)
        doc_closed_order = collection_closed_positions.find_one({'_id': positionKey})
    else:
        logs = doc_closed_order.get('logs', [])
        realizedPnl = doc_closed_order.get('realizedPnl', 0) + pnlDelta
        logs.append(new_close_log)
        collection_closed_positions.update_one(
            {'_id': positionKey},
            {'$set': {
                'realizedPnl': realizedPnl,
                'logs': logs
            }}
        )

    if positionSizeUsd > 0:
        if doc_opening_position is not None:
            collection_opening_positions.update_one(
                {'_id': positionKey},
                {'$set': {
                    'sizeUsd': positionSizeUsd
                }}
            )
        else:
            new_position = {
                '_id': positionKey,
                'positionKey': positionKey,
                'ownerAccount': owner,
                'asset': asset,
                'side': positionSide,
                'sizeUsd': positionSizeUsd,
                'entryPrice': price,
                'unrealizedPnl': 0,
                'logs': []
            }
            collection_opening_positions.insert_one(new_position)
    else:
        if doc_opening_position is not None:
            open_logs = doc_opening_position.get('logs', [])
            closed_logs = doc_closed_order.get('logs', [])
            merged_logs = open_logs + closed_logs
            merged_logs = sorted(merged_logs, key=lambda item: item['timestamp'], reverse=True)
            collection_opening_positions.delete_one({'_id': positionKey})
            collection_closed_positions.update_one(
                {'_id': positionKey},
                {'$set': {
                    'logs': merged_logs
                }}
            )


def gmx_events_analytics():

    cfg = collection_configs.find_one({'_id': 'gmx_last_updated_event'})
    last_crawl_block = cfg.get('last_updated_at_block_number', 0) if cfg else 0
    enrich = collection_configs.find_one({'_id': 'last_updated_gmx_analytics'})
    last_enrich_event = enrich.get('last_updated_at_block_number', -1) if enrich else -1

    start_block = last_enrich_event + 1
    end_block = start_block + 1000 - 1

    if end_block > last_crawl_block:
        return False

    start_timestamp = int(time.time())
    print(f"Process Block: {start_block} - {end_block}")

    _filter = {
        'blockNumber': {
            '$gte': start_block,
            '$lte': end_block
        }
    }

    cursor = collection_gmx_log.find(_filter).sort("blockNumber", 1)
    for doc in cursor:
        event_type = doc.get('eventName')
        if event_type == 'PositionIncrease':
            process_increase_event(doc)
        elif event_type == 'PositionDecrease':
            process_decrease_event(doc)

    collection_configs.update_one(
        {'_id': 'last_updated_gmx_analytics'},
        {'$set': {'last_updated_at_block_number': end_block}},
        upsert=True
    )

    end_timestamp = int(time.time())
    print(f"Done in {end_timestamp - start_timestamp}s")
    return True


def parse_args():
    parser = argparse.ArgumentParser(description='GMX Events Analytics')
    parser.add_argument('--uri', required=True, help='MongoDB connection URI')
    parser.add_argument('--db', required=True, help='database name')
    parser.add_argument('--configs', default='configs')
    parser.add_argument('--events', default='gmx_events')
    parser.add_argument('--accounts', default='gmx_accounts')
    parser.add_argument('--opening', default='gmx_opening_positions')
    parser.add_argument('--closed', default='gmx_closed_positions')
    parser.add_argument('--interval', type=int, default=10)
    return parser.parse_args()


def main():
    global client
    global collection_configs, collection_gmx_log, collection_accounts
    global collection_opening_positions, collection_closed_positions

    args = parse_args()

    client = pymongo.MongoClient(args.uri)
    db = client[args.db]
    collection_configs = db[args.configs]
    collection_gmx_log = db[args.events]
    collection_accounts = db[args.accounts]
    collection_opening_positions = db[args.opening]
    collection_closed_positions = db[args.closed]

    while True:
        success = gmx_events_analytics()
        if not success:
            print(f'Nothing to sync. Sleeping {args.interval}s...')
            time.sleep(args.interval)


if __name__ == '__main__':
    main()
