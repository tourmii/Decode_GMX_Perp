import argparse
import time
import pymongo

def update_traded_assets(collection_accounts, collection_opening_positions, collection_closed_positions):
    cursor_open = collection_opening_positions.find({}, {'positionKey': 1, 'asset': 1})
    cursor_closed = collection_closed_positions.find({}, {'positionKey': 1, 'asset': 1})

    tmp = {}
    for doc_open in cursor_open:
        tmp[doc_open['positionKey']] = doc_open['asset']
    for doc_closed in cursor_closed:
        tmp[doc_closed['positionKey']] = doc_closed['asset']

    cursor_accounts = collection_accounts.find({})
    result = {}
    for doc in cursor_accounts:
        account = doc['account']
        positionKeys = doc.get('positionKeys', [])
        assets = []
        for positionKey in positionKeys:
            if positionKey in tmp:
                asset = tmp[positionKey]
                if asset not in assets:
                    assets.append(asset)
        result[account] = assets

    bulks = []
    for account, assets in result.items():
        bulks.append(
            pymongo.UpdateOne(
                {'_id': account},
                {'$set': {'tradedAssets': assets}}
            )
        )

    if bulks:
        collection_accounts.bulk_write(bulks)


def parse_args():
    parser = argparse.ArgumentParser(description="GMX Traded Assets Updater")
    parser.add_argument("--uri",required=True)
    parser.add_argument("--db",required=True)
    parser.add_argument("--accounts", default="gmx_accounts")
    parser.add_argument("--opening", default="gmx_opening_positions",)
    parser.add_argument("--closed", default="gmx_closed_positions",)
    parser.add_argument("--interval", type=int, default=3600)
    return parser.parse_args()


def main():
    args = parse_args()

    client = pymongo.MongoClient(args.uri)
    db = client[args.db]
    collection_accounts = db[args.accounts]
    collection_opening_positions = db[args.opening]
    collection_closed_positions = db[args.closed]

    while True:
        print("Updating Traded Assets of all Accounts...")
        try:
            update_traded_assets(
                collection_accounts,
                collection_opening_positions,
                collection_closed_positions
            )
        except Exception as e:
            print(f"Error in update_traded_assets: {e}")
        print("Done")
        print(f"Sleep {args.interval}s...")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
