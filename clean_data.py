import json
from web3 import Web3
import pymongo

web3 = Web3(Web3.HTTPProvider('https://arb1.arbitrum.io/rpc'))

TOKEN_INFO_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    }
]


client         = pymongo.MongoClient("mongodb://localhost:27017/")
db             = client["MarketTradingTracker"]
market_data    = db["gmx_market"]
token_info     = db["token_info"]    

DEFAULT_USD_SCALE = 30


def get_token_info(token_addr: str) -> dict:
    addr = Web3.to_checksum_address(token_addr)

    doc = token_info.find_one({"_id": addr})
    if doc:
        return {"decimals": doc["decimals"], "symbol": doc["symbol"]}

    contract = web3.eth.contract(address=addr, abi=TOKEN_INFO_ABI)
    d = contract.functions.decimals().call()
    s = contract.functions.symbol().call()

    token_info.insert_one({
        "_id": addr,
        "decimals": d,
        "symbol": s
    })

    return {"decimals": d, "symbol": s}


def process_event(event: dict) -> dict:
    """
    Given one GMX PositionDecrease event JSON, normalize all
    integer fields into human‐readable floats/strings.
    """
    mkt_id   = event["market"]
    mkt_doc  = market_data.find_one({"_id": mkt_id})
    dec_idx  = mkt_doc["decimals"]
    event["indexTokenName"]     = mkt_doc["name"]
    event["indexTokenDecimals"] = dec_idx

    col_addr = event["collateralToken"]
    info     = get_token_info(col_addr)
    dec_col  = info["decimals"]
    sym_col  = info["symbol"]

    event["collateralTokenSymbol"]   = sym_col
    event["collateralTokenDecimals"] = dec_col

    for usd_field in ("sizeInUsd", "sizeDeltaUsd",
                      "priceImpactUsd", "basePnlUsd", "uncappedBasePnlUsd",
                      "borrowingFactor"):
        if usd_field in event:
            event[usd_field] = event[usd_field] / 10**DEFAULT_USD_SCALE

    if "sizeInTokens" in event:
        event["sizeInTokens"] = event["sizeInTokens"] / 10**dec_idx
    if "sizeDeltaInTokens" in event:
        event["sizeDeltaInTokens"] = event["sizeDeltaInTokens"] / 10**dec_idx

    if "collateralAmount" in event:
        event["collateralAmount"] = event["collateralAmount"] / 10**dec_col
    if "collateralDeltaAmount" in event:
        event["collateralDeltaAmount"] = event["collateralDeltaAmount"] / 10**dec_col

    event["executionPrice"]            = event["executionPrice"] / 10**(DEFAULT_USD_SCALE - dec_idx)
    event["indexTokenPrice.max"]    = event["indexTokenPrice.max"] / 10**(DEFAULT_USD_SCALE - dec_idx)
    event["indexTokenPrice.min"]    = event["indexTokenPrice.min"] / 10**(DEFAULT_USD_SCALE - dec_idx)
    event["collateralTokenPrice.max"] = event["collateralTokenPrice.max"] / 10**(DEFAULT_USD_SCALE - dec_col)
    event["collateralTokenPrice.min"] = event["collateralTokenPrice.min"] / 10**(DEFAULT_USD_SCALE - dec_col)

    event["fundingFeeAmountPerSize"]            = event["fundingFeeAmountPerSize"] / 10**dec_col
    event["longTokenClaimableFundingAmountPerSize"]  = event["longTokenClaimableFundingAmountPerSize"] / 10**(30)
    event["shortTokenClaimableFundingAmountPerSize"] = event["shortTokenClaimableFundingAmountPerSize"] / 10**(30)

    if "priceImpactAmount" in event:
        event["priceImpactAmount"] = event["priceImpactAmount"] / 10**(dec_idx)

    return event



if __name__ == "__main__":
    with open("gmx_events_output.json") as f:
        raw_events = json.load(f)

    for ev in raw_events:
        normalized = process_event(ev)
        print(json.dumps(normalized, indent=2))
