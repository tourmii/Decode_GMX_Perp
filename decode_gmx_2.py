import json
import argparse
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from hexbytes import HexBytes
from eth_utils import to_hex, to_checksum_address
from eth_abi import decode
import pymongo
from time import sleep


CONTRACT_ADDRESS = "0xC8ee91A54287DB53897056e12D9819156D3822Fb"
RPC_URL = "https://arb1.arbitrum.io/rpc"

EVENT_SIGNATURE = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"

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

DEFAULT_USD_SCALE = 30

with open("abi_emitter.json", "r") as f:
    EVENT_ABI = json.load(f)

def get_token_info(web3_instance, token_addr: str, token_info_collection) -> dict:
    """Get token information (decimals and symbol) from cache or blockchain"""
    addr = Web3.to_checksum_address(token_addr)

    doc = token_info_collection.find_one({"_id": addr})
    if doc:
        return {"decimals": doc["decimals"], "symbol": doc["symbol"]}

    try:
        contract = web3_instance.eth.contract(address=addr, abi=TOKEN_INFO_ABI)
        d = contract.functions.decimals().call()
        s = contract.functions.symbol().call()

        token_info_collection.insert_one({
            "_id": addr,
            "decimals": d,
            "symbol": s
        })

        return {"decimals": d, "symbol": s}
    except Exception as e:
        print(f"Error getting token info for {addr}: {e}")
        return {"decimals": 18, "symbol": "UNKNOWN"} 

def process_event(event: dict, web3_instance, market_data_collection, token_info_collection) -> dict:
    """
    Process and normalize a GMX event, converting raw integers to human-readable values
    """
    event = event.copy()
    
    field_mappings = {
        "indexTokenPrice.max": "indexTokenPriceMax",
        "indexTokenPrice.min": "indexTokenPriceMin", 
        "collateralTokenPrice.max": "collateralTokenPriceMax",
        "collateralTokenPrice.min": "collateralTokenPriceMin",
        "values.priceImpactDiffUsd": "priceImpactDiffUsd"
    }
    
    for old_field, new_field in field_mappings.items():
        if old_field in event:
            event[new_field] = event[old_field]
            del event[old_field]

    timestamp_mappings = {
        "decreasedAtTime": "timestamp",
        "increasedAtTime": "timestamp"
    }
    
    for old_field, new_field in timestamp_mappings.items():
        if old_field in event:
            event[new_field] = event[old_field]
            del event[old_field]
    
    if "market" not in event:
        for key, value in event.items():
            if isinstance(value, int):
                event[key] = str(value)
        return event
    
    try:
        mkt_id = event["market"]
        mkt_doc = market_data_collection.find_one({"_id": mkt_id})
        
        if not mkt_doc:
            print(f"Market data not found for market ID: {mkt_id}")
            for key, value in event.items():
                if isinstance(value, int):
                    event[key] = str(value)
            return event
            
        dec_idx = mkt_doc["decimals"]
        event["indexTokenName"] = mkt_doc["name"]
        event["indexTokenDecimals"] = dec_idx

        if "collateralToken" in event:
            col_addr = event["collateralToken"]
            info = get_token_info(web3_instance, col_addr, token_info_collection)
            dec_col = info["decimals"]
            sym_col = info["symbol"]

            event["collateralTokenSymbol"] = sym_col
            event["collateralTokenDecimals"] = dec_col
        else:
            dec_col = 18  

        usd_fields = [
            "sizeInUsd", "sizeDeltaUsd", "priceImpactUsd", "basePnlUsd", 
            "uncappedBasePnlUsd", "borrowingFactor", "priceImpactDiffUsd"
        ]
        
        for usd_field in usd_fields:
            if usd_field in event and event[usd_field] is not None:
                try:
                    val = int(event[usd_field]) if isinstance(event[usd_field], str) else event[usd_field]
                    event[usd_field] = val / 10**DEFAULT_USD_SCALE
                except (ValueError, TypeError):
                    pass

        token_size_fields = ["sizeInTokens", "sizeDeltaInTokens"]
        for field in token_size_fields:
            if field in event and event[field] is not None:
                try:
                    val = int(event[field]) if isinstance(event[field], str) else event[field]
                    event[field] = val / 10**dec_idx
                except (ValueError, TypeError):
                    pass

        collateral_fields = ["collateralAmount", "collateralDeltaAmount"]
        for field in collateral_fields:
            if field in event and event[field] is not None:
                try:
                    val = int(event[field]) if isinstance(event[field], str) else event[field]
                    event[field] = val / 10**dec_col
                except (ValueError, TypeError):
                    pass

        price_fields = {
            "executionPrice": DEFAULT_USD_SCALE - dec_idx,
            "indexTokenPriceMax": DEFAULT_USD_SCALE - dec_idx,
            "indexTokenPriceMin": DEFAULT_USD_SCALE - dec_idx,
            "collateralTokenPriceMax": DEFAULT_USD_SCALE - dec_col,
            "collateralTokenPriceMin": DEFAULT_USD_SCALE - dec_col,
        }
        
        for field, scale in price_fields.items():
            if field in event and event[field] is not None:
                try:
                    val = int(event[field]) if isinstance(event[field], str) else event[field]
                    event[field] = val / 10**scale
                except (ValueError, TypeError):
                    pass

        funding_fields = {
            "fundingFeeAmountPerSize": dec_col,
            "longTokenClaimableFundingAmountPerSize": 30,
            "shortTokenClaimableFundingAmountPerSize": 30,
        }
        
        for field, scale in funding_fields.items():
            if field in event and event[field] is not None:
                try:
                    val = int(event[field]) if isinstance(event[field], str) else event[field]
                    event[field] = val / 10**scale
                except (ValueError, TypeError):
                    pass

        if "priceImpactAmount" in event and event["priceImpactAmount"] is not None:
            try:
                val = int(event["priceImpactAmount"]) if isinstance(event["priceImpactAmount"], str) else event["priceImpactAmount"]
                event["priceImpactAmount"] = val / 10**dec_idx
            except (ValueError, TypeError):
                pass

    except Exception as e:
        print(f"Error processing event: {e}")
        for key, value in event.items():
            if isinstance(value, int):
                event[key] = str(value)
        
    return event

def get_contract_events(w3, from_block, to_block=None):
    if to_block is None:
        to_block = from_block

    print(f"Using event signature: {EVENT_SIGNATURE}")

    CHUNK_SIZE = 1000
    all_logs = []

    current_block = from_block
    while current_block <= to_block:
        chunk_end = min(current_block + CHUNK_SIZE - 1, to_block)
        print(f"Fetching logs for blocks {current_block} to {chunk_end}...")

        logs = w3.eth.get_logs({
            "fromBlock": current_block,
            "toBlock": chunk_end,
            "address": Web3.to_checksum_address(CONTRACT_ADDRESS),
            "topics": [EVENT_SIGNATURE]
        })

        print(f"Found {len(logs)} logs in blocks {current_block}-{chunk_end}")
        all_logs.extend(logs)
        current_block = chunk_end + 1

    print(f"Found {len(all_logs)} EventLog1 events in total")
    return all_logs

def decode_event_data(w3, logs):
    contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=[EVENT_ABI])

    decoded_events = []
    for i, log in enumerate(logs):
        processed_log = contract.events.EventLog1().process_log(log)
        if processed_log["args"]["eventName"] in ["PositionIncrease", "PositionDecrease"]:
            event_data = {
                "blockNumber": log["blockNumber"],
                "transactionHash": log["transactionHash"].hex(),
                "msgSender": processed_log["args"]["msgSender"],
                "eventName": processed_log["args"]["eventName"],
                "topic1": log["topics"][1].hex() if len(log["topics"]) > 1 else None, 
                'rawData': log['data']
            }
            decoded_events.append(event_data)

    return decoded_events

def get_type_string_from_abi_input(input_obj):
    if input_obj.get("indexed", False):
        return None

    type_str = input_obj.get("type")

    if not type_str:
         return None

    if not type_str.startswith("tuple"):
        return type_str

    components = input_obj.get("components", [])
    component_types = []

    for component in components:
        component_type = get_type_string_from_abi_input(component)
        if component_type:
            component_types.append(component_type)

    if not component_types:
        return None

    inner_types = ",".join(component_types)

    if type_str.endswith("[]"):
        return f"({inner_types})[]"
    else:
        return f"({inner_types})"

def extract_types_from_abi(abi):
    if abi.get("type") != "event":
        raise ValueError("ABI must be an event type")

    inputs = abi.get("inputs", [])
    types = []

    for input_obj in inputs:
        type_str = get_type_string_from_abi_input(input_obj)
        if type_str:
            types.append(type_str)

    return types

def format_value(value):
    if isinstance(value, bytes):
        return to_hex(value)
    elif isinstance(value, HexBytes):
        return value.hex()
    elif isinstance(value, list) or isinstance(value, tuple):
        return [format_value(item) for item in value]
    elif isinstance(value, dict):
        return {k: format_value(v) for k, v in value.items()}
    elif isinstance(value, str) and value.startswith("0x") and len(value) == 42:
         return to_checksum_address(value).lower()
    return value

def format_key_value_pairs(items, value_formatter):
    result = {}
    for key, value in items:
        result[key] = value_formatter(value)
    return result

def format_key_value_array_pairs(items, value_formatter):
    result = {}
    for key, values in items:
        result[key] = [value_formatter(value) for value in values]
    return result

def format_event_rawdata(event_data):
    if not isinstance(event_data, tuple) or len(event_data) != 7:
         print(f"Warning: Unexpected raw data structure: {event_data}")
         return {}

    address_items, uint_items, int_items, bool_items, bytes32_items, bytes_items, string_items = event_data

    formatted_address_items = {
        "items": format_key_value_pairs(address_items[0], format_value),
        "arrayItems": format_key_value_array_pairs(address_items[1], format_value)
    }

    formatted_uint_items = {
        "items": format_key_value_pairs(uint_items[0], format_value),
        "arrayItems": format_key_value_array_pairs(uint_items[1], format_value)
    }

    formatted_int_items = {
        "items": format_key_value_pairs(int_items[0], format_value),
        "arrayItems": format_key_value_array_pairs(int_items[1], format_value)
    }

    formatted_bool_items = {
        "items": format_key_value_pairs(bool_items[0], format_value),
        "arrayItems": format_key_value_array_pairs(bool_items[1], format_value)
    }

    formatted_bytes32_items = {
        "items": format_key_value_pairs(bytes32_items[0], format_value),
        "arrayItems": format_key_value_array_pairs(bytes32_items[1], format_value)
    }

    formatted_bytes_items = {
        "items": format_key_value_pairs(bytes_items[0], format_value),
        "arrayItems": format_key_value_array_pairs(bytes_items[1], format_value)
    }

    formatted_string_items = {
        "items": format_key_value_pairs(string_items[0], format_value),
        "arrayItems": format_key_value_array_pairs(string_items[1], format_value)
    }

    return {
        "addressItems": formatted_address_items,
        "uintItems": formatted_uint_items,
        "intItems": formatted_int_items,
        "boolItems": formatted_bool_items,
        "bytes32Items": formatted_bytes32_items,
        "bytesItems": formatted_bytes_items,
        "stringItems": formatted_string_items
    }

def flatten_event_data(event_data):
    flat_data = {}
    if not isinstance(event_data, dict):
        return flat_data

    for category, category_data in event_data.items():
        if isinstance(category_data, dict):
            for subkey, subvalue in category_data.items():
                if isinstance(subvalue, dict):
                    for k, v in subvalue.items():
                        if v not in [None, {}, [], ""]:
                            flat_data[k] = v
    return flat_data

def parse_arguments():
    parser = argparse.ArgumentParser(description="Fetch EventLog1 events from Arbitrum blockchain")
    parser.add_argument("--mongodb_uri", type=str, default="mongodb://localhost:27017/", help="MongoDB URI (default: mongodb://localhost:27017/)")
    parser.add_argument("--mongodb_db", type=str, default="MarketTradingTracker", help="MongoDB database name (default: gmx_events)")
    parser.add_argument("--realtime_wait", type=float, default=0.5, help="Seconds to wait between checks in real-time mode (default: 0.5)")
    parser.add_argument("--catchup_wait", type=float, default=0.1, help="Seconds to wait between chunks when catching up (default: 0.1)")
    parser.add_argument("--realtime_threshold", type=int, default=100, help="Blocks behind threshold for real-time mode (default: 100)")
    return parser.parse_args()

def main():
    args = parse_arguments()

    client = pymongo.MongoClient(args.mongodb_uri)
    db = client[args.mongodb_db]
    gmx_event = db["gmx_events"]
    market_data = db["gmx_market"]
    token_info = db["token_info"]
    config = db["configs"]

    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    from_block = config.find_one({"_id":"gmx_last_updated_event"})["last_updated_at_block_number"]

    while True:
        latest_block = w3.eth.block_number
        
        print(f"Current from_block: {from_block}, Latest blockchain block: {latest_block}")
        
        blocks_behind = latest_block - from_block
        
        if blocks_behind <= 0:
            print(f"Caught up to latest block {latest_block}. Waiting {args.realtime_wait}s for new blocks...")
            sleep(args.realtime_wait)
            continue
        
        if blocks_behind > args.realtime_threshold:
            to_block = from_block + min(10000, blocks_behind)
            processing_mode = "catch-up"
            wait_time = args.catchup_wait
            print(f"Catch-up mode: {blocks_behind} blocks behind, processing {to_block - from_block + 1} blocks")
        else:
            chunk_size = min(10, blocks_behind)  
            to_block = from_block + chunk_size - 1
            processing_mode = "real-time"
            wait_time = 0.1  
            print(f"Real-time mode: {blocks_behind} blocks behind, processing {chunk_size} blocks")
        
        if to_block > latest_block:
            to_block = latest_block
            
        print(f"Searching for events from block {from_block} to {to_block}")

        logs = get_contract_events(w3, from_block, to_block)
        
        if logs:
            decoded_events = decode_event_data(w3, logs)
            
            if decoded_events:
                data_types = extract_types_from_abi(EVENT_ABI)
                
                for event in decoded_events:
                    try:
                        cleaned_event = {
                            "msgSender": event["msgSender"],
                            "eventName": event["eventName"],
                            "topic1": event["topic1"],
                            "transactionHash": event["transactionHash"],
                            "blockNumber": event["blockNumber"]
                        }

                        raw_data_bytes = event["rawData"]
                        if raw_data_bytes and data_types:
                            if isinstance(raw_data_bytes, str):
                                if raw_data_bytes.startswith("0x"):
                                    raw_data_bytes = raw_data_bytes[2:]
                                raw_data_bytes = bytes.fromhex(raw_data_bytes)
                            elif not isinstance(raw_data_bytes, bytes):
                                raw_data_bytes = None 

                            if raw_data_bytes:
                                decoded_data_tuple = decode(data_types, raw_data_bytes)
                                if decoded_data_tuple and isinstance(decoded_data_tuple[-1], tuple):
                                    formatted_data = format_event_rawdata(decoded_data_tuple[-1])
                                    flat_event_data = flatten_event_data(formatted_data)
                                    cleaned_event.update(flat_event_data)
                                    
                        cleaned_event = process_event(cleaned_event, w3, market_data, token_info)
                        
                        cleaned_event["_id"] = cleaned_event["transactionHash"]
                        
                        gmx_event.replace_one(
                            {"_id": cleaned_event["_id"]}, 
                            cleaned_event, 
                            upsert=True
                        )
                        print(f"Processed event in block {event['blockNumber']}: {event['eventName']}")
                        
                    except Exception as e:
                        print(f"Error processing individual event: {e}")
                        continue
        
        from_block = to_block + 1
        
        config.update_one(
            {"_id": "gmx_last_updated_event"}, 
            {"$set": {"last_updated_at_block_number": from_block}}
        )
        
        if wait_time > 0:
            sleep(wait_time)

if __name__ == "__main__":
    main()