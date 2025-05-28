import json
import sys
import argparse
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from hexbytes import HexBytes
from eth_utils import to_hex, to_checksum_address
from eth_abi import decode
import pymongo 


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["MarketTradingTracker"]
collection = db["gmx_events"]

CONTRACT_ADDRESS = "0xC8ee91A54287DB53897056e12D9819156D3822Fb"
RPC_URL = "https://arb1.arbitrum.io/rpc"

EVENT_SIGNATURE = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"

with open("abi_emitter.json", "r") as f:
    EVENT_ABI = json.load(f)

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
                "topic1": log["topics"][1].hex() if len(log["topics"]) > 1 else None, # Corrected index from 2 to 1
                "rawData": log["data"]
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
         return to_checksum_address(value)
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
    parser.add_argument("from_block", type=int, help="Starting block number")
    parser.add_argument("to_block", nargs="?", type=int, help="Ending block number (optional, defaults to from_block)")
    return parser.parse_args()

def main():
    args = parse_arguments()
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    if not w3.is_connected():
        print(f"Error: Could not connect to RPC URL: {RPC_URL}")
        sys.exit(1)

    latest_block = w3.eth.block_number
    print(f"Latest block number: {latest_block}")

    from_block = args.from_block
    to_block = args.to_block if args.to_block is not None else from_block

    if from_block > to_block:
        print(f"Error: from_block ({from_block}) cannot be greater than to_block ({to_block})")
        sys.exit(1)

    print(f"Searching for events from block {from_block} to {to_block}")

    logs = get_contract_events(w3, from_block, to_block)
    if not logs:
        print("No matching logs found.")
        return

    decoded_events = decode_event_data(w3, logs)
    if not decoded_events:
        print("No relevant events decoded.")
        return

    data_types = extract_types_from_abi(EVENT_ABI)
    if not data_types:
        print("Warning: Could not extract data types from ABI for decoding rawData.")

    all_processed_events = [] 
    for event in decoded_events:
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
                 print(f"Warning: Unexpected rawData type for tx {event['transactionHash']}: {type(raw_data_bytes)}. Skipping decode.")
                 raw_data_bytes = None 

            if raw_data_bytes:
                decoded_data_tuple = decode(data_types, raw_data_bytes)
                if decoded_data_tuple and isinstance(decoded_data_tuple[-1], tuple):
                    formatted_data = format_event_rawdata(decoded_data_tuple[-1])
                    flat_event_data = flatten_event_data(formatted_data)
                    cleaned_event.update(flat_event_data)
                else:
                    print(f"Warning: Decoded data for tx {event['transactionHash']} does not have expected structure or is empty.")
            else:
                 print(f"Warning: Invalid rawData format for tx {event['transactionHash']}. Skipping decode.")

        elif not raw_data_bytes:
            print(f"Warning: Empty rawData for tx {event['transactionHash']}.")

        all_processed_events.append(cleaned_event)


    output_file = "gmx_final_data_fixed.json"
    with open(output_file, "w") as f:
        json.dump(all_processed_events, f, indent=2, default=str) 
    print(f"Events saved to {output_file}")

if __name__ == "__main__":
    main()

