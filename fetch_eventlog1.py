import json
import sys
import argparse
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from hexbytes import HexBytes
from eth_utils import to_hex, to_checksum_address
from eth_abi import decode 

CONTRACT_ADDRESS = '0xC8ee91A54287DB53897056e12D9819156D3822Fb'
RPC_URL = 'https://arb1.arbitrum.io/rpc'

EVENT_SIGNATURE = '0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160'

with open('abi_emitter.json', 'r') as f:
    EVENT_ABI = json.load(f)


def get_contract_events(w3, from_block, to_block=None):
    if to_block is None:
        to_block = from_block
    
    print(f"Using event signature: {EVENT_SIGNATURE}")
    
    CHUNK_SIZE = 1000
    all_logs = []
    
    for chunk_start in range(from_block, to_block + 1, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE - 1, to_block)
        print(f"Fetching logs for blocks {chunk_start} to {chunk_end}...")
        

        logs = w3.eth.get_logs({
            'fromBlock': chunk_start,
            'toBlock': chunk_end,
            'address': Web3.to_checksum_address(CONTRACT_ADDRESS),
            'topics': [EVENT_SIGNATURE]
        })
        
        print(f"Found {len(logs)} logs in blocks {chunk_start}-{chunk_end}")
        all_logs.extend(logs)
            
    print(f"Found {len(all_logs)} EventLog1 events in total")
    return all_logs

def decode_event_data(w3, logs):
    contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=[EVENT_ABI])
    
    decoded_events = []
    for i, log in enumerate(logs):

        processed_log = contract.events.EventLog1().process_log(log)
        if processed_log['args']['eventName'] in ['PositionIncrease', 'PositionDecrease']:
            event_data = {
                'blockNumber': log['blockNumber'],
                'transactionHash': log['transactionHash'].hex(),
                'msgSender': processed_log['args']['msgSender'],
                'eventName': processed_log['args']['eventName'],
                'topic1': log['topics'][2].hex() if len(log['topics']) > 2 else None,
                'rawData': log['data']
            }

            
            decoded_events.append(event_data)
            print(f"Event {i+1}/{len(logs)}: {event_data['eventName']}")
                
    return decoded_events

def format_event_data(decoded_events):
    formatted_events = []
    
    for event in decoded_events:
        formatted_event = {
            'blockNumber': event['blockNumber'],
            'transactionHash': event['transactionHash'].hex() if hasattr(event['transactionHash'], 'hex') else event['transactionHash'],
            'msgSender': event['msgSender'],
            'eventName': event['eventName'],
            'topic1': event['topic1'].hex() if hasattr(event['topic1'], 'hex') else event['topic1'],
            'rawData': event['rawData'].hex() if hasattr(event['rawData'], 'hex') else event['rawData'],
        }
        
        formatted_events.append(formatted_event)
    
    return formatted_events

def get_type_string_from_abi_input(input_obj):
    if input_obj.get('indexed', False):
        return None
    
    type_str = input_obj.get('type')
    
    if not type_str.startswith('tuple'):
        return type_str
    
    components = input_obj.get('components', [])
    component_types = []
    
    for component in components:
        component_type = get_type_string_from_abi_input(component)
        if component_type:
            component_types.append(component_type)

    inner_types = ','.join(component_types)
    
    if type_str.endswith('[]'):
        return f"({inner_types})[]"
    else:
        return f"({inner_types})"


def extract_types_from_abi(abi):
    if abi.get('type') != 'event':
        raise ValueError("ABI must be an event type")
    
    inputs = abi.get('inputs', [])
    types = []
    
    for input_obj in inputs:
        type_str = get_type_string_from_abi_input(input_obj)
        if type_str:  
            types.append(type_str)
    
    return types

def format_event_rawdata(event_data):    
    address_items, uint_items, int_items, bool_items, bytes32_items, bytes_items, string_items = event_data
    
    formatted_address_items = {
        'items': format_key_value_pairs(address_items[0], format_address),
        'arrayItems': format_key_value_array_pairs(address_items[1], format_address)
    }
    
    formatted_uint_items = {
        'items': format_key_value_pairs(uint_items[0], format_uint),
        'arrayItems': format_key_value_array_pairs(uint_items[1], format_uint)
    }
    
    formatted_int_items = {
        'items': format_key_value_pairs(int_items[0], format_int),
        'arrayItems': format_key_value_array_pairs(int_items[1], format_int)
    }
    
    formatted_bool_items = {
        'items': format_key_value_pairs(bool_items[0], format_bool),
        'arrayItems': format_key_value_array_pairs(bool_items[1], format_bool)
    }
    
    formatted_bytes32_items = {
        'items': format_key_value_pairs(bytes32_items[0], format_bytes32),
        'arrayItems': format_key_value_array_pairs(bytes32_items[1], format_bytes32)
    }
    
    formatted_bytes_items = {
        'items': format_key_value_pairs(bytes_items[0], format_bytes),
        'arrayItems': format_key_value_array_pairs(bytes_items[1], format_bytes)
    }
    
    formatted_string_items = {
        'items': format_key_value_pairs(string_items[0], format_string),
        'arrayItems': format_key_value_array_pairs(string_items[1], format_string)
    }
    
    return {
        'addressItems': formatted_address_items,
        'uintItems': formatted_uint_items,
        'intItems': formatted_int_items,
        'boolItems': formatted_bool_items,
        'bytes32Items': formatted_bytes32_items,
        'bytesItems': formatted_bytes_items,
        'stringItems': formatted_string_items
    }

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

def format_address(value):
    return to_checksum_address(value)

def format_uint(value):
    return value

def format_int(value):
    return value

def format_bool(value):
    return value

def format_bytes32(value):
    return to_hex(value)

def format_bytes(value):
    return to_hex(value)

def format_string(value):
    return value

def flatten_event_data(event_data):
    flat_data = {}
    for category, category_data in event_data.items():
        for subkey, subvalue in category_data.items():
            if isinstance(subvalue, dict):
                for k, v in subvalue.items():
                    if v not in [None, {}, [], ""]:
                        flat_data[k] = v
    return flat_data


def parse_arguments():
    parser = argparse.ArgumentParser(description='Fetch EventLog1 events from Arbitrum blockchain')
    parser.add_argument('from_block', nargs='?', type=int, help='Starting block number')
    parser.add_argument('to_block', nargs='?', type=int, help='Ending block number')
    return parser.parse_args()

def main():
    args = parse_arguments()
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    latest_block = w3.eth.block_number

    print(f"Latest block number: {latest_block}")
    
    if args.from_block is not None and args.to_block is not None:
        from_block = args.from_block
        to_block = args.to_block
    elif args.from_block is not None:
        from_block = args.from_block
        to_block = latest_block
    else:
        from_block = max(0, latest_block - 100000)
        to_block = latest_block
    
    print(f"Searching for events from block {from_block} to {to_block}")
    
    logs = get_contract_events(w3, from_block, to_block)
    
    decoded_events = decode_event_data(w3, logs)
    
    formatted_events = format_event_data(decoded_events)

    data_types = extract_types_from_abi(EVENT_ABI)

    all = []
    for event in formatted_events:
        cleaned_event = {}
        if event['eventName'] in ['PositionIncrease', 'PositionDecrease']:
            cleaned_event = {
                'msgSender': event['msgSender'],
                'eventName': event['eventName'],
                'topic1': event['topic1'],
                'transaction_hash': event['transactionHash'],
                'block_number': event['blockNumber']
            }

            raw_data = event['rawData']
            if raw_data.startswith('0x'):
                raw_data = raw_data[2:]
            cleaned_data = decode(data_types, bytes.fromhex(raw_data))
            final_data = format_event_rawdata(cleaned_data[-1])
            flat_event_data = flatten_event_data(final_data)
            cleaned_event.update(flat_event_data)

        all.append(cleaned_data)
    print(cleaned_data)
    output_file = 'gmx_final_data.json'
    with open(output_file, 'w') as f:
        json.dump(all, f, indent=2, default=str)
    
    print(f"Events saved to {output_file}")
    
    

if __name__ == "__main__":
    main()
