# Decode_GMX_Perp

Lightweight tools to fetch, decode and clean GMX perpetual event logs for on-chain analysis.

## Quick install

```bash
git clone https://github.com/tourmii/Decode_GMX_Perp.git
cd Decode_GMX_Perp
python -m venv .venv
source .venv/bin/activate    # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

## Quick usage

1. set RPC endpoint:

```bash
export ARB_RPC_URL="https://arb1.arbitrum.io/rpc"
```

2. fetch logs:

```bash
python fetch_eventlog1.py --from-block 12300000 --to-block 12301000 --out raw_events.json
```

3. decode logs:

```bash
python decode_gmx_2.py --in raw_events.json --abi abi_emitter.json --out decoded_events.json
```

4. clean / analyze:

```bash
python clean_data.py --in decoded_events.json --out cleaned.csv
python events_process_analyze.py cleaned.csv
```

## Main files

* fetch_eventlog1.py — download raw logs
* decode_gmx_2.py — decode logs using ABI
* clean_data.py — normalize / filter events
* events_process_analyze.py — analysis / export
* requirements.txt, abi_emitter.json, sample outputs

## Notes

* You must provide a correct ABI and a working RPC provider.
* Scripts are minimal; expect to tweak args, error handling, and block ranges for large jobs.

## License

Add a LICENSE file (MIT recommended) if you want this repo public.
