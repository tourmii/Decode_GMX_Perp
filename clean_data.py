import json 

with open('gmx_events_output.json', 'r') as f:
    data = json.load(f)

with open('gmx_market.json', 'r') as f:
    market_data = json.load(f)

for event in data:
    for x in market_data:
        if x.get('contract_address') == event.get('market'):
            event['asset'] = x.get('name')
            decimals = (x.get('decimals'))
            event['decimals'] = decimals
    event['sizeInUsd'] = event['sizeInUsd'] / 1e30 if 'sizeInUsd' in event else None
    event['sizeInTokens'] = event['sizeInTokens'] / decimals if 'sizeInTokens' in event else None
    event['sizeDeltaUsd'] = event['sizeDeltaUsd'] / 1e30 if 'sizeDeltaUsd' in event else None
    event['sizeDeltaInTokens'] = event['sizeDeltaInTokens'] / decimals if 'sizeDeltaInTokens' in event else None
    event['collateralTokenPrice.max'] = event['collateralTokenPrice.max'] / 1e23 if 'collateralTokenPrice.max' in event else None
    event['collateralTokenPrice.min'] = event['collateralTokenPrice.min'] / 1e23 if 'collateralTokenPrice.min' in event else None
    event['executionPrice'] = event['executionPrice'] / 10**(12) if 'executionPrice' in event else None
    event['indexTokenPrice.max'] = event['indexTokenPrice.max'] / 10**(12) if 'indexTokenPrice.max' in event else None
    event['indexTokenPrice.min'] = event['indexTokenPrice.min'] / 10**(12) if 'indexTokenPrice.min' in event else None
    event['collateralAmount'] = event['collateralAmount'] / 1e6 if 'collateralAmount' in event else None
    event['borrowingFactor'] = event['borrowingFactor'] / 1e30 if 'borrowingFactor' in event else None
    event['fundingFeeAmountPerSize'] = event['fundingFeeAmountPerSize'] / 1e30 if 'fundingFeeAmountPerSize' in event else None
    event['longTokenClaimableFundingAmountPerSize'] = event['longTokenClaimableFundingAmountPerSize'] / 1e30 if 'longTokenClaimableFundingAmountPerSize' in event else None
    event['shortTokenClaimableFundingAmountPerSize'] = event['shortTokenClaimableFundingAmountPerSize'] / 1e30 if 'shortTokenClaimableFundingAmountPerSize' in event else None
    event['priceImpactUsd'] = event['priceImpactUsd'] / 1e30 if 'priceImpactUsd' in event else None
    event['priceImpactAmount'] = event['priceImpactAmount'] / decimals if 'priceImpactAmount' in event else None
    event['collateralDeltaAmount'] = event['collateralDeltaAmount'] / 1e6 if 'collateralDeltaAmount' in event else None
    print(json.dumps(event, indent=2))