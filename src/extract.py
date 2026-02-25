import hypersync
from hypersync import (
    LogSelection,
    LogField,
    BlockField,
    #FieldSelection,
    #TransactionField,
    HexOutput,
    #Decoder
)
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()


USDT_WBTC = {"chain_name":"celo",
             "address" :"0x57332c214E647063bB4c5A73e5A8b7bbA79Be1E4",
             "out_path": "usdt_wbtc_events"}

USDT_WETH = {"chain_name":"celo",
             "address" :"0xF55791AfBB35aD42984f18D6Fe3e1fF73D81900c",
             "out_path": "usdt_weth_events"}

USDT_WETH_PLASMA = {"chain_name":"plasma",
                "address" :"0xCe4Ac514CA6a9db357CcCc105B7848d7fd37445d",
                "out_path": "plasma_usdt_weth_events"}

async def hypersync_indexer(chain_name : str, contract_address : str, out_path : str):
    # Initialize client
    client = hypersync.HypersyncClient(
        hypersync.ClientConfig(
            url=f"https://{chain_name}.hypersync.xyz",
            bearer_token=os.getenv("HYPERSYNC_BEARER_TOKEN"),  # Set in .env file
        )
    )

    # Define field selection
    field_selection = hypersync.FieldSelection(
        block=[
            BlockField.NUMBER, 
            BlockField.TIMESTAMP
        ],
        log=[
            LogField.BLOCK_NUMBER,
            LogField.TRANSACTION_HASH,
            LogField.TRANSACTION_INDEX,
            LogField.LOG_INDEX,
            LogField.DATA,
            LogField.ADDRESS,
            LogField.TOPIC0, 
            LogField.TOPIC1,
            LogField.TOPIC2,
            LogField.TOPIC3,
        ],
    )

    # Define query for UNI transfers
    
    # define height for to block
    height = await client.get_height()
    query = hypersync.Query(
        from_block=0,
        to_block=height,
        field_selection=field_selection,
        logs=[
            LogSelection(
                address=[contract_address],  
                topics=[
                    #[event_signature]  
                ]
            )
        ]
    )

    # Configure output
    config = hypersync.StreamConfig(
        hex_output=HexOutput.PREFIXED
    )

    # Collect data to a Parquet file
    print("Fetching logs...")
    result = await client.collect_parquet(out_path, query, config)
    print(f"Success. Processed blocks from {query.from_block} to {query.to_block}.")


# asyncio.run(hypersync_indexer(
#     chain_name="celo",
#     contract_address="0x57332c214E647063bB4c5A73e5A8b7bbA79Be1E4",
#     out_path="celo_usdt_wbtc_events2"
#     ))