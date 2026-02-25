import pandas as pd
import duckdb as ddb

def to_uint(hexstr):
    return int(hexstr, 16) if isinstance(hexstr, str) and hexstr else 0

def to_int256(hexstr):
    x = to_uint(hexstr)
    return x - 2**256 if x >= 2**255 else x

def load_events(logs_path, blocks_path):
    pool_events = ddb.sql(f"""SELECT
                                    block.TIMESTAMP::BIGINT as timestamp,
                                    log.*
            FROM '{logs_path}' as log
            INNER JOIN '{blocks_path}' as block
            ON log.BLOCK_NUMBER = block.NUMBER
            ORDER BY log.block_number, log.transaction_index, log.log_index ASC
            """).df()
    return pool_events

def decode_topic0(pool_events):
    topic0 = pool_events["topic0"].unique()

    # does the job since the other events 
    # are not user-related, more pool-owner related (SetFeeProtocol, CollectProtocol)
    topic0_decoded = ["Initialize","Mint","Swap","IncreaseObservationCardinalityNext","Burn","Collect","Flash"]  
        
    topic0_mapping = dict(zip(topic0, topic0_decoded))
    pool_events["event"] = pool_events["topic0"].map(topic0_mapping)
    return pool_events

def decode_mint_events(pool_events):
    data_clean = pool_events["data"].str[2:]

    sender = "0x" + data_clean.str.slice(24, 64)
    amount = data_clean.str.slice(64, 128).apply(to_uint)
    amount0 = data_clean.str.slice(128, 192).apply(to_uint)
    amount1 = data_clean.str.slice(192, 256).apply(to_uint)

    owner = "0x" + pool_events["topic1"].str[26:]
    tickLower = pool_events["topic2"].apply(to_int256)
    tickUpper = pool_events["topic3"].apply(to_int256)

    return pd.concat(
        [
            pool_events,
            pd.DataFrame(
            {
            'sender': sender,
            'owner': owner,
            'tickLower': tickLower,
            'tickUpper': tickUpper,
            'amount': amount,
            'amount0': amount0,
            'amount1': amount1
        }
        )],
        axis=1)

def decode_burn_events(pool_events):
    data_clean = pool_events["data"].str[2:]

    amount = data_clean.str.slice(0, 64).apply(to_uint)
    amount0 = data_clean.str.slice(64, 128).apply(to_uint)
    amount1 = data_clean.str.slice(128, 192).apply(to_uint)

    owner = "0x" + pool_events["topic1"].str[26:]
    tickLower = pool_events["topic2"].apply(to_int256)
    tickUpper = pool_events["topic3"].apply(to_int256)

    return pd.concat(
    [
        pool_events, 
        pd.DataFrame(
        {
            "owner": owner,
            "tickLower": tickLower,
            "tickUpper": tickUpper,
            "amount": amount,
            "amount0": amount0,
            "amount1": amount1,
        }
    )],
        axis=1)

def decode_swap_events(pool_events):
    data_clean = pool_events["data"].str[2:]

    amount0 = data_clean.str.slice(0, 64).apply(to_int256)
    amount1 = data_clean.str.slice(64, 128).apply(to_int256)
    sqrtPriceX96 = data_clean.str.slice(128, 192).apply(to_uint)
    liquidity = data_clean.str.slice(192, 256).apply(to_uint)
    tick = data_clean.str.slice(256, 320).apply(to_int256)

    sender = "0x" + pool_events["topic1"].str[26:]
    recipient = "0x" + pool_events["topic2"].str[26:]

    return pd.concat(
        [
            pool_events,
        pd.DataFrame({
            "sender": sender,
            "recipient": recipient,
            "amount0": amount0,
            "amount1": amount1,
            "sqrtPriceX96": sqrtPriceX96,
            "liquidity": liquidity,
            "tick": tick
        })
        ],
        axis=1,
    )

def decode_collect_events(pool_events):
    data_clean = pool_events["data"].str[2:]
    recipient = "0x" + data_clean.str.slice(24, 64)

    amount0 = data_clean.str.slice(64, 128).apply(to_uint)
    amount1 = data_clean.str.slice(128, 192).apply(to_uint)

    owner = "0x" + pool_events["topic1"].str[26:]
    tickLower = pool_events["topic2"].apply(to_int256)
    tickUpper = pool_events["topic3"].apply(to_int256)

    return pd.concat(
        [
            pool_events,
            pd.DataFrame(
                {
                    "owner": owner,
                    "recipient": recipient,
                    "tickLower": tickLower,
                    "tickUpper": tickUpper,
                    "amount0": amount0,
                    "amount1": amount1,
                }
            ),
        ],
        axis=1,
    )