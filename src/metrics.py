import pandas as pd

# modify tvl function to measure price through swap events
def tvl(
    mint_decoded,
    burn_decoded,
    swap_decoded,
    DECIMALS0,
    DECIMALS1,
    token0_is_usd=True,
    token1_is_usd=False,
    tvl_numeraire="token0",
):
    """
    Calculate TVL variation and attach price-derived covariates.

    Outputs include:
    - USD TVL (if one token is USD-anchored)
    - valuation-agnostic relative TVL in token0/token1 units
    - delta_TVL% for PoC survival analysis
    """
    import numpy as np
    import pandas as pd

    mint_part = mint_decoded.assign(
        amount0_delta=mint_decoded["amount0"],
        amount1_delta=mint_decoded["amount1"],
    )

    burn_part = burn_decoded.assign(
        amount0_delta=-burn_decoded["amount0"],
        amount1_delta=-burn_decoded["amount1"],
    )

    burn_part = burn_part[burn_part["amount"] > 0]

    cols = [
        "timestamp",
        "block_number",
        "transaction_index",
        "log_index",
        "event",
        "owner",
        "tickLower",
        "tickUpper",
        "amount",
        "amount0",
        "amount1",
        "amount0_delta",
        "amount1_delta",
    ]

    tvl_variation = (
        pd.concat([mint_part[cols], burn_part[cols]], ignore_index=True)
        .sort_values(by=["block_number", "transaction_index", "log_index"])
        .reset_index(drop=True)
    )

    tvl_variation["cumulative_amount0"] = tvl_variation["amount0_delta"].cumsum()
    tvl_variation["cumulative_amount1"] = tvl_variation["amount1_delta"].cumsum()

    tvl_variation["amount0_HRF"] = tvl_variation["amount0"] / 10**DECIMALS0
    tvl_variation["amount1_HRF"] = tvl_variation["amount1"] / 10**DECIMALS1
    tvl_variation["cumulative_amount0_HRF"] = tvl_variation["cumulative_amount0"] / 10**DECIMALS0
    tvl_variation["cumulative_amount1_HRF"] = tvl_variation["cumulative_amount1"] / 10**DECIMALS1

    # --- Compute last known swap price (as-of) ---
    if "sqrtPriceX96" not in swap_decoded.columns:
        raise ValueError("swap_decoded must contain sqrtPriceX96")

    swap_prices = (
        swap_decoded[["block_number", "transaction_index", "log_index", "sqrtPriceX96"]]
        .copy()
        .sort_values(by=["block_number", "transaction_index", "log_index"])
        .reset_index(drop=True)
    )

    # Create a stable ordering key for as-of merge
    def _event_order(df):
        return (
            df["block_number"].astype("int64") * 1_000_000_000
            + df["transaction_index"].astype("int64") * 1_000
            + df["log_index"].astype("int64")
        )

    tvl_variation["event_order"] = _event_order(tvl_variation)
    swap_prices["event_order"] = _event_order(swap_prices)

    # sqrtPriceX96 => price token1/token0 (raw), then adjust to human-readable
    sqrt_price = swap_prices["sqrtPriceX96"].astype("float64")
    price1_per0_raw = (sqrt_price / 2**96) ** 2
    price1_per0_hrf = price1_per0_raw * (10.0 ** (DECIMALS0 - DECIMALS1))
    swap_prices["price1_per0_hrf"] = price1_per0_hrf

    tvl_variation = pd.merge_asof(
        tvl_variation.sort_values("event_order"),
        swap_prices[["event_order", "price1_per0_hrf"]],
        on="event_order",
        direction="backward",
    )

    tvl_variation["price1_per0_hrf"] = tvl_variation["price1_per0_hrf"].bfill()

    # ---------- Valuation-agnostic TVL + delta_TVL% ----------
    tvl_variation["tvl_token0_units"] = (
        tvl_variation["cumulative_amount0_HRF"]
        + (tvl_variation["cumulative_amount1_HRF"] / tvl_variation["price1_per0_hrf"])
    )
    tvl_variation["tvl_token1_units"] = (
        tvl_variation["cumulative_amount1_HRF"]
        + (tvl_variation["cumulative_amount0_HRF"] * tvl_variation["price1_per0_hrf"])
    )

    if tvl_numeraire not in {"token0", "token1"}:
        raise ValueError('tvl_numeraire must be "token0" or "token1"')

    if tvl_numeraire == "token0":
        tvl_variation["tvl_relative"] = tvl_variation["tvl_token0_units"]
    else:
        tvl_variation["tvl_relative"] = tvl_variation["tvl_token1_units"]

    tvl_variation["delta_tvl_pct"] = tvl_variation["tvl_relative"].pct_change() * 100
    tvl_variation["delta_tvl_pct"] = (
        tvl_variation["delta_tvl_pct"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )

    # ---------- Optional USD conversion ----------
    if token0_is_usd and token1_is_usd:
        raise ValueError("Only one of token0_is_usd or token1_is_usd can be True.")

    if token0_is_usd:
        tvl_variation["price0_usd"] = 1.0
        tvl_variation["price1_usd"] = 1.0 / tvl_variation["price1_per0_hrf"]
    elif token1_is_usd:
        tvl_variation["price1_usd"] = 1.0
        tvl_variation["price0_usd"] = tvl_variation["price1_per0_hrf"]
    else:
        tvl_variation["price0_usd"] = np.nan
        tvl_variation["price1_usd"] = np.nan

    tvl_variation["tvl_usd"] = (
        tvl_variation["cumulative_amount0_HRF"] * tvl_variation["price0_usd"]
        + tvl_variation["cumulative_amount1_HRF"] * tvl_variation["price1_usd"]
    )

    return tvl_variation



def verify_lp_exit(mint_decoded, burn_decoded):
    """
    Fast, vectorized detection of LPs who have fully exited (all positions zero).

    Parameters
    ----------
    mint_decoded, burn_decoded : DataFrames
        Must contain columns: timestamp, block_number, transaction_index,
        log_index, owner, tickLower, tickUpper, amount.

    Returns
    -------
    (positions_status, lp_exit_status)
        positions_status : DataFrame with latest cumulative liquidity per position.
        lp_exit_status   : DataFrame with exit summary per LP.
    """

    # 1. Combine mint and burn, assign liquidity deltas
    mint_part = mint_decoded.assign(
        event_type='Mint',
        liquidity_delta=mint_decoded['amount']
    )
    burn_filtered = burn_decoded[burn_decoded['amount'] != 0].copy()
    burn_part = burn_filtered.assign(
        event_type='Burn',
        liquidity_delta=-burn_filtered['amount']
    )
    events = pd.concat([mint_part, burn_part], ignore_index=True)

    # Create position key
    events["lpPositionKey"] = (
        events["owner"].astype(str)
        + "_"
        + events["tickLower"].astype(str)
        + "_"
        + events["tickUpper"].astype(str)
    )

    # 2. Global chronological sort – mandatory for correct cumulative sums
    events = events.sort_values(
        ['block_number', 'transaction_index', 'log_index']
    ).reset_index(drop=True)

    # 3. Compute cumulative liquidity per position (vectorized groupby)
    events['cumulative_liquidity'] = events.groupby(
        'lpPositionKey'
    )['liquidity_delta'].transform(lambda x: x.cumsum())

    # 4. (Optional) Flag when a position becomes zero – vectorized using shift
    events['prev_liquidity'] = events.groupby(
        'lpPositionKey'
    )['cumulative_liquidity'].shift(1).fillna(0)
    events['position_fully_burned'] = (
        (events['cumulative_liquidity'] == 0) & (events['prev_liquidity'] != 0)
    )
    events.drop(columns=['prev_liquidity'], inplace=True)

    # 5. Latest state of each position (last event per position)
    # event_type = Mint means the position is still open : the 
    latest_position = (
        events.sort_values(['owner', 'block_number','lpPositionKey'
                            ])
        .groupby('lpPositionKey')
        .last()
        .reset_index()
        [['lpPositionKey', 'owner', 'tickLower', 'tickUpper', 'cumulative_liquidity',
            'event_type', 'block_number', 'timestamp']]
    )

    # 6. LP‑level summary
    lp_summary = (
        latest_position
        .groupby('owner')
        .agg(
            total_positions=('tickLower', 'count'),
            active_positions=('cumulative_liquidity',
                                lambda x: (x > 0).sum()),
            # all_zero = True iff active_positions == 0
        )
        .reset_index()
    )

    # find blocknumber of first mint ever

    lp_summary['has_exit_pool'] = lp_summary['active_positions'] == 0
    lp_summary['status'] = lp_summary['has_exit_pool'].astype(int)

    # 7. Exit timestamp: for fully exited LPs, the timestamp of their last event
    #    (fast groupby max, no loop)
    last_event_timestamp = events.groupby('owner')['timestamp'].max().rename('exit_timestamp')

    lp_summary = lp_summary.merge(last_event_timestamp, left_on='owner',
                                    right_index=True, how='left')
    
    # Only keep exit_timestamp for those truly fully exited
    max_timestamp = events['timestamp'].max()
    lp_summary.loc[~lp_summary['has_exit_pool'], 'exit_timestamp'] = max_timestamp
    
    first_mint_rows = (
        events[events["event_type"] == "Mint"]
        .sort_values(["block_number", "transaction_index", "log_index"])
        .groupby("owner", as_index=False)
        .first()[["owner", "block_number", "timestamp", "lpPositionKey"]]
        .rename(
            columns={
                "block_number": "first_mint_block",
                "lpPositionKey": "first_mint_lpPositionKey",
                "timestamp" : "first_mint_timestamp"
            }
        )
    )
    lp_summary = lp_summary.merge(first_mint_rows, on="owner", how="left")
    lp_summary["closed_positions"] = lp_summary["total_positions"] - lp_summary["active_positions"]
    lp_summary["duration"] = (lp_summary["exit_timestamp"] - lp_summary["first_mint_timestamp"]).astype("Int64")
    
    lp_summary.sort_values(by=["status", "total_positions"], ascending=[True, False])



    return latest_position, lp_summary


def trading_volume(swap_decoded, DECIMALS0, DECIMALS1):
    """Calculate trading volume variation from decoded swap events."""

    trading_volume = swap_decoded.copy()
    trading_volume['volume_amount0'] = trading_volume['amount0'].abs()
    trading_volume['volume_amount1'] = trading_volume['amount1'].abs()
    trading_volume['swap_direction'] = trading_volume['amount0'].apply(
        lambda x: 'TOKEN0 to TOKEN1' if x < 0 else 'TOKEN1 to TOKEN0'
    )

    cols = [
        "timestamp",
        "block_number",
        "transaction_index",
        "log_index",
        "transaction_hash",
        "volume_amount0",
        "volume_amount1",
        "swap_direction"
    ]
    
    # Select and sort relevant columns
    trading_volume = trading_volume[cols].sort_values(by=["block_number", "transaction_index", "log_index"]).reset_index(drop=True)

    # Calculate cumulative trading volume
    trading_volume['cumulative_volume_amount0'] = trading_volume['volume_amount0'].cumsum()
    trading_volume['cumulative_volume_amount1'] = trading_volume['volume_amount1'].cumsum()
    
    # Add human-readable volume columns
    trading_volume['volume_amount0_HRF'] = trading_volume['volume_amount0'] / 10**DECIMALS0
    trading_volume['volume_amount1_HRF'] = trading_volume['volume_amount1'] / 10**DECIMALS1
    trading_volume['cumulative_volume_amount0_HRF'] = trading_volume['cumulative_volume_amount0'] / 10**DECIMALS0
    trading_volume['cumulative_volume_amount1_HRF'] = trading_volume['cumulative_volume_amount1'] / 10**DECIMALS1

    return trading_volume

def collected_fees(collect_decoded, DECIMALS0, DECIMALS1):
    """Calculate collected fees variation from decoded collect events."""
    
    cols = [
        "timestamp",
        "block_number",
        "transaction_index",
        "log_index",
        "owner",
        "tickLower",
        "tickUpper",
        "fees_amount0",
        "fees_amount1",
    ]
    
    collected_fees = collect_decoded.assign(
        fees_amount0=collect_decoded["amount0"],
        fees_amount1=collect_decoded["amount1"],
    )[cols].sort_values(by=["block_number", "transaction_index", "log_index"]).reset_index(drop=True)

    # Add human-readable fee columns
    collected_fees['fees_amount0_HRF'] = collected_fees['fees_amount0'] / 10**DECIMALS0
    collected_fees['fees_amount1_HRF'] = collected_fees['fees_amount1'] / 10**DECIMALS1

    # Calculate cumulative collected fees
    collected_fees['cumulative_fees_amount0'] = collected_fees['fees_amount0'].cumsum()
    collected_fees['cumulative_fees_amount1'] = collected_fees['fees_amount1'].cumsum()
    collected_fees['cumulative_fees_amount0_HRF'] = collected_fees['cumulative_fees_amount0'] / 10**DECIMALS0
    collected_fees['cumulative_fees_amount1_HRF'] = collected_fees['cumulative_fees_amount1'] / 10**DECIMALS1

    return collected_fees
