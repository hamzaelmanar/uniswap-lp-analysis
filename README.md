# Uniswap V3 LP Survival Analysis

A data pipeline and statistical analysis tool for studying the **retention behaviour of Liquidity Providers (LPs)** on Uniswap V3 CLAMM pools, with support for [Merkl](https://merkl.xyz) incentive campaigns.

## What it does

Given a Merkl campaign URL, the tool:

1. **Fetches** raw on-chain pool events (Mint, Burn, Swap, Collect) from any EVM chain via [HyperSync (Envio)](https://docs.envio.dev/docs/HyperSync/overview)
2. **Decodes** ABI-encoded Uniswap V3 log data into structured DataFrames
3. **Computes** pool metrics — TVL (token units + USD), trading volume, collected fees, and price from `sqrtPriceX96`
4. **Enriches** LP position data with Merkl campaign windows (did the LP enter during an incentive campaign?)
5. **Runs survival analysis** using the Kaplan-Meier estimator, segmented by campaign entry, to quantify LP retention

## Usage

```bash
python main.py --URL https://app.merkl.xyz/opportunities/<chain>/<type>/<pool_address>
```

**Example:**
```bash
python main.py --URL https://app.merkl.xyz/opportunities/plasma/CLAMM/0xCe4Ac514CA6a9db357CcCc105B7848d7fd37445d
```

> Always run from the project root directory so that the `src` package is resolved correctly.

## Installation

```bash
pip install -r requirements.txt
```

## Project Structure

```
uniswap-lp-analysis/
├── src/
│   ├── extract.py            # HyperSync on-chain data fetching
│   ├── decode_events.py      # ABI decoding for Uniswap V3 events (Mint, Burn, Swap, Collect)
│   ├── merkl_campaigns.py    # Merkl API client and campaign parsing
│   ├── metrics.py            # TVL, trading volume, collected fees calculations
│   └── survival_analysis.py  # Kaplan-Meier survival models and plots
├── notebooks/
│   ├── wbtc_EDA.ipynb        # Exploratory analysis — WBTC pool
│   └── weth_EDA.ipynb        # Exploratory analysis — WETH pool
├── data/                     # Raw parquet outputs from HyperSync (gitignored)
├── main.py                   # CLI entrypoint
├── requirements.txt
└── .gitignore
```

## Outputs

- **Kaplan-Meier survival curve** — overall LP retention probability over time
- **Segmented KM curves** — LPs entering before vs. during a Merkl incentive campaign
- **Exit time histogram** — distribution of days-to-exit for fully exited LPs
