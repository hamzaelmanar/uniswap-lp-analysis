import json
import http.client
from collections import OrderedDict
import pandas as pd
import re

def parse_merkl_URL(URL):
    
    path_params = URL.split("/")

    chain_name = path_params[-3]
    type = path_params[-2]
    explorer_address = path_params[-1]

    return chain_name, type, explorer_address




def get_opportunity_details(URL):
    chain_name, type, explorer_address = parse_merkl_URL(URL)

    conn = http.client.HTTPSConnection("api.merkl.xyz")
    headers = {"Accept": "*/*"}

    conn.request(
        "GET",
        f"/v4/opportunities?chainName={chain_name}&type={type}&explorerAddress={explorer_address}",
        headers=headers
    )

    res = conn.getresponse()
    data = res.read()

    opportunity = json.loads(data.decode("utf-8"), object_pairs_hook=OrderedDict)[0]
    opportunityId = opportunity["id"]
    tokens = opportunity.get("tokens", [])

    def _is_usd_stablecoin_symbol(symbol: str) -> bool:
        if not symbol:
            return False

        s = symbol.strip().upper()

        # 1) Exact known symbols (recommended core)
        known = {
            "USDT", "USDC", "DAI", "USDE", "USDM", "FDUSD", "TUSD", "USDP", "GUSD", "LUSD", "FRAX",
            "USD₮", "USDC.E", "USDT.E"
        }
        if s in known:
            return True

        # 2) Regex fallback (for variants)
        # Examples matched: "USDm", "USDC.e", "xUSD", "sUSD"
        return bool(re.search(r"(^|[^A-Z])(USD|USDT|USDC|DAI|USDE|USDM|FDUSD|TUSD|USDP|GUSD|LUSD|FRAX)([^A-Z]|$)", s))
    
    token0 = {
        "decimals": tokens[0]["decimals"] if len(tokens) > 0 else None,
        "symbol": tokens[0]["symbol"] if len(tokens) > 0 else None,
        "price": tokens[0]["price"] if len(tokens) > 0 else None,
        "updatedAt": tokens[0]["updatedAt"] if len(tokens) > 0 else None,
        "is_usd": _is_usd_stablecoin_symbol(tokens[0]["symbol"]) if len(tokens) > 0 else False,
    }

    token1 = {
        "decimals": tokens[1]["decimals"] if len(tokens) > 1 else None,
        "symbol": tokens[1]["symbol"] if len(tokens) > 1 else None,
        "price": tokens[1]["price"] if len(tokens) > 1 else None,
        "updatedAt": tokens[1]["updatedAt"] if len(tokens) > 1 else None,
        "is_usd": _is_usd_stablecoin_symbol(tokens[1]["symbol"]) if len(tokens) > 0 else False
    }

    opportunity_id_str = str(opportunityId) if opportunityId is not None else ""
    token0_df = pd.DataFrame([token0])
    token1_df = pd.DataFrame([token1])

    return opportunity_id_str, token0_df, token1_df
    
def get_campaigns(opportunityId):
    
    conn = http.client.HTTPSConnection("api.merkl.xyz")
    headers = {"Accept": "*/*"}

    conn.request("GET", f"/v4/campaigns?opportunityId={opportunityId}", headers=headers)

    res = conn.getresponse()
    data = res.read()

    campaigns = json.loads(data.decode("utf-8"), object_pairs_hook=OrderedDict)

    return parse_campaigns(campaigns)


def parse_campaigns(campaigns_list):
    """
    Parse multiple campaigns from a list of OrderedDict objects.
    
    Args:
        campaigns_list: List of OrderedDict objects containing campaign data
    
    Returns:
        list: List of dictionaries with extracted campaign data for each campaign
    """
    parsed_campaigns = []
    
    for campaign_data in campaigns_list:
        # Convert OrderedDict to dict for easier access
        campaign = dict(campaign_data) if isinstance(campaign_data, OrderedDict) else campaign_data
        
        # Extract basic campaign info
        campaign_id = campaign.get('id')
        start_timestamp = campaign.get('startTimestamp')
        end_timestamp = campaign.get('endTimestamp')
        amount = campaign.get('amount')
        
        # Extract params data
        params = campaign.get('params', {})
        if isinstance(params, OrderedDict):
            params = dict(params)
        
        symbol_reward_token = params.get('symbolRewardToken')
        decimals_reward_token = params.get('decimalsRewardToken')
        
        # Extract daily rewards breakdown data
        daily_rewards_breakdown = campaign.get('dailyRewardsBreakdown', [])
        
        # Default values if breakdown exists
        daily_rewards_value = None
        daily_rewards_token_symbol = None
        
        if daily_rewards_breakdown and len(daily_rewards_breakdown) > 0:
            first_breakdown = daily_rewards_breakdown[0]
            if isinstance(first_breakdown, OrderedDict):
                first_breakdown = dict(first_breakdown)
            
            daily_rewards_value = first_breakdown.get('value')
            
            # Extract token info from breakdown
            token_info = first_breakdown.get('token', {})
            if isinstance(token_info, OrderedDict):
                token_info = dict(token_info)
            
            daily_rewards_token_symbol = token_info.get('symbol')
        
        # Add to parsed campaigns list
        parsed_campaigns.append({
            'campaign_id': campaign_id,
            'start_timestamp': start_timestamp,
            'end_timestamp': end_timestamp,
            'amount': amount,
            'decimals_reward_token': decimals_reward_token,
            'daily_rewards_value': daily_rewards_value,
            'daily_rewards_token_symbol': daily_rewards_token_symbol
        })
    
    return pd.DataFrame(parsed_campaigns)

# inutile je pense ? 
def add_campaign_duration_in_days(campaigns):
    campaigns["campaign_duration_in_days"] = ((campaigns["end_timestamp"] - campaigns["start_timestamp"]) / 86400).round().astype(int)
    return campaigns

def entered_during_campaign(lp_positions, campaigns):
    global_start = campaigns["start_timestamp"].min()
    global_end = campaigns["end_timestamp"].max()

    lp_positions["entered_during_campaign"] = lp_positions["first_mint_timestamp"].between(global_start, global_end)

    return lp_positions

# covariate to be integrated with lifelines.add_covariate()
def num_active_campaigns_at_t(campaigns):
    """
    Build an end-of-interval campaign-count table in a survival-analysis style.

    """

    # Use unique end points as interval ends (timestamps where we evaluate count).
    end_points = sorted(campaigns["end_timestamp"].dropna().astype("int64").unique())

    rows = []
    for t in end_points:
        active_count = (
            (campaigns["start_timestamp"] < t) &
            (campaigns["end_timestamp"] >= t)
        ).sum()
        rows.append({"timestamp": int(t), "num_active_campaigns": int(active_count)})

    num_active_campaigns = pd.DataFrame(rows, columns=["timestamp", "num_active_campaigns"])

    return num_active_campaigns
