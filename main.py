from src.extract import hypersync_indexer
import src.decode_events as de
import src.metrics as m
import src.survival_analysis as surv
import src.merkl_campaigns as merkl
import argparse
import pandas as pd
import asyncio

def main():
    parser = argparse.ArgumentParser(description='LP survival analysis')
    parser.add_argument('--URL', type=str, required=True, help='Merkl CLAMM campaign URL')
    args = parser.parse_args()

    merkl_URL = args.URL

    ## Getting campaign data from merkl URL

    chain_name, type, explorer_address = merkl.parse_merkl_URL(merkl_URL)
    opportunityId, token0, token1 = merkl.get_opportunity_details(merkl_URL)
    campaigns = merkl.get_campaigns(opportunityId)

    DECIMALS0 = token0["decimals"]
    DECIMALS1 = token1["decimals"]

    pool_pair = token0["symbol"].values[0].lower() + "_" + token1["symbol"].values[0].lower()

    out_path = "data/" + chain_name + "_" + pool_pair + "_events"

    # Fetching raw data from blockchain indexer (Hypersync - Envio)
    asyncio.run(hypersync_indexer(chain_name,explorer_address,out_path))

    ## Loading events 
    logs_path = out_path + "/logs.parquet"
    blocks_path = out_path + "/blocks.parquet"
    pool_events = de.load_events(logs_path, blocks_path)
    

    ## Decoding events
    pool_events = de.decode_topic0(pool_events)
    INITIAL_TIMESTAMP = pool_events[pool_events["event"] == "Initialize"]["timestamp"].iloc[0]
    
    mint_events_decoded = de.decode_mint_events(pool_events[pool_events["event"] == "Mint"])
    burn_events_decoded = de.decode_burn_events(pool_events[pool_events["event"] == "Burn"])
    swap_events_decoded = de.decode_swap_events(pool_events[pool_events["event"] == "Swap"])

    ## Computing metrics
    # TVL variation for (ideally) APR change, fallback to delta-TVL % if not possible
    # not used for now, Cox model is not very informative
    tvl_variation = m.tvl(mint_events_decoded,
                          burn_events_decoded,
                          swap_events_decoded,
                          DECIMALS0,
                          DECIMALS1,
                          token0["is_usd"].values[0],
                          token1["is_usd"].values[0]
                          )
    
    # LP summary in counting format for survival analysis
    _ , lp_summary = m.verify_lp_exit(mint_events_decoded, burn_events_decoded)

    lp_summary = merkl.entered_during_campaign(lp_summary, campaigns)

    ## Survival analysis
    
    # Kaplan-Meier estimator
    kmf = surv.km_scurve(lp_summary)
    kmf_segmented = surv.km_scurve_segmented(lp_summary)
    
    surv.exit_time_distribution(lp_summary)

    # Cox Proportional Hazards model : constant covariates
    # cox_const_cov_data = surv.data_for_constant_covariates_analysis(lp_summary)
    # cph_const = surv.cox_constant_covariates(cox_const_cov_data)

    # # Cox Proportional Hazards model : time-varying covariates    
    # num_active_campaigns = merkl.num_active_campaigns_at_t(campaigns)
    # cox_time_varying_cov_data \
    # = surv.data_for_time_varying_covariates_analysis(
    #                                                 lp_summary, 
    #                                                 num_active_campaigns,
    #                                                 tvl_variation,
    #                                                 INITIAL_TIMESTAMP
    #                                                 )

    # cph_time_varying = surv.cox_time_varying_covariates(cox_time_varying_cov_data)






if __name__ == "__main__":
    main()










