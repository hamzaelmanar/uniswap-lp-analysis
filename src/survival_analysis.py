from lifelines import CoxTimeVaryingFitter, KaplanMeierFitter, CoxPHFitter
from lifelines.utils import add_covariate_to_timeline, to_long_format
import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines.utils import median_survival_times
from lifelines.statistics import logrank_test
import matplotlib.pyplot as plt

from src.merkl_campaigns import num_active_campaigns_at_t

### Kaplan-Meier estimator :
def km_scurve(lp_summary):
    # Prepare data (convert seconds -> days)
    durations_days = lp_summary["duration"] / 86400
    events = lp_summary["status"]

    # Fit Kaplan-Meier model
    kmf = KaplanMeierFitter()
    kmf.fit(
        durations=durations_days,
        event_observed=events,
        label="All LPs",
    )

    # Plot survival curve
    plt.figure(figsize=(12, 7))
    kmf.plot_survival_function(at_risk_counts=True)

    # Add median survival marker
    median_survival_days = kmf.median_survival_time_
    plt.axvline(
        median_survival_days,
        color="red",
        linestyle=":",
        alpha=0.5,
        label="Median survival",
    )

    # Styling
    plt.title("Kaplan-Meier Survival Curve - All LPs", fontsize=14)
    plt.xlabel("Duration (days)", fontsize=12)
    plt.ylabel("Probability of Still Providing Liquidity", fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()

    return kmf

def km_scurve_segmented(lp_summary):
    plt.figure(figsize=(12, 7))

    # Plot segmented KM curves
    last_kmf = None
    for entered_during_campaign in sorted(lp_summary["entered_during_campaign"].dropna().unique()):
        mask = lp_summary["entered_during_campaign"] == entered_during_campaign
        durations_days = lp_summary.loc[mask, "duration"] / 86400
        events = lp_summary.loc[mask, "status"]

        kmf = KaplanMeierFitter()
        kmf.fit(
            durations=durations_days,
            event_observed=events,
            label=f"{entered_during_campaign} (n={mask.sum()})",
        )
        kmf.plot_survival_function(ci_show=True)
        last_kmf = kmf

    # Styling
    plt.title(
        "Survival Curves: LPs entering (False) before campaign vs (True) during campaign",
        fontsize=14,
    )
    plt.xlabel("Duration (days)", fontsize=12)
    plt.ylabel("Survival Probability", fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()

    return last_kmf

def exit_time_distribution(lp_summary):
    # 5. Additional analysis: Time to exit distribution
    plt.figure(figsize=(12, 5))

    # Histogram of exit times for those who exited
    exited_durations = lp_summary[lp_summary['status'] == 1]['duration']
    plt.hist(exited_durations / 86400, bins=30, alpha=0.7, edgecolor='black')
    plt.xlabel('Days to Exit')
    plt.ylabel('Number of LPs')
    plt.title('Distribution of Exit Times (Exited LPs)')
    plt.grid(True, alpha=0.3)
    plt.show()

# ### Cox Model :
# def data_for_constant_covariates_analysis(lp_summary):
#     # Prepare data for Cox model
#     cox_data = lp_summary[["duration", "status", "closed_positions","entered_during_campaign"]].copy()
#     cox_data["entered_during_campaign"] = cox_data["entered_during_campaign"].astype(int)
#     return cox_data

# def cox_constant_covariates(cox_data):
#     cph = CoxPHFitter()
#     cph.fit(cox_data, duration_col="duration", event_col="status")
#     cph.print_summary()
#     return cph

# def data_for_time_varying_covariates_analysis(lp_summary, campaigns,tvl_variation,INITIAL_TIMESTAMP):
#     # Prepare data for time-varying covariates analysis
#     cv_num_active_campaigns = num_active_campaigns_at_t(campaigns)

#     tvl_variation["duration"] = tvl_variation["timestamp"] - INITIAL_TIMESTAMP
#     cv_num_active_campaigns["duration"] = cv_num_active_campaigns["timestamp"] - INITIAL_TIMESTAMP

#     def _merge_owners_with_cv(tvl_variation, cv_num_active_campaigns):
#         owners = tvl_variation[["owner"]].drop_duplicates().reset_index(drop=True)

#         owners["_k"] = 1
#         cv_num_active_campaigns["_k"] = 1

#         cv_num_active_campaigns = (
#             owners.merge(cv_num_active_campaigns, on="_k")
#             .drop(columns="_k")
#             [["owner", "timestamp", "num_active_campaigns", "duration"]]
#         )
#         return cv_num_active_campaigns

#     cv_num_active_campaigns = _merge_owners_with_cv(tvl_variation,cv_num_active_campaigns)

#     cv_tvl_change_pct = tvl_variation[["owner", "duration", "delta_tvl_pct"]].copy()

#     base_df = lp_summary[["owner", "duration", "status", "closed_positions","entered_during_campaign"]].copy()
#     base_df = to_long_format(base_df, duration_col="duration")
   
#     cox_df = add_covariate_to_timeline(base_df, cv_num_active_campaigns, 
#                                        duration_col = "duration", id_col="owner", 
#                                        event_col="status")
#     cox_df = add_covariate_to_timeline(cox_df, cv_tvl_change_pct,
#                                        duration_col = "duration", id_col="owner",
#                                        event_col="status")
#     return cox_df

# def cox_time_varying_covariates(cox_data):
    # ctv = CoxTimeVaryingFitter(penalizer=0.1)    
    # ctv.fit(cox_data, id_col="owner", event_col="status", start_col="start",stop_col="stop")
    # ctv.print_summary()
    # return ctv