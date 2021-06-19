import pandas as pd
import datetime
import sys
import glob, os
from os import listdir, path, sep, makedirs
import warnings

warnings.filterwarnings("ignore")

print("Start combining daily report files")

COLUMNS = ["participant_ID", "date", "study_mode", "eod_original_prompted_num", "eod_completed_num",
           "burst_ema_original_prompted_num", "burst_ema_completed_num", "sleep_time_reported_hr",
           "phone_off_min", "watch_off_dur_shut", "samples_collected_percent", "samples_collected_hr",
           "uema_prompted_num", "uema_completed_num",
           "nondata_time_computed_hr", "sleep_time_computed_hr", "watch_charging_min", "watch_charging_num_bouts",
           "start_date", "end_Date",
           "day_of_week", "days_into_study", "version_code", "watch_assigned",
           "is_postponed_burst_mode_day", "current_wake_time", "current_sleep_time",
           "next_wake_time", "wear_time_computed_hr", "nonwear_time_computed_hr",
           "computed_sleep_time_when_sleeping_hr", "computed_nonwear_time_when_sleeping_hr",
           "sleep_time_change_num", "wake_time_change_num", "previous_sleep_time", "previous_wake_time",
           "phone_dnd_mode_min", "watch_dnd_mode_min", "watch_off_min",
           "sleep_qs_prompted_num", "sleep_qs_reprompt_num",
           "sleep_qs_started_num", "sleep_qs_completed_num",
           "sleep_qs_completed_first_prompt_num", "sleep_qs_completed_reprompt_num",
           "burst_ema_prompted_num", "burst_ema_reprompt_num",
           "burst_ema_started_num", "burst_ema_completed_first_prompt_num", "burst_ema_completed_reprompt_num",
           "eod_prompted_num", "eod_reprompt_num", "eod_started_num", "eod_completed_first_prompt_num",
           "eod_completed_reprompt_num", "set_types_presented", "all_ema_ques_skip_num", "uema_partial_completed_num",
           "uema_cs_prompted", "uema_cs_completed", "uema_trivia_prompted", "uema_trivia_completed",
           "uema_undo_num", "uema_validation_perc",
           "sleep_qs_completion_rate", "sleep_qs_first_prompt_completion_rate",
           "burst_ema_completion_rate", "burst_ema_first_prompt_completion_rate", "burst_ema_compliance_rate",
           "burst_ema_9h_compliance_rate", "burst_ema_comp_compliance",
           "uema_completion_rate", "uema_compliance_rate", "uema_9h_compliance_rate",
           "all_ema_surv_resp_time_mean", "all_ema_surv_resp_time_med",
           "sleep_surv_resp_time_mean", "burst_ema_surv_resp_time_mean", "burst_ema_surv_resp_time_med",
           "eod_surv_resp_time_mean", "eod_surv_resp_time_med", "uema_resp_time_mean", "uema_resp_time_med",
           "all_ema_response_latency_from_first_prompt_mean", "all_ema_response_latency_from_first_prompt_med",
           "all_ema_response_latency_from_reprompt_mean", "all_ema_response_latency_from_reprompt_med",
           "all_ema_single_quest_resp_time_mean", "all_ema_single_quest_resp_time_med",
           "all_ema_single_ques_resp_time_without_latency_mean",
           "all_ema_single_ques_resp_time_without_latency_med", "phone_charging_num_bouts", "phone_charging_min",
           "watch_full_notif_count",
           "watch_battery_low_notif_count", "watch_service_not_run_notif_count",
           "watch_update_notif_count", "watch_turn_off_dnd_notif_count", "watch_connect_notf_count",
           "phone_unlock_num", "location_data_avail_min", "location_clusters_identified",
           "notifs_received_excluding_TIME_num", "unique_apps_used_num",
           "daily_step_count", "watch_no_motion_min", "watch_sampling_rate_range",
           "watch_available_memory_MB", "watch_available_RAM", "phone_wifi_off_min",  # "phone_accel_summary",
           "phone_logs_size_KB", "phone_data_size_MB",
           "watch_logs_size_KB", "watch_data_size_MB", "watch_data_files_num", "samples_collected"]


def mhealth_timestamp_parser(str):
    MHEALTH_TIMESTAMP_FORMAT = "%m/%d/%Y %H:%M:%S.%f"
    return datetime.datetime.strptime(str, MHEALTH_TIMESTAMP_FORMAT)


in_path = sys.argv[1]

combined_report_folder = "combined_report"

out_path = in_path + sep + combined_report_folder

count = 0

first = True

relevant_df = pd.DataFrame()

for path in glob.glob(os.path.join(in_path, "*/*/phone_watch_daily_report_clean_*.csv")):
    print('Reading ...' + path)
    try:
        df = pd.read_csv(path, header=0, sep=',', compression="infer", quotechar='"')
        if first:
            first = False
            relevant_df = df
        else:
            relevant_df = pd.concat([relevant_df, df], axis=0, join='outer', ignore_index=True)
    except:
        continue

relevant_df.columns = COLUMNS
relevant_df = relevant_df[COLUMNS]
if not os.path.exists(out_path):
    os.makedirs(out_path)
relevant_df.to_csv(os.path.join(out_path, "combined_report.csv"), index=False)
print("Total days combined: " + str(relevant_df.shape[0]))
