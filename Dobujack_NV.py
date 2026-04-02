script_as_string = """
import requests
import pandas as pd
import gspread
import os
import json
import time
from datetime import date
from google.oauth2.service_account import Credentials


# ====================================
# META CONFIG
# ====================================

ACCESS_TOKEN = os.environ["META_TOKEN"]

ACCOUNTS = {
    "Shopee_NV": "237156437315256",
    "Tokopedia_NV": "1017051122093087",
    "Lazada_NV": "335812939198419"
}


# ====================================
# SAFE REQUEST (ANTI TIMEOUT)
# ====================================

def safe_request(url, params=None):

    for i in range(3):

        try:
            response = requests.get(url, params=params, timeout=60)
            return response

        except requests.exceptions.RequestException:

            print(f"Retry {i+1}/3")
            time.sleep(5)

    raise Exception("Meta API gagal")


# ====================================
# FETCH META DATA
# ====================================

def fetch_meta_data(account_id):

    url = f"https://graph.facebook.com/v18.0/act_{account_id}/insights"

    params = {

        "fields": "campaign_name,impressions,spend,actions",

        "date_preset": "today",

        "level": "campaign",

        "action_report_time": "conversion",
        "action_attribution_windows": ["7d_click","1d_view"],

        "access_token": ACCESS_TOKEN
    }

    all_data = []

    response = safe_request(url, params)
    result = response.json()

    while True:

        if "data" in result:
            all_data.extend(result["data"])

        if "paging" in result and "next" in result["paging"]:
            response = safe_request(result["paging"]["next"])
            result = response.json()
        else:
            break


    df = pd.DataFrame(all_data)

    if df.empty:
        return None


    # ====================================
    # FILTER NV
    # ====================================

    df = df[
        df["campaign_name"].str.contains("NV", case=False, na=False) &
        ~df["campaign_name"].str.contains("RM", case=False, na=False)
    ].copy()


    # ====================================
    # ACTION PARSER
    # ====================================

    def get_action(actions, types):

        if isinstance(actions, list):

            for a in actions:

                if a["action_type"] in types:
                    return int(a["value"])

        return 0


    df["clicks"] = df["actions"].apply(
        lambda x: get_action(x, ["link_click"])
    )

    df["purchases"] = df["actions"].apply(
        lambda x: get_action(x, ["purchase","omni_purchase","offsite_conversion.purchase"])
    )


    cols = ["impressions","spend","clicks","purchases"]

    df[cols] = df[cols].apply(pd.to_numeric)

    daily = df[cols].sum()


    # convert numpy → python int
    daily = {k: int(v) for k,v in daily.items()}

    return daily


# ====================================
# GOOGLE SHEETS AUTH
# ====================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

service_account_info = json.loads(
    os.environ["GOOGLE_SERVICE_ACCOUNT"]
)

creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=SCOPES
)

gc = gspread.authorize(creds)

spreadsheet = gc.open("Import Range Dobujack")


# ====================================
# PROCESS PLATFORM
# ====================================

today = str(date.today())

for sheet_name, account_id in ACCOUNTS.items():

    print(f"\\nProcessing {sheet_name}")

    data = fetch_meta_data(account_id)

    if data is None:
        print("No data returned")
        continue

    worksheet = spreadsheet.worksheet(sheet_name)


    # ====================================
    # ANTI DUPLICATE DATE
    # ====================================

    existing_dates = worksheet.col_values(1)

    if today in existing_dates:

        print(f"{sheet_name} already has data for {today}")
        continue


    # ====================================
    # APPEND DATA
    # ====================================

    row = [
        today,
        data["impressions"],
        data["spend"],
        data["clicks"],
        data["purchases"]
    ]

    worksheet.append_row(row, value_input_option="USER_ENTERED")

    print(f"{sheet_name} appended for {today}")

    time.sleep(1)
"""
