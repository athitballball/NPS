import json
import os
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --- Config ---
SPREADSHEET_ID    = os.environ["SPREADSHEET_ID"]
SOURCE_SHEETS     = ["opd", "IPD"]
TARGET_COLUMNS    = ["OS004", "date_create", "cguid"]
OUTPUT_SHEET_NAME = "NPS"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_service():
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def read_sheet(service, sheet_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=sheet_name
    ).execute()
    rows = result.get("values", [])
    if not rows:
        print(f"  [{sheet_name}] ไม่มีข้อมูล")
        return pd.DataFrame()
    headers = rows[0]
    data    = rows[1:]
    data    = [r + [""] * (len(headers) - len(r)) for r in data]
    df      = pd.DataFrame(data, columns=headers)

    missing = [c for c in TARGET_COLUMNS if c not in df.columns]
    if missing:
        print(f"  [{sheet_name}] ไม่พบ column: {missing} — ข้ามไป")
        return pd.DataFrame()

    df = df[TARGET_COLUMNS].copy()
    df["source_sheet"] = sheet_name
    print(f"  [{sheet_name}] {len(df)} rows")
    return df

def ensure_output_sheet(service):
    meta   = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = [s["properties"]["title"] for s in meta["sheets"]]
    if OUTPUT_SHEET_NAME not in sheets:
        body = {"requests": [{"addSheet": {"properties": {"title": OUTPUT_SHEET_NAME}}}]}
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body=body
        ).execute()
        print(f"สร้าง sheet ใหม่: {OUTPUT_SHEET_NAME}")

def write_sheet(service, df):
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=OUTPUT_SHEET_NAME
    ).execute()
    values = [df.columns.tolist()] + df.fillna("").values.tolist()
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{OUTPUT_SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()
    print(f"เขียน {len(df)} rows → '{OUTPUT_SHEET_NAME}' เรียบร้อย")

def main():
    service = get_service()
    frames  = [read_sheet(service, s) for s in SOURCE_SHEETS]
    frames  = [f for f in frames if not f.empty]

    if not frames:
        print("ไม่มีข้อมูลให้ concat")
        return

    combined = pd.concat(frames, ignore_index=True)
    ensure_output_sheet(service)
    write_sheet(service, combined)

if __name__ == "__main__":
    main()
