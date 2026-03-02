from datetime import datetime, timedelta, timezone
from pathlib import Path
import openpyxl
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ==========================================
# Configuration & Setup
# ==========================================
SCRIPT_NAME = "youtube_diy_trends.py"
DATA_FILENAME = "youtube_diy_sentiment.xlsx"
PRIMARY_QUERY = "Gaming PC Build"
API_KEY = "AIzaSyCHwNxI4HSv5cbLx3praqwLv7w_1YdGeCM"

# Window for counting new upload velocity (how many videos published recently)
UPLOAD_WINDOW_DAYS = 7
MAX_UPLOAD_COUNT = 2000

# ==========================================
# Helpers
# ==========================================

def get_iso_date(days_ago: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def fetch_video_ids(youtube, query, published_after, order, max_results):
    """Page through search results and return a list of video IDs."""
    video_ids = []
    next_page_token = None

    while len(video_ids) < max_results:
        fetch_count = min(max_results - len(video_ids), 50)
        resp = youtube.search().list(
            q=query,
            part="id",
            maxResults=fetch_count,
            type="video",
            order=order,
            publishedAfter=published_after,
            pageToken=next_page_token,
        ).execute()

        items = resp.get("items", [])
        if not items:
            break

        for item in items:
            video_ids.append(item["id"]["videoId"])

        next_page_token = resp.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids

# ==========================================
# Signal – YouTube upload velocity
# ==========================================

def get_youtube_data():
    """
    Tracks upload velocity: counts how many new 'Gaming PC Build' videos
    were published in the past 7 days.  When real interest rises, more
    creators publish content → count goes up.  Completely independent of
    YouTube's relevance/recommendation algorithm.
    """
    try:
        youtube = build("youtube", "v3", developerKey=API_KEY)

        published_after_7d = get_iso_date(UPLOAD_WINDOW_DAYS)
        print(f"[{SCRIPT_NAME}] Counting new uploads in last {UPLOAD_WINDOW_DAYS} days...")
        upload_ids = fetch_video_ids(
            youtube, PRIMARY_QUERY, published_after_7d,
            order="date", max_results=MAX_UPLOAD_COUNT
        )
        new_upload_count = len(upload_ids)
        print(f"[{SCRIPT_NAME}] New uploads (last {UPLOAD_WINDOW_DAYS}d): {new_upload_count}")

        return {
            "new_uploads_7d": new_upload_count,
        }

    except HttpError as e:
        print(f"[{SCRIPT_NAME}] YouTube API HttpError: {e}")
        return None
    except Exception as e:
        print(f"[{SCRIPT_NAME}] YouTube error: {e}")
        return None

# ==========================================
# Excel output
# ==========================================

HEADERS = [
    "Date",
    "Query",
    # Upload velocity: number of new videos published in the last 7 days
    "New_Uploads_Last7d",
]

def update_excel(yt_metrics):
    script_dir = Path(__file__).resolve().parent
    data_dir   = script_dir.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    file_path  = data_dir / DATA_FILENAME

    if not file_path.exists():
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "YouTube Trends"
        ws.append(HEADERS)
    else:
        wb = openpyxl.load_workbook(file_path)
        ws = wb.active
        # Refresh header row if the file predates this script version
        existing = [ws.cell(row=1, column=i).value for i in range(1, ws.max_column + 1)]
        if existing != HEADERS:
            for col, h in enumerate(HEADERS, 1):
                ws.cell(row=1, column=col).value = h

    ws.append([
        datetime.now().strftime("%Y-%m-%d"),
        PRIMARY_QUERY,
        yt_metrics["new_uploads_7d"],
    ])

    wb.save(file_path)
    print(f"[{SCRIPT_NAME}] Data saved to {file_path}")

# ==========================================
# Entry point
# ==========================================

def main():
    yt_metrics = get_youtube_data()

    if yt_metrics:
        update_excel(yt_metrics)
    else:
        print(f"[{SCRIPT_NAME}] Skipped Excel update due to missing YouTube data.")

if __name__ == "__main__":
    main()