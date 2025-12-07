import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import openpyxl
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ==========================================
# Configuration & Setup
# ==========================================
SCRIPT_NAME = "youtube_diy_trends.py"
DATA_FILENAME = "youtube_diy_sentiment.xlsx"
SEARCH_QUERY = "Gaming PC Build"
MAX_RESULTS = 20

# Retrieve API Key from Environment
API_KEY = os.environ.get("YOUTUBE_API_KEY")

if not API_KEY:
    print(f"[{SCRIPT_NAME}] ERROR: YOUTUBE_API_KEY environment variable not found.")
    sys.exit(1)

def get_youtube_data():
    """
    Fetches recent videos and calculates metrics.
    Returns a dictionary of metrics or None if failed.
    """
    try:
        # Build the YouTube service
        youtube = build('youtube', 'v3', developerKey=API_KEY)

        # Calculate date for "7 days ago" (RFC 3339 format required by YouTube)
        published_after = (datetime.utcnow() - timedelta(days=7)).isoformat("T") + "Z"

        print(f"[{SCRIPT_NAME}] Searching for '{SEARCH_QUERY}' published after {published_after}...")

        # 1. Search for videos (Returns ID and Snippet, but NO View Count)
        search_response = youtube.search().list(
            q=SEARCH_QUERY,
            part="id,snippet",
            maxResults=MAX_RESULTS,
            publishedAfter=published_after,
            type="video",
            order="relevance"  # Simulate standard user search behavior
        ).execute()

        search_items = search_response.get("items", [])

        if not search_items:
            print(f"[{SCRIPT_NAME}] No videos found for the given criteria.")
            return None

        # Extract Video IDs to fetch statistics
        video_ids = [item['id']['videoId'] for item in search_items]

        # 2. Fetch Video Statistics (Returns View Counts)
        stats_response = youtube.videos().list(
            part="statistics,snippet",
            id=",".join(video_ids)
        ).execute()

        stats_items = stats_response.get("items", [])

        # Process Metrics
        total_views = 0
        video_count = len(stats_items)
        
        # We use the title from the first SEARCH result as it is the most "relevant"
        top_video_title = search_items[0]['snippet']['title']

        for video in stats_items:
            # Some videos might hide view counts
            views = int(video['statistics'].get('viewCount', 0))
            total_views += views

        avg_views = int(total_views / video_count) if video_count > 0 else 0

        print(f"[{SCRIPT_NAME}] Success. Found {video_count} videos. Top: {top_video_title[:30]}...")
        
        return {
            "Total_Views_Top20": total_views,
            "Avg_Views_Top20": avg_views,
            "Top_Video_Title": top_video_title
        }

    except HttpError as e:
        print(f"[{SCRIPT_NAME}] API HttpError occurred: {e}")
        return None
    except Exception as e:
        print(f"[{SCRIPT_NAME}] General Error: {e}")
        return None

def update_excel(metrics):
    """
    Appends metrics to the Excel file in the ../data folder.
    Creates the file and headers if it doesn't exist.
    """
    # 1. Path Handling (Relative to script location)
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir.parent / "data"
    
    # Ensure data directory exists
    data_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = data_dir / DATA_FILENAME

    # 2. Excel Handling
    headers = ["Date", "Query", "Total_Views_Top20", "Avg_Views_Top20", "Top_Video_Title"]
    current_date = datetime.now().strftime("%Y-%m-%d")

    if not file_path.exists():
        # Create new workbook
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "YouTube Trends"
        ws.append(headers)
        print(f"[{SCRIPT_NAME}] Created new file: {file_path}")
    else:
        # Load existing workbook
        wb = openpyxl.load_workbook(file_path)
        ws = wb.active

    # Append Data
    row_data = [
        current_date, 
        SEARCH_QUERY, 
        metrics["Total_Views_Top20"], 
        metrics["Avg_Views_Top20"], 
        metrics["Top_Video_Title"]
    ]
    
    ws.append(row_data)
    wb.save(file_path)
    print(f"[{SCRIPT_NAME}] Data saved to {file_path}")

def main():
    metrics = get_youtube_data()
    if metrics:
        update_excel(metrics)
    else:
        print(f"[{SCRIPT_NAME}] Skipped Excel update due to missing data.")

if __name__ == "__main__":
    main()