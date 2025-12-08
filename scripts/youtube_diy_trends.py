import os
import sys
from datetime import datetime
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
MAX_RESULTS = 100

# SET THIS TO TRUE TO SEE DETAILED OUTPUT IN CONSOLE
DEBUG = False 

# Retrieve API Key from Environment
API_KEY = "AIzaSyCHwNxI4HSv5cbLx3praqwLv7w_1YdGeCM"

if not API_KEY:
    print(f"[{SCRIPT_NAME}] ERROR: YOUTUBE_API_KEY environment variable not found.")
    sys.exit(1)

def get_youtube_data():
    """
    Fetches top 100 most relevant videos (no date limit) and calculates metrics.
    """
    try:
        youtube = build('youtube', 'v3', developerKey=API_KEY)

        print(f"[{SCRIPT_NAME}] Searching for '{SEARCH_QUERY}' (Top {MAX_RESULTS} by Relevance)...")

        # 1. Search for videos (Pagination loop)
        video_ids = []
        top_video_title = "N/A"
        next_page_token = None
        
        while len(video_ids) < MAX_RESULTS:
            remaining = MAX_RESULTS - len(video_ids)
            fetch_count = min(remaining, 50) 

            # NOTE: Removed 'publishedAfter' to remove the day cap
            search_response = youtube.search().list(
                q=SEARCH_QUERY,
                part="id,snippet",
                maxResults=fetch_count,
                type="video",
                order="relevance",
                pageToken=next_page_token
            ).execute()

            items = search_response.get("items", [])
            if not items:
                break

            # Capture top title from the very first result
            if not next_page_token and items:
                top_video_title = items[0]['snippet']['title']

            for item in items:
                video_ids.append(item['id']['videoId'])

            next_page_token = search_response.get("nextPageToken")
            if not next_page_token:
                break

        print(f"[{SCRIPT_NAME}] Found {len(video_ids)} relevant videos. Fetching stats...")

        if not video_ids:
            return None

        # 2. Fetch Video Statistics (Chunked)
        total_views = 0
        video_count = 0
        
        def chunked_list(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        if DEBUG:
            print(f"\n{'='*20} DEBUG: INDIVIDUAL VIDEO STATS {'='*20}")

        for chunk in chunked_list(video_ids, 50):
            # Fetch snippet AND statistics to get titles for debug
            stats_response = youtube.videos().list(
                part="statistics,snippet",
                id=",".join(chunk)
            ).execute()

            for video in stats_response.get("items", []):
                views = int(video['statistics'].get('viewCount', 0))
                title = video['snippet']['title']
                
                if DEBUG:
                    print(f"[Views: {views:,}] {title}")

                total_views += views
                video_count += 1

        if DEBUG:
            print(f"{'='*60}\n")

        avg_views = int(total_views / video_count) if video_count > 0 else 0

        print(f"[{SCRIPT_NAME}] Success. Analyzed {video_count} videos. Avg Views: {avg_views:,}")
        
        return {
            "Total_Views": total_views,
            "Avg_Views": avg_views,
            "Top_Video_Title": top_video_title
        }

    except HttpError as e:
        print(f"[{SCRIPT_NAME}] API HttpError occurred: {e}")
        return None
    except Exception as e:
        print(f"[{SCRIPT_NAME}] General Error: {e}")
        return None

def update_excel(metrics):
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    file_path = data_dir / DATA_FILENAME

    # Updated headers (Removed "30d" since we removed the cap)
    headers = ["Date", "Query", "Total_Views_Top100", "Avg_Views_Top100", "Top_Video_Title"]
    current_date = datetime.now().strftime("%Y-%m-%d")

    if not file_path.exists():
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "YouTube Trends"
        ws.append(headers)
    else:
        wb = openpyxl.load_workbook(file_path)
        ws = wb.active

    row_data = [
        current_date, 
        SEARCH_QUERY, 
        metrics["Total_Views"], 
        metrics["Avg_Views"], 
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