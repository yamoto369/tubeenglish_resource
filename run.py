import os
import json
import csv
import time
import base64
import urllib.parse
from typing import Dict, Any, Optional, List
import requests  # type: ignore

VT_DIR = 'vtsubtitle'
YT_DIR = 'ytsubtitle'
PROGRESS_FILE = 'progress.json'
CSV_FILE = 'allvideos_rows.csv'
MAX_PER_SESSION = 100
MAX_RETRIES = 3
DELTA_TIME = 3.5

# Status constants
STATUS_OK = 'ok'
STATUS_NO_SUBTITLE = 'no_subtitle'
STATUS_FAILED = 'failed'


def setup_dirs():
    os.makedirs(VT_DIR, exist_ok=True)
    os.makedirs(YT_DIR, exist_ok=True)


def load_progress() -> Dict[str, Any]:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}


def save_progress(progress: Dict[str, Any]):
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def is_done(entry: Dict[str, Any]) -> bool:
    """Return True if both vt and yt are finished (no need to retry)."""
    vt_done = entry.get('vt_status') in (STATUS_OK, STATUS_NO_SUBTITLE)
    yt_done = entry.get('yt_status') in (STATUS_OK, STATUS_NO_SUBTITLE)
    return vt_done and yt_done


def fetch_vt_transcript(vtid: str) -> Optional[List[Dict]]:
    url = f"https://vtapi.voicetube.com/v2.1.1/enUS/videos/{vtid}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.status_code != 200:
        return None
    data = resp.json()
    if not data or 'data' not in data:
        return None

    caption_lines = data['data'].get('captionLines')
    if not caption_lines:
        return None

    transcript = []
    for line in caption_lines:
        start_at = float(line.get('startAt', 0))
        duration = float(line.get('duration', 0))
        original_text = line.get('originalText')
        if not original_text:
            continue
        text = original_text.get('text', '')
        end_time = start_at + duration
        transcript.append({
            "startTime": start_at,
            "endTime": end_time,
            "text": text
        })
    return transcript if transcript else None


def create_lang_protobuf(lang_code: str) -> bytearray:
    lang_bytes = lang_code.encode('utf-8')
    res = bytearray([0x0A, 0x03])
    res.extend(b'asr')
    res.append(0x12)
    res.append(len(lang_bytes))
    res.extend(lang_bytes)
    res.extend([0x1A, 0x00])
    return res


def create_params_protobuf(video_id: str, url_encoded_lang: str) -> bytearray:
    vid_bytes = video_id.encode('utf-8')
    lang_bytes = url_encoded_lang.encode('utf-8')
    res = bytearray([0x0A, len(vid_bytes)])
    res.extend(vid_bytes)
    res.append(0x12)
    res.append(len(lang_bytes))
    res.extend(lang_bytes)
    res.extend([0x18, 0x01])
    return res


def encode_params(video_id: str, lang_code: str = 'en') -> str:
    lang_proto = create_lang_protobuf(lang_code)
    lang_b64 = base64.b64encode(lang_proto).decode('utf-8')
    url_encoded = urllib.parse.quote(lang_b64).replace('=', '%3D')

    params_proto = create_params_protobuf(video_id, url_encoded)
    return base64.b64encode(params_proto).decode('utf-8')


def extract_initial_segments(data: Dict) -> Optional[List]:
    actions = data.get('actions')
    if not actions or len(actions) == 0:
        return None

    action = actions[0]
    elements_command = action.get('elementsCommand', {})
    transform_entity_command = elements_command.get('transformEntityCommand', {})
    arguments = transform_entity_command.get('arguments', {})
    transform_args = arguments.get('transformTranscriptSegmentListArguments', {})
    overwrite = transform_args.get('overwrite', {})
    initial_segments = overwrite.get('initialSegments')

    if not initial_segments or len(initial_segments) == 0:
        return None

    return initial_segments


def parse_segments(initial_segments: List) -> List[Dict]:
    segments = []
    for segment in initial_segments:
        renderer = segment.get('transcriptSegmentRenderer')
        if not renderer:
            continue

        snippet = renderer.get('snippet', {})
        elements_str = snippet.get('elementsAttributedString', {})
        content = elements_str.get('content')

        if content is not None:
            start_ms = int(renderer.get('startMs', 0))
            end_ms = int(renderer.get('endMs', 0))
            segments.append({
                "startTime": start_ms / 1000.0,
                "endTime": end_ms / 1000.0,
                "text": content
            })
    return segments


def fetch_yt_transcript(video_id: str, lang_code: str = 'en') -> Optional[List[Dict]]:
    params_str = encode_params(video_id, lang_code)

    payload = {
        'context': {
            'client': {
                'hl': 'en',
                'gl': 'US',
                'clientName': 'IOS',
                'clientVersion': '19.29.1',
                'deviceModel': 'iPhone14,5',
                'userAgent': 'com.google.ios.youtube/19.29.1 (iPhone14,5; U; CPU iOS 17_5_1 like Mac OS X;)',
                'timeZone': 'Asia/Ho_Chi_Minh'
            }
        },
        'params': params_str,
    }

    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'com.google.ios.youtube/19.29.1 (iPhone14,5; U; CPU iOS 17_5_1 like Mac OS X;)',
        'X-Youtube-Client-Name': '5',
        'X-Youtube-Client-Version': '19.29.1',
        'Origin': 'https://www.youtube.com'
    }

    url = 'https://www.youtube.com/youtubei/v1/get_transcript'

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code != 200:
        return None

    data = resp.json()
    initial_segments = extract_initial_segments(data)
    if not initial_segments:
        return None

    return parse_segments(initial_segments)


def download_with_retry(fetch_fn, video_id: str, label: str) -> str:
    """
    Try to fetch a transcript up to MAX_RETRIES times.

    Returns:
        STATUS_OK          — fetched and data available (caller saves file)
        STATUS_NO_SUBTITLE — server returned a valid response but no subtitle exists
        STATUS_FAILED      — all retries exhausted due to errors
    """
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = fetch_fn(video_id)
            if data is None:
                # Server responded OK but there is no subtitle
                print(f"    [{label}] No subtitle found for {video_id}.")
                return STATUS_NO_SUBTITLE
            return data  # type: ignore[return-value]  # caller checks type
        except Exception as e:
            last_exception = e
            print(f"    [{label}] Attempt {attempt}/{MAX_RETRIES} failed for {video_id}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(DELTA_TIME)

    print(f"    [{label}] All retries failed for {video_id}. Last error: {last_exception}")
    return STATUS_FAILED


def process_video(row_id: str, vtid: str, ytid: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    """Download missing/failed subtitles for a single video and return updated entry."""
    vt_status = entry.get('vt_status', STATUS_FAILED)
    yt_status = entry.get('yt_status', STATUS_FAILED)

    # --- VoiceTube ---
    if vtid and vt_status not in (STATUS_OK, STATUS_NO_SUBTITLE):
        result = download_with_retry(fetch_vt_transcript, vtid, 'VT')
        if isinstance(result, list):
            with open(os.path.join(VT_DIR, f"{vtid}.json"), 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"    [VT] Saved transcript for {vtid}.")
            vt_status = STATUS_OK
        else:
            vt_status = result  # STATUS_NO_SUBTITLE or STATUS_FAILED
    elif not vtid:
        vt_status = STATUS_NO_SUBTITLE

    # --- YouTube ---
    if ytid and yt_status not in (STATUS_OK, STATUS_NO_SUBTITLE):
        result = download_with_retry(fetch_yt_transcript, ytid, 'YT')
        if isinstance(result, list):
            with open(os.path.join(YT_DIR, f"{ytid}.json"), 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"    [YT] Saved transcript for {ytid}.")
            yt_status = STATUS_OK
        else:
            yt_status = result
    elif not ytid:
        yt_status = STATUS_NO_SUBTITLE

    return {
        "vtid": vtid,
        "ytid": ytid,
        "vt_status": vt_status,
        "yt_status": yt_status,
        "timestamp": time.time()
    }


def main():
    setup_dirs()
    progress = load_progress()

    if not os.path.exists(CSV_FILE):
        print(f"CSV file {CSV_FILE} not found!")
        return

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total_videos = len(rows)
    print(f"Found {total_videos} videos in CSV.")

    processed = 0
    for i, row in enumerate(rows):
        if processed >= MAX_PER_SESSION:
            print(f"Reached session limit of {MAX_PER_SESSION} videos. Stopping.")
            break

        row_id = row.get('id', '')
        vtid = row.get('vtid', '')
        ytid = row.get('ytid', '')

        existing = progress.get(row_id, {})

        # Skip if both subtitles are already finished
        if existing and is_done(existing):
            continue

        print(f"[{i+1}/{total_videos}] (session {processed+1}/{MAX_PER_SESSION}) ID {row_id} | YT: {ytid} | VT: {vtid}")

        progress[row_id] = process_video(row_id, vtid, ytid, existing)
        save_progress(progress)

        processed += 1
        time.sleep(DELTA_TIME)

    print(f"\nSession complete. Processed {processed} videos.")


if __name__ == "__main__":
    main()
