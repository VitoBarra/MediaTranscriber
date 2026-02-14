import sys
from pathlib import Path


if getattr(sys, 'frozen', False):
    # in installed app
    DATA_PROC_BASE_DIR = Path(sys.executable).parent
else:
    # in normal python environment
    DATA_PROC_BASE_DIR = Path(__file__).parent.parent/"data"

RAW_AUDIO_FOLDER = DATA_PROC_BASE_DIR / "1.1-RawAUDIO"
RAW_VIDEO_FOLDER = DATA_PROC_BASE_DIR / "1.2-RawVIDEO"
RAW_SHAREDPOINT_FOLDER = DATA_PROC_BASE_DIR / "1.3-RawLINK"
SPLITTED_AUDIO_FOLDER = DATA_PROC_BASE_DIR / "2.1-SplittedAUDIO"
SPLITTED_VIDEO_FOLDER = DATA_PROC_BASE_DIR / "2.2-SplittedVIDEO"
HTML_OUTPUT_FOLDER = DATA_PROC_BASE_DIR / "3.1-HTML"
JSON_OUTPUT_FOLDER = DATA_PROC_BASE_DIR / "3.1-JSON"
OUTPUT_TRANSCRIPT = DATA_PROC_BASE_DIR / "4-Transcript"
ENHANCED_AUDIO_FOLDER = DATA_PROC_BASE_DIR / "EXTRA-EnhancedAUDIO"

folders = [OUTPUT_TRANSCRIPT, HTML_OUTPUT_FOLDER,
           RAW_VIDEO_FOLDER , SPLITTED_VIDEO_FOLDER,
           RAW_AUDIO_FOLDER , SPLITTED_AUDIO_FOLDER, ENHANCED_AUDIO_FOLDER,
           RAW_SHAREDPOINT_FOLDER]

for folder in folders:
    folder.mkdir(parents=True, exist_ok=True)

VIDEO_EXTENSIONS = (".mp4", ".mov", ".3gp", ".avi", ".mkv")
AUDIO_EXTENSIONS = (".mp3", ".wav", ".m4a")
