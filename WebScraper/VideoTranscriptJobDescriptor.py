import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path

from DataProcessing import VIDEO_EXTENSIONS, AUDIO_EXTENSIONS
from Utility.FileUtil import ReadJson, WriteJson


class VideoTranscriptJobDescriptor:

    def __init__(self, videoProjectFolder: Path | str, videoPath: Path | str, outFolder: Path | str, metadata: dict):
        self.Lock = threading.Lock()
        self.IsCompleted = False
        self.VideoPath = Path(videoPath)
        self.VideoProjectFolder = Path(videoProjectFolder)
        self.Language = metadata.get("Language", "english")

        # File di output .txt con lo stesso nome del video
        self.OutputFolder = Path(outFolder)
        self.OutputFilePath = self.VideoProjectFolder / f"{self.VideoPath.stem}.txt"

    def __str__(self):
        return f" project: {self.VideoProjectFolder.name} component: {self.VideoPath.name} completed: {self.IsCompleted}"

    def GetHTMLOutputFilePath(self) -> Path:
        return self.OutputFolder / self.VideoProjectFolder / f"{self.VideoPath.stem}.html"


def GenerateJobsFromVideo(video_folder: Path | str, out_folder_html: Path | str) -> list[VideoTranscriptJobDescriptor]:
    jobs = []
    video_folder = Path(video_folder)

    # Loop ricorsivo su tutte le sottocartelle
    for root, _, files in os.walk(video_folder):
        root_path = Path(root)
        if root_path.name.lower() == video_folder.name.lower():
            continue
        video_files = [f for f in files if f.lower().endswith(VIDEO_EXTENSIONS+AUDIO_EXTENSIONS)]
        metadata_file = root_path / "metadata.json"

        if video_files:
            try:
                metadata = ReadJson(metadata_file)
            except (json.JSONDecodeError, IOError):
                metadata = {"Language": "english"}
                if not metadata_file.exists():
                    WriteJson(metadata_file, metadata)

            jobs.extend(
                [
                    VideoTranscriptJobDescriptor(
                        root_path.name,
                        root_path / file,
                        out_folder_html,
                        metadata,
                    )
                    for file in video_files
                ]
            )

    return jobs


@dataclass(frozen=True)
class SharePointLinkJob:
    name: str
    url: str
