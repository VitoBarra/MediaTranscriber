import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from DataProcessing import RAW_VIDEO_FOLDER, RAW_AUDIO_FOLDER, \
    SPLITTED_AUDIO_FOLDER, HTML_OUTPUT_FOLDER, OUTPUT_TRANSCRIPT, ENHANCED_AUDIO_FOLDER, SPLITTED_VIDEO_FOLDER
from DataProcessing.AudioEnhancer import EnhanceAudioFolder
from DataProcessing.AudioExtractor import AudioFormat, VideoFolderToAudio
from DataProcessing.HTMLToMDConverter import ExtractTextFromFolder
from DataProcessing.MediaSplitter import SplitMediaInFolder
from DataProcessing.VideoCreator import AudioFolderToVideo
from DataProcessing.ffmpegUtil import VideoFormat
from Utility.FileUtil import WriteJson
from Utility.Logger import LogLevel, Logger
from WebScraper import PROXY_FILE
from WebScraper.AnyToText import tryUpload_AnyToText
from WebScraper.ProxyUtil import getProxyList
from WebScraper.VideoTranscriptJobDescriptor import GenerateJobsFromVideo
from WebScraper.VzardAIUploader import tryUpload_VzardAi
from WebScraper.WebScrapingUtility import JobStatus

# --- Settings ---
HEADLESS_MODE = True
DEFAULT_WORKERS = 8


def main():
    parser = argparse.ArgumentParser(
        description="Media Processing Pipelines",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-p", "--pipeline",
        choices=["audio", "video", "html"],
        help="Choose which pipeline to run: 'audio', 'video', 'html'"
    )
    parser.add_argument(
        "-s", "--split",
        type=int,
        default=15,
        help="Split length in minutes for audio/video chunks"
    )
    parser.add_argument(
        "-w", "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of parallel workers"
    )
    parser.add_argument(
        "-l", "--log-level",
        type=str,
        default="info",
        choices=[lvl.name.lower() for lvl in LogLevel],
        help="Set the minimum log level (debug, info, warning, error, critical)"
    )

    args = parser.parse_args()

    # --- Setup logger based on CLI arg ---
    level = LogLevel[args.log_level.upper()]
    Logger.setup(level=level)

    split_minutes = args.split
    workers = args.workers

    while not args.pipeline:
        Logger.GetConsole().print("\nSelect a pipeline to run:")
        Logger.GetConsole().print("1) Audio Pipeline (Video → Audio → HTML → Transcript)")
        Logger.GetConsole().print("2) Video Pipeline (Audio → Video → HTML → Transcript)")
        Logger.GetConsole().print("3) Html (HTML → Transcript)")
        Logger.GetConsole().print("4) Help (Show usage)")
        Logger.GetConsole().print("5) Exit")

        choice = input("Enter choice [1/2/3/4/5]: ").strip()
        if choice == "1":
            args.pipeline = "audio"
        elif choice == "2":
            args.pipeline = "video"
        elif choice == "3":
            args.pipeline = "html"
        elif choice == "4":
            parser.print_help()
        elif choice == "5":
            Logger.GetConsole().print("Exiting.")
            return
        else:
            Logger.GetConsole().print("Invalid choice. Try again.")

    if args.pipeline == "html":
        Logger.info("Running HTML -> transcript conversion only...")
        ExtractTextFromFolder(HTML_OUTPUT_FOLDER, OUTPUT_TRANSCRIPT)
        Logger.info("Transcript extraction complete.")
        return

    if args.pipeline == "audio":
        Logger.info("Starting Audio Pipeline...\n")
        AudioPipeline(split_minutes, workers)
    elif args.pipeline == "video":
        Logger.info("Starting Video Pipeline...\n")
        VideoPipeline(split_minutes, workers)


# --- Pipeline functions ---
def AudioPipeline(split_minutes: int, workers: int):
    Logger.info("Converting videos to audio...")
    VideoFolderToAudio(RAW_VIDEO_FOLDER, RAW_AUDIO_FOLDER, AudioFormat.WAV, overwrite=False)
    Logger.info("Video-to-audio conversion complete.")

    Logger.info(f"Splitting audio files into {split_minutes}-minute chunks...")
    SplitMediaInFolder(RAW_AUDIO_FOLDER, SPLITTED_AUDIO_FOLDER, 60 * split_minutes)
    Logger.info("Audio splitting complete.")

    Logger.info("Enhancing audio files (filtering, compression, gain)...")
    EnhanceAudioFolder(
        SPLITTED_AUDIO_FOLDER,
        ENHANCED_AUDIO_FOLDER,
        AudioFormat.WAV,
        lowcut=100,
        highcut=6000,
        compress_threshold_db=-30,
        compress_ratio=4,
        gain_db=8,
    )
    Logger.info("Audio enhancement complete.")

    Logger.info("Uploading audio chunks for transcription...")
    jobToDo = True
    while jobToDo:
        jobToDo = not WebScrapingJobLauncher(
            tryUpload_VzardAi,
            SPLITTED_AUDIO_FOLDER,
            HTML_OUTPUT_FOLDER,
            HEADLESS_MODE,
            workers
        )
    Logger.info("Upload complete.")

    Logger.info("Extracting transcript from uploaded results...")
    ExtractTextFromFolder(HTML_OUTPUT_FOLDER, OUTPUT_TRANSCRIPT)
    Logger.info("Transcript extraction complete.")

# --- Video functions ---
def VideoPipeline(split_minutes: int, workers: int):
    Logger.info("Converting audio to video...")
    AudioFolderToVideo(RAW_AUDIO_FOLDER, RAW_VIDEO_FOLDER, VideoFormat.MP4, overwrite=False)
    Logger.info("Audio-to-video conversion complete.")

    Logger.info(f"Splitting videos into {split_minutes}-minute chunks...")
    SplitMediaInFolder(RAW_VIDEO_FOLDER, SPLITTED_VIDEO_FOLDER, 60 * split_minutes)
    Logger.info("Video splitting complete.")

    Logger.info("Uploading video chunks for transcription...")
    jobToDo = True
    while jobToDo:
        jobToDo = not WebScrapingJobLauncher(
            tryUpload_VzardAi,
            SPLITTED_VIDEO_FOLDER,
            HTML_OUTPUT_FOLDER,
            HEADLESS_MODE,
            workers
        )
    Logger.info("Upload complete.")

    Logger.info("Extracting transcript from uploaded results...")
    ExtractTextFromFolder(HTML_OUTPUT_FOLDER, OUTPUT_TRANSCRIPT)
    Logger.info("Transcript extraction complete.")


def WebScrapingJobLauncher(uploadFunction, Input_folder=SPLITTED_VIDEO_FOLDER, output_folder=HTML_OUTPUT_FOLDER,
                           headless_Mode=False, workers: int = 8) -> bool:
    """
    :param uploadFunction:
    :param Input_folder:
    :param output_folder:
    :param headless_Mode:
    :param workers:
    :return: true if all jobs completed successfully
    """
    MAX_AGE_SECONDS = 1800
    MAX_RETRIES = 3
    proxy_failures = {}

    proxy_list = getProxyList(PROXY_FILE, MAX_AGE_SECONDS)
    video_jobs = GenerateJobsFromVideo(Input_folder, output_folder)
    incomplete_jobs = [job for job in video_jobs if not os.path.isfile(job.GetHTMLOutputFilePath())]

    if not incomplete_jobs:
        Logger.info("All transcription jobs completed successfully")
        return True

    while proxy_list:
        for job in list(incomplete_jobs):
            if job.Lock.locked():
                continue
            Logger.info(f"Processing transcription job: {job}")

            with ThreadPoolExecutor(max_workers=workers) as executor:
                proxyTried = 0
                futures = {executor.submit(uploadFunction, job, proxy, headless_Mode): proxy for proxy in proxy_list}
                proxy_to_try = len(proxy_list)
                for future in as_completed(futures):
                    proxyTried += 1
                    proxy = futures[future]
                    status = future.result()
                    proxy_str = f"{proxy['ip']}:{proxy['port']}"

                    Logger.info(f"Job progress, proxy tried: {proxyTried}/{proxy_to_try}")
                    if status == JobStatus.Success:
                        Logger.info(f"Job {job} completed successfully with proxy {proxy_str}")
                        job.IsCompleted = True
                        if job in incomplete_jobs:
                            incomplete_jobs.remove(job)
                        break
                    elif status == JobStatus.PageConnectionError:
                        if proxy in proxy_list:
                            proxy_list.remove(proxy)
                            WriteJson(PROXY_FILE, proxy_list)
                    elif status == JobStatus.GenericError:
                        proxy_failures[proxy_str] = proxy_failures.get(proxy_str, 0) + 1
                        if proxy_failures[proxy_str] >= MAX_RETRIES:
                            if proxy in proxy_list:
                                proxy_list.remove(proxy)
                            del proxy_failures[proxy_str]
                            WriteJson(PROXY_FILE, proxy_list)
                            Logger.warning(f"Removed proxy {proxy_str} after {MAX_RETRIES} failures")

                if not job.IsCompleted:
                    Logger.error(f"Upload failed for job {job}")

        time.sleep(2)

    return all(job.IsCompleted for job in video_jobs)


if __name__ == '__main__':
    main()
