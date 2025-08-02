import os
import sys
import time
import shutil
import cv2
import json
import logging
import subprocess
import redis
import hashlib
from ultralytics import YOLO
from postgres_wrapper import PostgresWrapper, VideoProcessingResultFields

# Load config
with open("config.json", "r") as f:
    CONFIG = json.load(f)
    
logging.basicConfig(
    filename="pose_extractor.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

model = YOLO(CONFIG["model_path"])

# all fields that MUST be present in the config.json file
expected_config_keys = ['input_folder', 'pose_data_folder', 'processed_folder', 'scan_interval_sec', 'model_path',
                        'supported_file_formats', 'db_table_name']

def get_video_codec(video_path: str) -> str:
    """
    extracts and returns the video codec if possible, N/A if error in process
    :param video_path: full path of the video file
    :return: video codec (i.e. h264 etc.) or N/A if an error occurs
    """
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name",
                "-of", "json",
                video_path
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        info = json.loads(result.stdout)
        codec = info["streams"][0]["codec_name"] if "streams" in info and info["streams"] else "N/A"
        logging.info(f"code: {codec}")
        return codec
    except Exception as e:
        logging.error(f"Failed to extract codec: {e}")
        return 'N/A'

def extract_metadata(video_path: str) -> VideoProcessingResultFields:
    """
    extract and return video file metadata - filename, duration, resolution, frame rate, codec
    :param video_path: full path of the video file
    :return: dict containing video metadata
    """
    filename = os.path.basename(video_path)
    metadata = VideoProcessingResultFields(filename)

    try:
        cap = cv2.VideoCapture(video_path)

        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        logging.info(f"Frame rate: {fps:.2f} FPS")
        metadata.frame_rate = f'{fps:.2f}'

        duration = frame_count / fps if fps else 0
        logging.info(f"Duration: {duration:.2f} seconds")
        metadata.duration_seconds = duration

        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        logging.info(f"resolution: {width} x {height}")
        metadata.resolution = f'{width} x {height}'

        metadata.codec = get_video_codec(video_path)

        cap.release()
    except Exception as e:
        logging.error(f"error while trying to extract metadata for file {video_path} - {e}")
    else:
        logging.info(f"finished extracting metadata for file {video_path}")
    finally:
        return metadata

def is_corrupted(video_path: str) -> bool:
    """
    check if video is corrupted or not
    :param video_path: full path of the video file
    :return: True if video is corrupted, False otherwise
    """
    corrupted = True
    cap = cv2.VideoCapture(video_path)

    if not cap.isOpened():
        logging.info("Failed to open video. It may be corrupted or unsupported.")
    else:
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        success, frame = cap.read()
        if not success or frame is None:
            logging.info("Unable to read frames. Video may be corrupted.")
        else:
            logging.info(f"Video opened successfully. Frame count: {frame_count}")
            corrupted = False

    cap.release()
    return corrupted

def pose_detection(video_path: str) -> str:
    """
    runs a pose detection using YOLOv11n pose model on the video file, and stores the data in a json file.
    :param video_path: full path of the video file
    :return: the path to the json file containing the pose data
    """
    filename = os.path.basename(video_path)
    name, _ = os.path.splitext(filename)
    json_path = 'N/A'

    try:
        cap = cv2.VideoCapture(video_path)
        pose_data = []
        frame_count = 0

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            results = model.predict(source=frame, task='pose', conf=0.25)
            frame_info = {
                "frame": frame_count,
                "keypoints": [kp.tolist() for kp in results[0].keypoints.data]
            }
            pose_data.append(frame_info)
            frame_count += 1

        cap.release()
        logging.info(f"pose detection process for file {video_path} finished successfully.")
    except Exception as e:
        logging.error(f'unable to perform pose detection on video {video_path} - {e}')
    else:
        json_path = os.path.join(CONFIG["pose_data_folder"], f"{name}_pose.json")
        with open(json_path, "w") as f:
            json.dump(pose_data, f, indent=2)

    return json_path

def move_processed_file(video_path: str) -> None:
    """
    move the video file after done processing.
    :param video_path: full path of the video file
    :return: None
    """
    try:
        filename = os.path.basename(video_path)
        destination = os.path.join(CONFIG["processed_folder"], filename)
        shutil.move(video_path, destination)
    except FileNotFoundError:
        logging.error(f"File not found: {video_path}")
    except PermissionError:
        logging.error(f"Permission denied when moving {video_path} to {destination}")
    except OSError as e:
        logging.error(f"OS error during move: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    else:
        logging.info(f"moved {filename} to {destination}")

def init_config() -> bool:
    """
    check that config.json has all necessary parameters in order to run the script.
    :return: True if config.json is valid, False otherwise
    """
    response = True
    for key in expected_config_keys:
        if key not in CONFIG:
            logging.error(f"config.json file missing required key: {key}")
            response = False

    return response


if __name__ == "__main__":
    valid_config = init_config()

    if not valid_config:
        logging.error("init configuration failed, please check config.json. exiting...")
        sys.exit(1)

    os.makedirs(CONFIG["input_folder"], exist_ok=True)
    os.makedirs(CONFIG["pose_data_folder"], exist_ok=True)
    os.makedirs(CONFIG["processed_folder"], exist_ok=True)
    supported_formats = tuple(CONFIG["supported_file_formats"])

    # create connection with postgresql
    db_con = PostgresWrapper()

    # Connect to Redis
    r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", 6379)), decode_responses=True)

    while True:
        logging.info("Scanning for new videos...")

        for file in os.listdir(CONFIG["input_folder"]):
            if file.lower().endswith(supported_formats):
                should_process = True
                full_path = os.path.join(CONFIG["input_folder"], file)
                try:
                    logging.info(f"*** got new file to process - {file} ***")

                    # check if video has already been processed
                    file_hash = hashlib.md5(open(full_path, 'rb').read()).hexdigest()
                    if r.exists(file_hash):
                        logging.info(f"for file {file}, hash {file_hash} already processed. skipping...")
                        should_process = False

                    if should_process:
                        video_metadata = extract_metadata(full_path)

                        if not is_corrupted(full_path):
                            video_metadata.pose_file_path = pose_detection(full_path)
                            video_metadata.corrupted = False
                        else:
                            logging.info(f'{file} is corrupted. will not perform pose detection.')
                            video_metadata.corrupted = True

                        if db_con.is_connected:
                            db_con.db_insert(db_table= CONFIG["db_table_name"], row_data= video_metadata)
                except Exception as e:
                    logging.error(f"error while trying to process {file} - {e}")
                else:
                    if should_process:
                        logging.info(f'finished processing {file}')
                        # store file hash in redis
                        r.set(file_hash, "processed")
                finally:
                    move_processed_file(full_path)

        logging.info(f"Waiting {CONFIG['scan_interval_sec']} seconds...")
        time.sleep(CONFIG["scan_interval_sec"])
