# video-processing
this is a setup for periodically scanning video files and processing them.

setup: docker compose.

Requirements:
1. docker
2. docker compose

instructions:
1. download all files to 1 folder
2. create a folder called 'samples' and inside it 2 folders - 'to_be_processed', 'processed'
3. run it using: docker compose up --build
4. this will spawn 3 containers:
  a. pose_app - logic container
  b. pose_db - postgreSQL container
  c. pose_redis - redis container
5. move a video file (or any other file) to the folder samples/to_be_processed
6. file will be processed (after waiting time) and then moved to folder samples/processed
7. Pose Detection Processing will be written to samples/processed/pose_data/ as {original_file_name}_pose.json (only in case it is not corrupted).
8. log file is called pose_extractor.log inside container pose_app

logics:
1. docker compose because we need 2 offical containers (postgreSQL & redis) for ease of use without any modifications and installations
2. redis db in order to skip processing of already processed files (detection by file hash, so even same file with different name will not be processed) - faster executions and avoiding redundant resource usage.
