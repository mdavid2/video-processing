CREATE TABLE IF NOT EXISTS video_processing_results (
    video_filename TEXT PRIMARY KEY,
    duration_seconds FLOAT,
    resolution TEXT,
    codec TEXT,
    frame_rate FLOAT,
    corrupted BOOLEAN,
    pose_file_path TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
