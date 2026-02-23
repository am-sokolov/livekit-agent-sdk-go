#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VIDEO_URL="https://sync-one2.harkwood.co.uk/media/Sync-One2_Test_720p_59.94_H.264_PCM_Stereo.mov.zip"
ZIP_FILE="source_video.zip"
OUTPUT_H264_FILE="test.mp4"
OUTPUT_AV1_FILE="testav1.mp4"

echo "Downloading test video from $VIDEO_URL..."
curl -L -o "$ZIP_FILE" "$VIDEO_URL"

echo "Extracting archive..."
unzip -o "$ZIP_FILE"

# Find the extracted .mov file
MOV_FILE="$(find . -maxdepth 1 -name "*.mov" -type f -print -quit)"

if [ -z "$MOV_FILE" ]; then
    echo "Error: No .mov file found in archive"
    exit 1
fi

echo "Converting $MOV_FILE to H.264 + Opus ($OUTPUT_H264_FILE)..."
ffmpeg -i "$MOV_FILE" \
    -c:v copy \
    -c:a libopus -b:a 128k \
    -y "$OUTPUT_H264_FILE"

echo "Converting $MOV_FILE to AV1 + Opus ($OUTPUT_AV1_FILE)..."
ENCODERS="$(ffmpeg -hide_banner -encoders 2>/dev/null)"
if echo "$ENCODERS" | grep -q "libsvtav1"; then
    ffmpeg -i "$MOV_FILE" \
        -c:v libsvtav1 -preset 8 -crf 35 \
        -c:a libopus -b:a 128k \
        -y "$OUTPUT_AV1_FILE"
elif echo "$ENCODERS" | grep -q "libaom-av1"; then
    ffmpeg -i "$MOV_FILE" \
        -c:v libaom-av1 -crf 35 -b:v 0 -cpu-used 8 -row-mt 1 \
        -c:a libopus -b:a 128k \
        -y "$OUTPUT_AV1_FILE"
else
    echo "Error: ffmpeg does not have an AV1 encoder (need libsvtav1 or libaom-av1)"
    exit 1
fi

echo "Cleaning up temporary files..."
rm -f "$ZIP_FILE" "$MOV_FILE"
rm -rf __MACOSX

echo "Done! Test videos created:"
ls -lh "$OUTPUT_H264_FILE" "$OUTPUT_AV1_FILE"
