#!/usr/bin/env python3
"""
YouTube 24/7 Live Streamer with Google Drive Support
Supports large files from Google Drive (1-2GB+)
"""

import os
import sys
import subprocess
import time
import re
import requests
from pathlib import Path

class YouTubeStreamer:
    def __init__(self):
        self.stream_key = os.getenv('YOUTUBE_STREAM_KEY')
        self.video_url = os.getenv('VIDEO_URL')
        self.quality = os.getenv('VIDEO_QUALITY', '720p')
        self.aspect_ratio = os.getenv('ASPECT_RATIO', '16:9')
        
        # Streaming settings
        self.rtmp_url = f"rtmp://a.rtmp.youtube.com/live2/{self.stream_key}"
        self.video_file = "video.mp4"
        self.max_retries = 3
        self.retry_delay = 5
        
    def print_status(self, message, emoji="ℹ️"):
        """Print status with emoji"""
        print(f"{emoji} {message}")
        sys.stdout.flush()
    
    def is_google_drive_url(self, url):
        """Check if URL is from Google Drive"""
        return 'drive.google.com' in url or 'docs.google.com' in url
    
    def extract_gdrive_id(self, url):
        """Extract file ID from Google Drive URL"""
        patterns = [
            r'/file/d/([a-zA-Z0-9_-]+)',
            r'id=([a-zA-Z0-9_-]+)',
            r'/open\?id=([a-zA-Z0-9_-]+)',
            r'/d/([a-zA-Z0-9_-]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None
    
    def download_from_gdrive(self, file_id, output_path):
        """Download large files from Google Drive using gdown method"""
        self.print_status("Downloading from Google Drive (large file method)...", "📥")
        
        # Method 1: Try using requests with proper headers
        session = requests.Session()
        
        # Google Drive direct download URL
        base_url = "https://drive.google.com/uc?export=download"
        
        try:
            # First request
            self.print_status("Getting download page...", "🔍")
            response = session.get(base_url, params={'id': file_id}, stream=True)
            
            # Extract confirm token from response
            token = None
            for key, value in response.cookies.items():
                if key.startswith('download_warning'):
                    token = value
                    break
            
            # If token not in cookies, look in HTML
            if not token:
                content = response.text
                match = re.search(r'confirm=([^&"]+)', content)
                if match:
                    token = match.group(1)
                    self.print_status(f"Found confirmation token: {token[:20]}...", "🔑")
            
            # Second request with token
            if token:
                params = {'id': file_id, 'confirm': token}
            else:
                params = {'id': file_id, 'confirm': 't'}  # Try generic confirmation
            
            self.print_status("Starting download...", "⬇️")
            response = session.get(base_url, params=params, stream=True, timeout=60)
            
            # Check if we got HTML error page
            content_type = response.headers.get('content-type', '')
            if 'text/html' in content_type:
                self.print_status("Got HTML response, trying alternative method...", "⚠️")
                return self.download_gdrive_alternative(file_id, output_path)
            
            # Download the file
            total_size = int(response.headers.get('content-length', 0))
            
            if total_size > 0:
                self.print_status(f"File size: {total_size / (1024*1024):.1f} MB", "📊")
            else:
                self.print_status("Downloading (size unknown)...", "📊")
            
            downloaded = 0
            chunk_size = 32768
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Progress every 50MB
                        if downloaded % (1024 * 1024 * 50) < chunk_size:
                            self.print_status(f"Downloaded: {downloaded/(1024*1024):.1f} MB", "⬇️")
            
            file_size = os.path.getsize(output_path)
            self.print_status(f"Download complete: {file_size/(1024*1024):.1f} MB", "✅")
            
            # Verify file is valid
            if file_size < 10000:  # Less than 10KB is probably an error page
                self.print_status("Downloaded file too small, trying alternative...", "⚠️")
                return self.download_gdrive_alternative(file_id, output_path)
            
            return True
            
        except Exception as e:
            self.print_status(f"Download error: {e}", "❌")
            return self.download_gdrive_alternative(file_id, output_path)
    
    def download_gdrive_alternative(self, file_id, output_path):
        """Alternative download method using gdown or wget"""
        self.print_status("Trying alternative download method...", "🔄")
        
        # Try installing and using gdown
        try:
            self.print_status("Installing gdown...", "📦")
            subprocess.run([sys.executable, '-m', 'pip', 'install', 'gdown', '-q'], 
                         check=True, timeout=60)
            
            self.print_status("Downloading with gdown...", "📥")
            url = f"https://drive.google.com/uc?id={file_id}"
            
            result = subprocess.run(
                ['gdown', url, '-O', output_path, '--fuzzy'],
                capture_output=True,
                text=True,
                timeout=1800  # 30 minutes timeout
            )
            
            if result.returncode == 0 and os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                self.print_status(f"Download complete: {file_size/(1024*1024):.1f} MB", "✅")
                return file_size > 10000
            else:
                self.print_status(f"gdown failed: {result.stderr[:100]}", "❌")
                
        except Exception as e:
            self.print_status(f"Alternative method failed: {e}", "❌")
        
        return False
    
    def download_video(self):
        """Download video from URL (supports Google Drive)"""
        self.print_status("Preparing video download...", "🎬")
        
        # Check if it's a Google Drive URL
        if self.is_google_drive_url(self.video_url):
            self.print_status("Detected Google Drive URL", "🔍")
            
            file_id = self.extract_gdrive_id(self.video_url)
            
            if not file_id:
                self.print_status("Error: Could not extract file ID from Google Drive URL", "❌")
                self.print_status(f"URL: {self.video_url}", "📋")
                return False
            
            self.print_status(f"File ID: {file_id}", "🆔")
            
            # Download using special method for large files
            return self.download_from_gdrive(file_id, self.video_file)
        
        else:
            # Regular download for non-Google Drive URLs
            self.print_status("Downloading from direct URL...", "📥")
            
            try:
                response = requests.get(self.video_url, stream=True, timeout=30)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                
                with open(self.video_file, 'wb') as f:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            if total_size > 0 and downloaded % (1024 * 1024 * 10) < 8192:
                                progress = (downloaded / total_size) * 100
                                self.print_status(f"Downloaded: {downloaded/(1024*1024):.1f}MB ({progress:.1f}%)", "⬇️")
                
                self.print_status("Download complete!", "✅")
                return True
                
            except Exception as e:
                self.print_status(f"Download error: {e}", "❌")
                return False
    
    def get_video_duration(self):
        """Get video duration using ffprobe"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                self.video_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                return None
                
            duration = float(result.stdout.strip())
            
            hours = int(duration // 3600)
            minutes = int((duration % 3600) // 60)
            seconds = int(duration % 60)
            
            self.print_status(f"Video duration: {hours:02d}:{minutes:02d}:{seconds:02d}", "⏱️")
            return duration
            
        except Exception as e:
            self.print_status(f"Could not determine duration: {e}", "⚠️")
            return None
    
    def start_streaming(self):
        """Start FFmpeg streaming in loop"""
        self.print_status("Starting FFmpeg stream...", "🚀")
        self.print_status(f"Quality: {self.quality}", "🎬")
        self.print_status(f"Aspect Ratio: {self.aspect_ratio}", "📐")
        
        # Parse quality
        if 'p' in self.quality:
            height = int(self.quality.replace('p', ''))
            
            # Calculate width based on aspect ratio
            if self.aspect_ratio == '16:9':
                width = int(height * 16 / 9)
            elif self.aspect_ratio == '9:16':
                width = int(height * 9 / 16)
            elif self.aspect_ratio == '4:3':
                width = int(height * 4 / 3)
            elif self.aspect_ratio == '1:1':
                width = height
            else:
                width = int(height * 16 / 9)
            
            width = width if width % 2 == 0 else width + 1
        else:
            width, height = 1280, 720
        
        # Video bitrate based on quality
        bitrate_map = {
            360: '800k',
            480: '1200k',
            720: '2500k',
            1080: '4500k',
            1440: '9000k',
            2160: '20000k'
        }
        video_bitrate = bitrate_map.get(height, '2500k')
        
        self.print_status(f"Output resolution: {width}x{height}", "📺")
        self.print_status(f"Video bitrate: {video_bitrate}", "💾")
        
        # FFmpeg command for 24/7 loop
        ffmpeg_cmd = [
            'ffmpeg',
            '-stream_loop', '-1',  # Loop forever
            '-re',
            '-i', self.video_file,
            '-vf', f'scale={width}:{height}:force_original_aspect_ratio=decrease,pad={width}:{height}:(ow-iw)/2:(oh-ih)/2',
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-b:v', video_bitrate,
            '-maxrate', video_bitrate,
            '-bufsize', str(int(video_bitrate.replace('k', '')) * 2) + 'k',
            '-pix_fmt', 'yuv420p',
            '-g', '60',
            '-c:a', 'aac',
            '-b:a', '128k',
            '-ar', '44100',
            '-f', 'flv',
            self.rtmp_url
        ]
        
        self.print_status("🔴 LIVE - Stream running 24/7...", "▶️")
        self.print_status("Stream will continue until workflow timeout", "⏰")
        
        # Start streaming with auto-restart
        attempt = 0
        while attempt < 999:  # Virtually unlimited retries
            try:
                self.print_status(f"Stream session {attempt + 1}", "🔄")
                
                process = subprocess.run(
                    ffmpeg_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                
                # If stream ends, retry
                self.print_status(f"Stream ended (code {process.returncode}), restarting...", "🔄")
                    
            except KeyboardInterrupt:
                self.print_status("Stream stopped by user", "🛑")
                return True
                
            except Exception as e:
                self.print_status(f"Stream error: {e}", "❌")
            
            attempt += 1
            time.sleep(self.retry_delay)
        
        return True
    
    def run(self):
        """Main execution"""
        self.print_status("=== YouTube 24/7 Live Streamer ===", "🎥")
        
        # Validate inputs
        if not self.stream_key:
            self.print_status("ERROR: YOUTUBE_STREAM_KEY not set", "❌")
            return False
        
        if not self.video_url:
            self.print_status("ERROR: VIDEO_URL not set", "❌")
            return False
        
        self.print_status(f"Stream Key: {self.stream_key[:8]}...{self.stream_key[-4:]}", "🔑")
        self.print_status(f"Video URL: {self.video_url[:60]}...", "🔗")
        
        # Download video
        if not self.download_video():
            self.print_status("Failed to download video", "❌")
            return False
        
        # Verify video file
        if not os.path.exists(self.video_file):
            self.print_status("Video file not found after download", "❌")
            return False
        
        file_size = os.path.getsize(self.video_file)
        self.print_status(f"Video file size: {file_size / (1024*1024):.2f} MB", "📁")
        
        if file_size < 10000:
            self.print_status("Video file too small - download failed", "❌")
            return False
        
        # Get duration
        self.get_video_duration()
        
        # Start streaming
        self.start_streaming()
        
        self.print_status("=== Stream Session Complete ===", "✅")
        return True

def main():
    try:
        streamer = YouTubeStreamer()
        success = streamer.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️ Stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()