"""
DITA Realtime Scheduler
Auto-scraping data cuaca setiap 10 menit untuk monitoring realtime.
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

SCRAPER_PATH = Path(__file__).parent.parent.parent / "scraper" / "scrape.py"
DATA_DIR = Path(__file__).parent.parent / "data"
REALTIME_FILE = DATA_DIR / "dieng_realtime_current.json"

class RealtimeScheduler:
    def __init__(self, interval_seconds=600):  # Default 10 menit
        self.interval = interval_seconds
        self.running = False
        self.last_update = None
        self.update_count = 0
        
    async def scrape_now(self):
        """Jalankan scraper realtime sekali."""
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(SCRAPER_PATH),
                "--realtime",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
            except asyncio.TimeoutError:
                process.kill()
                await process.communicate()
                print("[Scheduler] Scrape timeout setelah 30 detik")
                return False
            except asyncio.CancelledError:
                if process.returncode is None:
                    process.kill()
                await process.wait()
                raise
            
            if process.returncode == 0:
                self.last_update = datetime.now()
                self.update_count += 1
                print(f"[Scheduler] Realtime scrape #{self.update_count} berhasil - {self.last_update}")
                return True
            else:
                error = stderr.decode(errors="replace").strip()
                print(f"[Scheduler] Scrape gagal: {error or stdout.decode(errors='replace').strip()}")
                return False
        except Exception as e:
            print(f"[Scheduler] Error: {e}")
            return False
    
    async def start(self):
        """Mulai scheduler loop."""
        self.running = True
        print(f"[Scheduler] Started - interval {self.interval}s")
        
        # Scrape pertama kali saat start
        await self.scrape_now()
        
        while self.running:
            await asyncio.sleep(self.interval)
            if self.running:
                await self.scrape_now()
    
    def stop(self):
        """Stop scheduler."""
        self.running = False
        print("[Scheduler] Stopped")
    
    def get_status(self):
        """Status scheduler."""
        return {
            "running": self.running,
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "update_count": self.update_count,
            "interval_seconds": self.interval,
        }
    
    def get_latest_data(self):
        """Ambil data realtime terbaru dari file."""
        if REALTIME_FILE.exists():
            with open(REALTIME_FILE, "r") as f:
                return json.load(f)
        return None


# Singleton instance
_scheduler = None

def get_scheduler(interval_seconds=600):
    global _scheduler
    if _scheduler is None:
        _scheduler = RealtimeScheduler(interval_seconds)
    return _scheduler
