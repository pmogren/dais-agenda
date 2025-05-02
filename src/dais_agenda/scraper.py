import json
import logging
import requests
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import re
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import uuid
import ast

logger = logging.getLogger(__name__)

class DaisScraper:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.sessions_dir = self.data_dir / "sessions"
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = "https://www.databricks.com/dataaisummit/agenda"
        self.driver = None

    def setup_driver(self):
        """Set up Chrome WebDriver with appropriate options."""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Run in headless mode
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Successfully set up Chrome WebDriver")
        except Exception as e:
            logger.error(f"Error setting up Chrome WebDriver: {e}")
            raise

    def extract_session_data(self, data: Dict) -> List[Dict]:
        """Extract and format session data from the raw data structure."""
        sessions = []
        
        # Handle different data structures
        if "sessions" in data:
            raw_sessions = data["sessions"]
        elif "agenda" in data:
            raw_sessions = data["agenda"]
        elif isinstance(data, list):
            raw_sessions = data
        else:
            raw_sessions = [data]
        
        for raw_session in raw_sessions:
            try:
                # Generate a unique session ID if not present
                session_id = raw_session.get("id", str(uuid.uuid4()))
                
                # Process speakers - handle both string and dict formats
                raw_speakers = raw_session.get("speakers", [])
                speakers = []
                for speaker in raw_speakers:
                    if isinstance(speaker, dict):
                        name = speaker.get("name", "").strip()
                        if name:
                            speakers.append(name)
                    elif isinstance(speaker, str):
                        # Try to parse as dict if it looks like one
                        if speaker.startswith("{"):
                            try:
                                speaker_dict = ast.literal_eval(speaker)
                                name = speaker_dict.get("name", "").strip()
                                if name:
                                    speakers.append(name)
                            except:
                                # If parsing fails, use the string as is
                                speakers.append(speaker.strip())
                        else:
                            speakers.append(speaker.strip())
                
                # Extract basic session information
                session = {
                    "session_id": session_id,
                    "title": str(raw_session.get("title", "")).strip(),
                    "description": str(raw_session.get("description", "")).strip(),
                    "track": str(raw_session.get("track", "")).strip(),
                    "level": str(raw_session.get("level", "")).strip(),
                    "type": str(raw_session.get("type", "")).strip(),
                    "industry": str(raw_session.get("industry", "")).strip(),
                    "category": str(raw_session.get("category", "")).strip(),
                    "areas_of_interest": raw_session.get("areas_of_interest", []),
                    "speakers": speakers,
                    "schedule": {
                        "day": str(raw_session.get("day", "")).strip(),
                        "room": str(raw_session.get("room", "")).strip(),
                        "start_time": str(raw_session.get("start_time", "")).strip(),
                        "end_time": str(raw_session.get("end_time", "")).strip()
                    }
                }
                
                # Clean up empty values
                for key, value in list(session.items()):
                    if isinstance(value, str) and not value:
                        session[key] = ""
                    elif isinstance(value, list) and not value:
                        session[key] = []
                
                sessions.append(session)
            except Exception as e:
                logger.error(f"Error processing session data: {e}")
                continue
        
        return sessions

    def save_sessions(self, sessions: List[Dict]):
        """Save session data to JSONL files."""
        try:
            # Save all sessions
            all_sessions_file = self.sessions_dir / "sessions_.jsonl"
            with open(all_sessions_file, "w") as f:
                for session in sessions:
                    json.dump(session, f)
                    f.write("\n")
            
            # Save sessions by track
            tracks = {}
            for session in sessions:
                track = session.get("track", "Other")
                if track not in tracks:
                    tracks[track] = []
                tracks[track].append(session)
            
            for track, track_sessions in tracks.items():
                track_file = self.sessions_dir / f"sessions_{track.lower().replace(' ', '_')}.jsonl"
                with open(track_file, "w") as f:
                    for session in track_sessions:
                        json.dump(session, f)
                        f.write("\n")
            
            logger.info(f"Saved {len(sessions)} sessions to {self.sessions_dir}")
        except Exception as e:
            logger.error(f"Error saving sessions: {e}")
            raise

    def fetch_sessions(self) -> List[Dict]:
        """Fetch session data from the Databricks website using Selenium."""
        try:
            if not self.driver:
                self.setup_driver()
            
            logger.info(f"Fetching sessions from {self.base_url}")
            self.driver.get(self.base_url)
            
            # Wait for the page to load and render
            time.sleep(5)  # Initial wait for page load
            
            # Try to find Next.js data structure
            try:
                nextjs_data = self.extract_nextjs_data_selenium()
                if nextjs_data:
                    logger.info("Found Next.js data structure")
                    if "props" in nextjs_data and "pageProps" in nextjs_data["props"]:
                        page_props = nextjs_data["props"]["pageProps"]
                        if "agenda" in page_props:
                            logger.info("Found agenda data in pageProps")
                            return self.extract_session_data(page_props["agenda"])
            except Exception as e:
                logger.error(f"Error extracting Next.js data: {e}")
            
            # Try to extract session data directly from the DOM
            try:
                dom_data = self.extract_session_data_from_dom()
                if dom_data:
                    logger.info("Found session data in DOM")
                    return dom_data
            except Exception as e:
                logger.error(f"Error extracting session data from DOM: {e}")
            
            logger.error("Could not find session data in response")
            return []
            
        except Exception as e:
            logger.error(f"Error fetching sessions: {e}")
            return []
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None

    def extract_nextjs_data_selenium(self) -> Optional[Dict]:
        """Extract Next.js data structure using Selenium."""
        try:
            # Execute JavaScript to get Next.js data
            script = """
            try {
                return window.__NEXT_DATA__;
            } catch (e) {
                return null;
            }
            """
            nextjs_data = self.driver.execute_script(script)
            if nextjs_data:
                logger.debug("Found Next.js data through JavaScript")
                return nextjs_data
            
            # Look for __NEXT_DATA__ script tag
            next_data_elements = self.driver.find_elements(By.CSS_SELECTOR, 'script#__NEXT_DATA__')
            for element in next_data_elements:
                try:
                    data = json.loads(element.get_attribute('innerHTML'))
                    logger.debug("Found Next.js data in script tag")
                    return data
                except json.JSONDecodeError:
                    continue
            
            return None
        except Exception as e:
            logger.error(f"Error extracting Next.js data with Selenium: {e}")
            return None

    def extract_session_data_from_dom(self) -> List[Dict]:
        """Extract session data directly from the DOM structure."""
        try:
            sessions = []
            
            # Look for session cards/containers
            session_elements = self.driver.find_elements(By.CSS_SELECTOR, '[class*="session"], [class*="agenda-item"], [class*="track-item"]')
            for element in session_elements:
                try:
                    session_data = {
                        "session_id": str(uuid.uuid4()),
                        "title": "",
                        "description": "",
                        "track": "",
                        "level": "",
                        "type": "",
                        "industry": "",
                        "category": "",
                        "areas_of_interest": [],
                        "speakers": [],
                        "schedule": {
                            "day": "",
                            "room": "",
                            "start_time": "",
                            "end_time": ""
                        }
                    }
                    
                    # Extract title
                    title_element = element.find_elements(By.CSS_SELECTOR, '[class*="title"], h1, h2, h3, h4')
                    if title_element:
                        session_data["title"] = title_element[0].text.strip()
                    
                    # Extract description
                    desc_element = element.find_elements(By.CSS_SELECTOR, '[class*="description"], [class*="content"], p')
                    if desc_element:
                        session_data["description"] = desc_element[0].text.strip()
                    
                    # Extract track
                    track_element = element.find_elements(By.CSS_SELECTOR, '[class*="track"], [class*="category"]')
                    if track_element:
                        session_data["track"] = track_element[0].text.strip()
                    
                    # Extract level
                    level_element = element.find_elements(By.CSS_SELECTOR, '[class*="level"], [class*="difficulty"]')
                    if level_element:
                        session_data["level"] = level_element[0].text.strip()
                    
                    # Extract type
                    type_element = element.find_elements(By.CSS_SELECTOR, '[class*="type"], [class*="format"]')
                    if type_element:
                        session_data["type"] = type_element[0].text.strip()
                    
                    # Extract speakers
                    speaker_elements = element.find_elements(By.CSS_SELECTOR, '[class*="speaker"]')
                    session_data["speakers"] = [s.text.strip() for s in speaker_elements if s.text.strip()]
                    
                    # Extract schedule
                    time_element = element.find_elements(By.CSS_SELECTOR, '[class*="time"], [class*="schedule"]')
                    if time_element:
                        time_text = time_element[0].text.strip()
                        # Try to parse time text into start and end times
                        time_parts = time_text.split("-")
                        if len(time_parts) == 2:
                            session_data["schedule"]["start_time"] = time_parts[0].strip()
                            session_data["schedule"]["end_time"] = time_parts[1].strip()
                    
                    # Extract room
                    room_element = element.find_elements(By.CSS_SELECTOR, '[class*="room"], [class*="location"]')
                    if room_element:
                        session_data["schedule"]["room"] = room_element[0].text.strip()
                    
                    # Only add sessions that have at least a title
                    if session_data["title"]:
                        sessions.append(session_data)
                except Exception as e:
                    logger.error(f"Error processing session element: {e}")
                    continue
            
            return sessions
        except Exception as e:
            logger.error(f"Error extracting session data from DOM: {e}")
            return []

def main():
    """Main entry point for the scraper."""
    logging.basicConfig(level=logging.INFO)
    scraper = DaisScraper()
    sessions = scraper.fetch_sessions()
    if sessions:
        scraper.save_sessions(sessions)
        print(f"Successfully saved {len(sessions)} sessions")
    else:
        print("No sessions were found or saved")

if __name__ == "__main__":
    main() 