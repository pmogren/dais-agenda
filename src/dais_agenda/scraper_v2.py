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

class DaisScraperV2:
    def __init__(self, data_dir: str = "data", preview_mode: bool = False, preview_count: int = 3):
        self.data_dir = Path(data_dir)
        self.sessions_dir = self.data_dir / "sessions"
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = "https://www.databricks.com/dataaisummit/agenda"
        self.driver = None
        self.preview_mode = preview_mode
        self.preview_count = preview_count

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

    def extract_session_data(self, data: Dict, session_url: str = "") -> List[Dict]:
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
                # Use URL path segment as session ID if available, otherwise generate UUID
                session_id = session_url.split("/")[-1] if session_url else str(uuid.uuid4())
                
                # Extract title - handle both string and object formats
                title = raw_session.get("title", "")
                if isinstance(title, dict):
                    title = title.get("text", "") or title.get("value", "")
                title = str(title).strip()
                
                # Extract description - handle both string and HTML formats
                description = raw_session.get("description", "")
                if isinstance(description, dict):
                    description = description.get("html", "") or description.get("text", "")
                description = str(description).strip()
                
                # Extract track information - check multiple possible fields
                track = (
                    raw_session.get("track", "") or 
                    raw_session.get("trackName", "") or 
                    raw_session.get("track_name", "") or
                    raw_session.get("category", "")  # Some sessions use category as track
                )
                if isinstance(track, dict):
                    track = track.get("name", "") or track.get("value", "")
                track = str(track).strip()
                
                # Extract session type - check multiple possible fields
                session_type = (
                    raw_session.get("type", "") or 
                    raw_session.get("sessionType", "") or 
                    raw_session.get("session_type", "") or
                    raw_session.get("format", "")
                )
                if isinstance(session_type, dict):
                    session_type = session_type.get("name", "") or session_type.get("value", "")
                session_type = str(session_type).strip()
                
                # Extract level - check multiple possible fields
                level = (
                    raw_session.get("level", "") or 
                    raw_session.get("experienceLevel", "") or 
                    raw_session.get("experience_level", "") or
                    raw_session.get("difficulty", "")
                )
                if isinstance(level, dict):
                    level = level.get("name", "") or level.get("value", "")
                level = str(level).strip()
                
                # Process speakers - handle both string and dict formats
                raw_speakers = raw_session.get("speakers", [])
                if isinstance(raw_speakers, str):
                    raw_speakers = [raw_speakers]
                elif isinstance(raw_speakers, dict):
                    raw_speakers = [raw_speakers]
                
                speakers = []
                for speaker in raw_speakers:
                    if isinstance(speaker, dict):
                        name = (
                            speaker.get("name", "") or 
                            speaker.get("speakerName", "") or
                            speaker.get("speaker_name", "") or
                            speaker.get("displayName", "")
                        )
                        if name:
                            speakers.append(str(name).strip())
                    elif isinstance(speaker, str):
                        # Try to parse as dict if it looks like one
                        if speaker.startswith("{"):
                            try:
                                speaker_dict = ast.literal_eval(speaker)
                                name = (
                                    speaker_dict.get("name", "") or 
                                    speaker_dict.get("speakerName", "") or
                                    speaker_dict.get("speaker_name", "") or
                                    speaker_dict.get("displayName", "")
                                )
                                if name:
                                    speakers.append(str(name).strip())
                            except:
                                # If parsing fails, use the string as is
                                speakers.append(speaker.strip())
                        else:
                            speakers.append(speaker.strip())
                
                # Extract schedule information
                schedule = raw_session.get("schedule", {})
                if isinstance(schedule, dict):
                    schedule_data = schedule
                else:
                    schedule_data = {
                        "day": raw_session.get("day", "") or raw_session.get("date", ""),
                        "room": raw_session.get("room", "") or raw_session.get("location", "") or raw_session.get("venue", ""),
                        "start_time": raw_session.get("startTime", "") or raw_session.get("start_time", "") or raw_session.get("start", ""),
                        "end_time": raw_session.get("endTime", "") or raw_session.get("end_time", "") or raw_session.get("end", "")
                    }
                
                # Extract areas of interest
                areas = raw_session.get("areas_of_interest", []) or raw_session.get("areasOfInterest", []) or raw_session.get("topics", [])
                if isinstance(areas, str):
                    areas = [areas]
                elif isinstance(areas, dict):
                    areas = [areas.get("name", "") or areas.get("value", "")]
                areas = [str(area).strip() for area in areas if area]
                
                # Extract industry information
                industry = raw_session.get("industry", "") or raw_session.get("vertical", "")
                if isinstance(industry, dict):
                    industry = industry.get("name", "") or industry.get("value", "")
                industry = str(industry).strip()
                
                # Extract category information (if different from track)
                category = raw_session.get("category", "") or raw_session.get("sessionCategory", "")
                if isinstance(category, dict):
                    category = category.get("name", "") or category.get("value", "")
                category = str(category).strip()
                if category == track:  # If category was used as track, clear it
                    category = ""
                
                # Build the final session object
                session = {
                    "session_id": session_id,
                    "title": title,
                    "description": description,
                    "track": track,
                    "level": level,
                    "type": session_type,
                    "industry": industry,
                    "category": category,
                    "areas_of_interest": areas,
                    "speakers": speakers,
                    "schedule": {
                        "day": str(schedule_data.get("day", "")).strip(),
                        "room": str(schedule_data.get("room", "")).strip(),
                        "start_time": str(schedule_data.get("start_time", "")).strip(),
                        "end_time": str(schedule_data.get("end_time", "")).strip()
                    }
                }
                
                # Clean up empty values
                for key, value in list(session.items()):
                    if isinstance(value, str) and not value:
                        session[key] = ""
                    elif isinstance(value, list) and not value:
                        session[key] = []
                    elif isinstance(value, dict):
                        for subkey, subvalue in list(value.items()):
                            if isinstance(subvalue, str) and not subvalue:
                                value[subkey] = ""
                
                # Only add sessions that have at least a title
                if session["title"]:
                    sessions.append(session)
            except Exception as e:
                logger.error(f"Error processing session data: {e}")
                continue
        
        return sessions

    def save_sessions(self, sessions: List[Dict]):
        """Save session data to JSONL files."""
        try:
            # Sort sessions by session_id
            sessions = sorted(sessions, key=lambda x: x["session_id"])
            
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
                # Sort track sessions by session_id
                track_sessions = sorted(track_sessions, key=lambda x: x["session_id"])
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
            
            # Initialize sessions list
            sessions = []
            
            # Store session URLs to visit
            session_urls = set()
            
            # Find session links by looking for links that contain "/session/" in their href
            session_links = self.driver.find_elements(
                By.CSS_SELECTOR,
                'a[href*="/session/"]'
            )
            
            # Filter out "SEE DETAILS" links and collect URLs
            for link in session_links:
                if "SEE DETAILS" not in link.text:
                    href = link.get_attribute("href")
                    if href:
                        session_urls.add(href)
            
            logger.info(f"Found {len(session_urls)} unique session URLs")
            
            # If in preview mode, limit the number of URLs to process
            if self.preview_mode:
                session_urls = list(session_urls)[:self.preview_count]
                logger.info(f"Preview mode: Processing {len(session_urls)} sessions")
            
            # Process each session URL
            for i, url in enumerate(session_urls):
                try:
                    logger.info(f"Processing session {i+1} of {len(session_urls)}: {url}")
                    
                    # Navigate directly to the session URL
                    self.driver.get(url)
                    time.sleep(2)  # Wait for page to load
                    
                    # Try to find Next.js data structure first
                    nextjs_data = self.extract_nextjs_data_selenium()
                    if nextjs_data:
                        logger.info("Found Next.js data structure")
                        if "props" in nextjs_data and "pageProps" in nextjs_data["props"]:
                            page_props = nextjs_data["props"]["pageProps"]
                            if "agenda" in page_props:
                                logger.info("Found agenda data in pageProps")
                                new_sessions = self.extract_session_data(page_props["agenda"], url)
                                if new_sessions:
                                    sessions.extend(new_sessions)
                                    continue
                    
                    # If no sessions found in Next.js data, try DOM extraction
                    dom_data = self.extract_session_data_from_dom(url)
                    if dom_data:
                        logger.info("Found session data in DOM")
                        sessions.extend(dom_data)
                    
                except Exception as e:
                    logger.error(f"Error processing session URL: {e}")
                    continue
                    
            if not sessions:
                logger.error("Could not find session data in response")
                return []
            
            # Remove duplicates based on title
            unique_sessions = {}
            for session in sessions:
                if session["title"] not in unique_sessions:
                    unique_sessions[session["title"]] = session
            
            return list(unique_sessions.values())
            
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
            
            # Try to find any script tag containing session data
            script_elements = self.driver.find_elements(By.TAG_NAME, 'script')
            for element in script_elements:
                try:
                    script_content = element.get_attribute('innerHTML')
                    if 'session' in script_content.lower() or 'agenda' in script_content.lower():
                        # Try to extract JSON data from the script
                        json_match = re.search(r'({.*})', script_content)
                        if json_match:
                            data = json.loads(json_match.group(1))
                            if 'session' in data or 'agenda' in data:
                                logger.debug("Found session data in script tag")
                                return data
                except Exception as e:
                    logger.debug(f"Error parsing script content: {e}")
                    continue
            
            return None
        except Exception as e:
            logger.error(f"Error extracting Next.js data with Selenium: {e}")
            return None

    def extract_session_data_from_dom(self, session_url: str = "") -> List[Dict]:
        """Extract session data directly from the DOM structure."""
        try:
            sessions = []
            
            # Wait for any content to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except Exception as e:
                logger.warning(f"Timeout waiting for page to load: {e}")
                return []
            
            # Look for session details container with more flexible selectors
            session_elements = self.driver.find_elements(
                By.CSS_SELECTOR, 
                'div[class*="session"], div[class*="agenda"], div[class*="event"], article, main'
            )
            
            if not session_elements:
                logger.warning("No session elements found in DOM")
                return []
            
            for element in session_elements:
                try:
                    # Use URL path segment as session ID if available, otherwise generate UUID
                    session_id = session_url.split("/")[-1] if session_url else str(uuid.uuid4())
                    
                    session_data = {
                        "session_id": session_id,
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
                    
                    # Extract title - check multiple possible selectors
                    title_selectors = [
                        'h1', 'h2', 'h3', 'h4', '[class*="title"]', '[class*="heading"]',
                        '[data-type="title"]', '[data-test="session-title"]', 'title'
                    ]
                    for selector in title_selectors:
                        try:
                            title_element = element.find_elements(By.CSS_SELECTOR, selector)
                            if title_element and title_element[0].text.strip():
                                session_data["title"] = title_element[0].text.strip()
                                break
                        except Exception as e:
                            logger.debug(f"Error extracting title with selector {selector}: {e}")
                    
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
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape Databricks Data + AI Summit session data")
    parser.add_argument("--preview", action="store_true", help="Run in preview mode (process only 3 sessions)")
    parser.add_argument("--preview-count", type=int, default=3, help="Number of sessions to process in preview mode")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    scraper = DaisScraperV2(preview_mode=args.preview, preview_count=args.preview_count)
    sessions = scraper.fetch_sessions()
    if sessions:
        scraper.save_sessions(sessions)
        print(f"Successfully saved {len(sessions)} sessions")
    else:
        print("No sessions were found or saved")

if __name__ == "__main__":
    main() 