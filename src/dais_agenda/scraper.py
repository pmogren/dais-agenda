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
                    logger.debug(f"Next.js data keys: {list(nextjs_data.keys())}")
                    if "props" in nextjs_data:
                        logger.debug(f"Props keys: {list(nextjs_data['props'].keys())}")
                    if "pageProps" in nextjs_data.get("props", {}):
                        logger.debug(f"PageProps keys: {list(nextjs_data['props']['pageProps'].keys())}")
                    
                    if "props" in nextjs_data and "pageProps" in nextjs_data["props"]:
                        page_props = nextjs_data["props"]["pageProps"]
                        if "agenda" in page_props:
                            logger.info("Found agenda data in pageProps")
                            return self.extract_session_data(page_props["agenda"])
            except Exception as e:
                logger.error(f"Error extracting Next.js data: {e}")
            
            # Try to find session data in other locations
            try:
                session_data = self.find_session_data_selenium()
                if session_data:
                    logger.info("Found session data in alternative location")
                    return self.extract_session_data(session_data)
            except Exception as e:
                logger.error(f"Error finding session data: {e}")
            
            # Try to find session data in script tags
            try:
                script_data = self.find_script_data_selenium()
                if script_data:
                    logger.info("Found session data in script tags")
                    return self.extract_session_data(script_data)
            except Exception as e:
                logger.error(f"Error finding script data: {e}")
            
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

    def find_session_data_selenium(self) -> Optional[Dict]:
        """Look for session data in various locations using Selenium."""
        try:
            # Look for data attributes
            elements_with_data = self.driver.find_elements(By.CSS_SELECTOR, '[data-session], [data-agenda], [data-track], [data-sessions]')
            sessions = []
            for element in elements_with_data:
                for attr in ['data-session', 'data-agenda', 'data-track', 'data-sessions']:
                    try:
                        data = element.get_attribute(attr)
                        if data:
                            session_data = json.loads(data)
                            if isinstance(session_data, dict):
                                sessions.append(session_data)
                            elif isinstance(session_data, list):
                                sessions.extend(session_data)
                    except (json.JSONDecodeError, TypeError):
                        continue
            
            if sessions:
                logger.debug(f"Found {len(sessions)} sessions in data attributes")
                return {"sessions": sessions}
            
            return None
        except Exception as e:
            logger.error(f"Error finding session data with Selenium: {e}")
            return None

    def find_script_data_selenium(self) -> Optional[Dict]:
        """Look for session data in script tags using Selenium."""
        try:
            # Get all script tags
            script_elements = self.driver.find_elements(By.TAG_NAME, 'script')
            for script in script_elements:
                try:
                    content = script.get_attribute('innerHTML')
                    if not content:
                        continue
                    
                    # Look for variable assignments
                    for var_name in ['agenda', 'sessions', 'pageData']:
                        patterns = [
                            f'var {var_name}\\s*=\\s*({{.*?}});',
                            f'window\\.{var_name}\\s*=\\s*({{.*?}});',
                            f'const {var_name}\\s*=\\s*({{.*?}});',
                            f'let {var_name}\\s*=\\s*({{.*?}});'
                        ]
                        for pattern in patterns:
                            match = re.search(pattern, content, re.DOTALL)
                            if match:
                                try:
                                    data = json.loads(match.group(1))
                                    if isinstance(data, dict):
                                        if "sessions" in data or "agenda" in data or "tracks" in data:
                                            logger.debug("Found session data in script tag")
                                            return data
                                except json.JSONDecodeError:
                                    continue
                except Exception:
                    continue
            
            return None
        except Exception as e:
            logger.error(f"Error finding script data with Selenium: {e}")
            return None

    def extract_session_data_from_dom(self) -> List[Dict]:
        """Extract session data directly from the DOM structure."""
        try:
            sessions = []
            
            # Look for session cards/containers
            session_elements = self.driver.find_elements(By.CSS_SELECTOR, '[class*="session"], [class*="agenda-item"], [class*="track-item"]')
            for element in session_elements:
                try:
                    session_data = {}
                    
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
                    
                    # Extract speakers
                    speaker_elements = element.find_elements(By.CSS_SELECTOR, '[class*="speaker"]')
                    session_data["speakers"] = [s.text.strip() for s in speaker_elements if s.text.strip()]
                    
                    # Extract schedule
                    time_element = element.find_elements(By.CSS_SELECTOR, '[class*="time"], [class*="schedule"]')
                    if time_element:
                        time_text = time_element[0].text.strip()
                        session_data["schedule"] = {"time": time_text}
                    
                    # Only add sessions that have at least a title
                    if session_data.get("title"):
                        sessions.append(session_data)
                except Exception as e:
                    logger.error(f"Error extracting session data from element: {e}")
                    continue
            
            if sessions:
                logger.info(f"Extracted {len(sessions)} sessions from DOM")
                return sessions
            
            return []
        except Exception as e:
            logger.error(f"Error extracting session data from DOM: {e}")
            return []

    def extract_session_data(self, data: Dict) -> List[Dict]:
        """Extract session data from the data structure."""
        sessions = []
        try:
            # Handle different possible data structures
            if isinstance(data, list):
                # Direct list of sessions
                session_list = data
            elif isinstance(data, dict):
                if "sessions" in data:
                    # Sessions in top-level sessions key
                    session_list = data["sessions"]
                elif "agenda" in data:
                    # Sessions in agenda key
                    session_list = data["agenda"]
                elif "tracks" in data:
                    # Sessions organized by tracks
                    session_list = []
                    for track in data["tracks"]:
                        track_name = track.get("name", "")
                        for session in track.get("sessions", []):
                            session["track"] = track_name
                            session_list.append(session)
                else:
                    # Try to find sessions in nested structures
                    session_list = []
                    for key, value in data.items():
                        if isinstance(value, (list, dict)):
                            try:
                                nested_sessions = self.extract_session_data(value)
                                if nested_sessions:
                                    session_list.extend(nested_sessions)
                            except Exception:
                                continue
                    if not session_list:
                        # If no sessions found in nested structures, check if this is a single session
                        if any(key in data for key in ["title", "description", "startTime", "endTime"]):
                            session_list = [data]
                        else:
                            logger.error("Unknown data structure")
                            return []
            else:
                logger.error("Invalid data type")
                return []

            # Process each session
            for session in session_list:
                try:
                    session_data = {
                        "session_id": session.get("id", ""),
                        "title": session.get("title", ""),
                        "description": session.get("description", ""),
                        "track": session.get("track", ""),
                        "level": session.get("level", ""),
                        "type": session.get("type", ""),
                        "industry": session.get("industry", ""),
                        "category": session.get("category", ""),
                        "areas_of_interest": session.get("areasOfInterest", []),
                        "speakers": [s.get("name", "") for s in session.get("speakers", [])],
                        "schedule": {
                            "day": session.get("day", ""),
                            "room": session.get("room", ""),
                            "start_time": session.get("startTime", ""),
                            "end_time": session.get("endTime", "")
                        }
                    }
                    sessions.append(session_data)
                except Exception as e:
                    logger.error(f"Error processing session: {e}")
                    continue

            logger.info(f"Extracted {len(sessions)} sessions")
        except Exception as e:
            logger.error(f"Error extracting session data: {e}")
        
        return sessions

    def save_sessions(self, sessions: List[Dict]):
        """Save sessions to JSONL files grouped by track."""
        # Group sessions by track
        sessions_by_track = {}
        for session in sessions:
            track = session.get("track", "unknown")
            if track not in sessions_by_track:
                sessions_by_track[track] = []
            sessions_by_track[track].append(session)
        
        # Save each track to a separate file
        for track, track_sessions in sessions_by_track.items():
            # Create a safe filename from the track name
            safe_track_name = track.lower().replace(" ", "_").replace("&", "and")
            filename = f"sessions_{safe_track_name}.jsonl"
            file_path = self.sessions_dir / filename
            
            try:
                with open(file_path, "w") as f:
                    for session in track_sessions:
                        f.write(json.dumps(session) + "\n")
                logger.info(f"Saved {len(track_sessions)} sessions to {filename}")
            except Exception as e:
                logger.error(f"Error saving sessions to {filename}: {e}")

def main():
    logging.basicConfig(level=logging.DEBUG)  # Changed to DEBUG for more detailed logging
    scraper = DaisScraper()
    sessions = scraper.fetch_sessions()
    if sessions:
        scraper.save_sessions(sessions)
        logger.info(f"Successfully saved {len(sessions)} sessions")
    else:
        logger.error("No sessions were found or saved")

if __name__ == "__main__":
    main() 