import json
import logging
import requests
from pathlib import Path
from typing import Dict, List, Optional, Any
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
            logger.debug("Successfully set up Chrome WebDriver")
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
                
                # Clean up session type
                session_type = self.clean_session_type(session_type)
                
                # If session type is still empty or contains Drupal content type, try to infer from title or description
                if not session_type or "menu_link_content" in session_type:
                    # Look for session type keywords in title and description
                    text_to_search = f"{title} {description}".lower()
                    type_keywords = {
                        'Breakout': ['breakout session', 'breakout'],
                        'Deep Dive': ['deep dive', 'deep-dive', 'deepdive'],
                        'Evening Event': ['evening event', 'evening session', 'evening'],
                        'Keynote': ['keynote session', 'keynote'],
                        'Lightning Talk': ['lightning talk', 'lightning'],
                        'Meetup': ['meetup', 'meet-up', 'meet up'],
                        'Paid Training': ['training session', 'paid training', 'workshop', 'tutorial'],
                        'Special Interest': ['special interest', 'special-interest']
                    }
                    
                    for standard_type, keywords in type_keywords.items():
                        if any(keyword in text_to_search for keyword in keywords):
                            session_type = standard_type
                            break
                    
                    # If still not found, default to "Breakout" as it's the most common type
                    if not session_type or "menu_link_content" in session_type:
                        session_type = "Breakout"
                
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
                
                # Build the final session object
                session = {
                    "session_id": session_id,
                    "title": title,
                    "track": track,
                    "level": level,
                    "type": session_type,
                    "industry": industry,
                    "areas_of_interest": areas,
                    "speakers": speakers,
                    "schedule": {
                        "day": str(schedule_data.get("day", "")).strip(),
                        "room": str(schedule_data.get("room", "")).strip(),
                        "start_time": str(schedule_data.get("start_time", "")).strip(),
                        "end_time": str(schedule_data.get("end_time", "")).strip()
                    },
                    "description": description
                }
                
                # Clean up empty values
                for key, value in list(session.items()):
                    if isinstance(value, str) and not value:
                        del session[key]
                    elif isinstance(value, list) and not value:
                        del session[key]
                    elif isinstance(value, dict) and not any(v for v in value.values()):
                        del session[key]
                
                sessions.append(session)
            except Exception as e:
                logger.error(f"Error processing session: {e}")
                continue
        
        return sessions

    def clean_session_type(self, session_type: str) -> str:
        """Clean and normalize session type values."""
        # Remove any Drupal-specific content types
        session_type = re.sub(r'menu_link_content--.*$', '', session_type).strip()
        
        # If empty after cleaning, return empty string
        if not session_type:
            return ""
            
        # Define standard session types and their variations (including uppercase)
        standard_types = {
            'Breakout': ['breakout', 'session', 'regular session', 'BREAKOUT', 'SESSION', 'REGULAR SESSION'],
            'Deep Dive': ['deep dive', 'deep-dive', 'deepdive', 'DEEP DIVE', 'DEEP-DIVE', 'DEEPDIVE'],
            'Evening Event': ['evening', 'evening event', 'evening session', 'EVENING', 'EVENING EVENT', 'EVENING SESSION'],
            'Keynote': ['keynote', 'plenary', 'KEYNOTE', 'PLENARY'],
            'Lightning Talk': ['lightning', 'lightning talk', 'quick talk', 'LIGHTNING', 'LIGHTNING TALK', 'QUICK TALK'],
            'Meetup': ['meetup', 'meet-up', 'meet up', 'MEETUP', 'MEET-UP', 'MEET UP'],
            'Paid Training': ['paid training', 'training', 'workshop', 'tutorial', 'PAID TRAINING', 'TRAINING', 'WORKSHOP', 'TUTORIAL'],
            'Special Interest': ['special interest', 'special-interest', 'special', 'SPECIAL INTEREST', 'SPECIAL-INTEREST', 'SPECIAL']
        }
        
        # First check if the session type exactly matches a standard type (case-insensitive)
        session_type_lower = session_type.lower()
        for standard_type in standard_types:
            if session_type_lower == standard_type.lower():
                return standard_type
            
        # Try to match the session type to a known type
        for standard_type, variations in standard_types.items():
            if session_type_lower in [v.lower() for v in variations] or any(var.lower() in session_type_lower for var in variations):
                return standard_type
        
        # If no match found, return the original type (if not empty)
        return session_type if session_type else ""

    def save_sessions(self, sessions: List[Dict]):
        """Save session data to JSONL files."""
        try:
            # Remove duplicates based on session_id
            unique_sessions = {}
            for session in sessions:
                session_id = session["session_id"]
                if session_id not in unique_sessions:
                    # Clean up session type if it's still a Drupal content type
                    if "type" in session and "menu_link_content" in session["type"]:
                        # Try to infer session type from title and description
                        text_to_search = f"{session.get('title', '')} {session.get('description', '')}".lower()
                        type_keywords = {
                            'Breakout': ['breakout session', 'breakout'],
                            'Deep Dive': ['deep dive', 'deep-dive', 'deepdive'],
                            'Evening Event': ['evening event', 'evening session', 'evening'],
                            'Keynote': ['keynote session', 'keynote'],
                            'Lightning Talk': ['lightning talk', 'lightning'],
                            'Meetup': ['meetup', 'meet-up', 'meet up'],
                            'Paid Training': ['training session', 'paid training', 'workshop', 'tutorial'],
                            'Special Interest': ['special interest', 'special-interest']
                        }
                        
                        session_type = None
                        for standard_type, keywords in type_keywords.items():
                            if any(keyword in text_to_search for keyword in keywords):
                                session_type = standard_type
                                break
                        
                        # If still not found, default to "Breakout" as it's the most common type
                        session["type"] = session_type or "Breakout"
                    
                    # Sort arrays in the session data
                    if "areas_of_interest" in session:
                        session["areas_of_interest"] = sorted(session["areas_of_interest"])
                    if "speakers" in session:
                        session["speakers"] = sorted(session["speakers"])
                    if "technologies" in session:
                        session["technologies"] = sorted(session["technologies"])
                    
                    unique_sessions[session_id] = session
            
            # Convert back to list and sort by session_id
            sessions = sorted(unique_sessions.values(), key=lambda x: x["session_id"])
            
            # Define the order of fields
            field_order = [
                "session_id",
                "title",
                "track",
                "level",
                "type",
                "industry",
                "technologies",
                "duration",
                "experience",
                "areas_of_interest",
                "speakers",
                "schedule",
                "description"
            ]
            
            def ordered_session(session):
                """Create an ordered dictionary with fields in the specified order."""
                ordered = {}
                for field in field_order:
                    if field in session:
                        ordered[field] = session[field]
                return ordered
            
            # Clear existing files
            for file in self.sessions_dir.glob("sessions_*.jsonl"):
                file.unlink()
            
            # Save sessions by track
            tracks = {}
            for session in sessions:
                track = session.get("track", "Other")
                if track not in tracks:
                    tracks[track] = []
                tracks[track].append(session)
            
            # Save all sessions to a single file
            all_sessions_file = self.sessions_dir / "sessions.jsonl"
            with open(all_sessions_file, "w") as f:
                for session in sessions:
                    ordered = ordered_session(session)
                    json.dump(ordered, f)
                    f.write("\n")
            
            # Save sessions by track
            for track, track_sessions in tracks.items():
                # Sort track sessions by session_id
                track_sessions = sorted(track_sessions, key=lambda x: x["session_id"])
                track_file = self.sessions_dir / f"sessions_by_track_{track.lower().replace(' ', '_')}.jsonl"
                with open(track_file, "w") as f:
                    for session in track_sessions:
                        ordered = ordered_session(session)
                        json.dump(ordered, f)
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
            
            # Keep track of current page
            current_page = 1
            
            # Process pages until no more sessions are found
            while True:
                # Construct the URL with the page parameter
                page_url = f"{self.base_url}?page={current_page}"
                logger.info(f"Processing page {current_page}")
                
                # Navigate to the page
                self.driver.get(page_url)
                time.sleep(2)  # Wait for page to load
                
                # Find session links by looking for links that contain "/session/" in their href
                session_links = self.driver.find_elements(
                    By.CSS_SELECTOR,
                    'a[href*="/session/"]'
                )
                
                # If no session links found, we've reached the end
                if not session_links:
                    logger.debug(f"No more sessions found on page {current_page}")
                    break
                
                # Filter out "SEE DETAILS" links and collect URLs
                page_urls = set()
                for link in session_links:
                    if "SEE DETAILS" not in link.text:
                        href = link.get_attribute("href")
                        if href:
                            page_urls.add(href)
                
                # If no new URLs found on this page, we've reached the end
                if not page_urls:
                    logger.debug(f"No new session URLs found on page {current_page}")
                    break
                
                # Add new URLs to our collection
                session_urls.update(page_urls)
                logger.debug(f"Found {len(page_urls)} new session URLs on page {current_page}")
                
                current_page += 1
            
            logger.info(f"Found {len(session_urls)} unique session URLs across {current_page - 1} pages")
            
            # Convert to sorted list for consistent ordering
            session_urls = sorted(list(session_urls))
            
            # If in preview mode, limit the number of URLs to process
            if self.preview_mode:
                session_urls = session_urls[:self.preview_count]
                logger.info(f"Preview mode: Processing {len(session_urls)} sessions")
            
            # Process each session URL
            for i, url in enumerate(session_urls):
                try:
                    logger.debug(f"Processing session {i+1} of {len(session_urls)}: {url}")
                    
                    # Navigate directly to the session URL
                    self.driver.get(url)
                    time.sleep(2)  # Wait for page to load
                    
                    # Try to find Next.js data structure first
                    nextjs_data = self.extract_nextjs_data()
                    if nextjs_data:
                        logger.debug("Found Next.js data structure")
                        if "props" in nextjs_data and "pageProps" in nextjs_data["props"]:
                            page_props = nextjs_data["props"]["pageProps"]
                            if "agenda" in page_props:
                                logger.debug("Found agenda data in pageProps")
                                new_sessions = self.extract_session_data(page_props["agenda"], url)
                                if new_sessions:
                                    sessions.extend(new_sessions)
                                    continue
                    
                    # If no sessions found in Next.js data, try DOM extraction
                    dom_data = self.extract_session_data_from_dom(url)
                    if dom_data:
                        logger.debug("Found session data in DOM")
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

    def extract_nextjs_data(self) -> Optional[Dict]:
        """Extract session data from Next.js data structure."""
        try:
            # First try to get data from window.__NEXT_DATA__
            script = """
            try {
                const data = window.__NEXT_DATA__;
                if (data) {
                    return JSON.stringify(data);
                }
                
                // Look for session data in other global variables
                const globals = Object.keys(window).filter(key => 
                    key.toLowerCase().includes('session') || 
                    key.toLowerCase().includes('event') || 
                    key.toLowerCase().includes('agenda')
                );
                
                for (const key of globals) {
                    const value = window[key];
                    if (value && typeof value === 'object') {
                        return JSON.stringify(value);
                    }
                }
                
                return null;
            } catch (e) {
                return null;
            }
            """
            result = self.driver.execute_script(script)
            if result:
                try:
                    data = json.loads(result)
                    logger.debug("Found data in JavaScript variables")
                    
                    # Try to find session data in the structure
                    if isinstance(data, dict):
                        # Look for metadata in common locations
                        metadata = {}
                        schedule = {}
                        areas = []
                        
                        def search_dict(d: Dict, prefix: str = "") -> None:
                            """Recursively search dictionary for metadata."""
                            if not isinstance(d, dict):
                                return
                            
                            # Look for metadata fields
                            for field in ["track", "level", "type", "industry", "category"]:
                                if field in d and isinstance(d[field], str):
                                    metadata[field] = d[field]
                                elif f"{field}Name" in d and isinstance(d[f"{field}Name"], str):
                                    metadata[field] = d[f"{field}Name"]
                                elif f"{field}Type" in d and isinstance(d[f"{field}Type"], str):
                                    metadata[field] = d[f"{field}Type"]
                            
                            # Look for schedule information
                            if "schedule" in d and isinstance(d["schedule"], dict):
                                schedule.update(d["schedule"])
                            elif "datetime" in d and isinstance(d["datetime"], str):
                                schedule["datetime"] = d["datetime"]
                            elif all(key in d for key in ["startTime", "endTime"]):
                                schedule["start_time"] = d["startTime"]
                                schedule["end_time"] = d["endTime"]
                            elif "time" in d and isinstance(d["time"], str):
                                schedule["time"] = d["time"]
                            
                            # Look for areas of interest
                            if "areas" in d and isinstance(d["areas"], (list, str)):
                                if isinstance(d["areas"], list):
                                    areas.extend(d["areas"])
                                else:
                                    areas.extend(d["areas"].split(","))
                            elif "topics" in d and isinstance(d["topics"], (list, str)):
                                if isinstance(d["topics"], list):
                                    areas.extend(d["topics"])
                                else:
                                    areas.extend(d["topics"].split(","))
                            elif "tags" in d and isinstance(d["tags"], (list, str)):
                                if isinstance(d["tags"], list):
                                    areas.extend(d["tags"])
                                else:
                                    areas.extend(d["tags"].split(","))
                            
                            # Recursively search nested dictionaries
                            for key, value in d.items():
                                if isinstance(value, dict):
                                    search_dict(value, f"{prefix}.{key}" if prefix else key)
                                elif isinstance(value, list):
                                    for item in value:
                                        if isinstance(item, dict):
                                            search_dict(item, f"{prefix}.{key}[]" if prefix else key)
                        
                        # Start recursive search
                        search_dict(data)
                        
                        # Clean up and return found data
                        if metadata or schedule or areas:
                            result = {}
                            if metadata:
                                result["metadata"] = metadata
                            if schedule:
                                # Parse schedule information if needed
                                if "datetime" in schedule:
                                    # Try to parse datetime string
                                    try:
                                        dt = datetime.strptime(schedule["datetime"], "%Y-%m-%dT%H:%M:%S")
                                        schedule["day"] = dt.strftime("%A")
                                        schedule["start_time"] = dt.strftime("%I:%M %p")
                                    except ValueError:
                                        pass
                                elif "time" in schedule:
                                    # Try to parse time string
                                    time_patterns = [
                                        r'(\d{1,2}:\d{2}\s*[AP]M)\s*[-–]\s*(\d{1,2}:\d{2}\s*[AP]M)',
                                        r'(\d{1,2}(?::\d{2})?\s*[AP]M)\s*[-–]\s*(\d{1,2}(?::\d{2})?\s*[AP]M)'
                                    ]
                                    for pattern in time_patterns:
                                        match = re.search(pattern, schedule["time"])
                                        if match:
                                            schedule["start_time"] = match.group(1)
                                            schedule["end_time"] = match.group(2)
                                            break
                                result["schedule"] = schedule
                            if areas:
                                result["areas_of_interest"] = list(set(area.strip() for area in areas if area.strip()))
                            return result
                except json.JSONDecodeError:
                    pass
            
            # If no data found in JavaScript variables, try looking in script tags
            script_elements = self.driver.find_elements(By.TAG_NAME, "script")
            for script in script_elements:
                try:
                    script_text = script.get_attribute("textContent")
                    if script_text and "__NEXT_DATA__" in script_text:
                        # Parse JSON data
                        json_text = script_text.strip()
                        data = json.loads(json_text)
                        
                        # Try to find session data in the Next.js structure
                        if "props" in data:
                            props = data["props"]
                            # Look in common locations for session data
                            locations = [
                                props.get("pageProps", {}),
                                props.get("initialState", {}),
                                props.get("initialProps", {}),
                                props.get("session", {}),
                                props.get("data", {})
                            ]
                            
                            # Search for session data in each location
                            for location in locations:
                                if isinstance(location, dict):
                                    # Look for session metadata
                                    metadata = {}
                                    for field in ["track", "level", "type", "industry", "category"]:
                                        # Try different paths where metadata might be stored
                                        paths = [
                                            field,
                                            f"session_{field}",
                                            f"event_{field}",
                                            f"metadata.{field}",
                                            f"session.{field}",
                                            f"event.{field}"
                                        ]
                                        for path in paths:
                                            value = self._get_nested_value(location, path)
                                            if value and isinstance(value, str):
                                                metadata[field] = value
                                                break
                                    
                                    # Look for schedule information
                                    schedule = {}
                                    schedule_paths = [
                                        "schedule",
                                        "session_schedule",
                                        "event_schedule",
                                        "datetime",
                                        "time",
                                        "session.schedule",
                                        "event.schedule"
                                    ]
                                    for path in schedule_paths:
                                        schedule_data = self._get_nested_value(location, path)
                                        if schedule_data:
                                            if isinstance(schedule_data, dict):
                                                schedule.update(schedule_data)
                                            elif isinstance(schedule_data, str):
                                                # Try to parse schedule string
                                                # Look for day
                                                day_patterns = [
                                                    r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)',
                                                    r'\d{1,2}/\d{1,2}(?:/\d{2,4})?',
                                                    r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?(?:,? \d{4})?'
                                                ]
                                                for pattern in day_patterns:
                                                    match = re.search(pattern, schedule_data, re.IGNORECASE)
                                                    if match:
                                                        schedule["day"] = match.group(0)
                                                        break
                                                
                                                # Look for time
                                                time_patterns = [
                                                    r'(\d{1,2}:\d{2}\s*[AP]M)\s*[-–]\s*(\d{1,2}:\d{2}\s*[AP]M)',
                                                    r'(\d{1,2}(?::\d{2})?\s*[AP]M)\s*[-–]\s*(\d{1,2}(?::\d{2})?\s*[AP]M)'
                                                ]
                                                for pattern in time_patterns:
                                                    match = re.search(pattern, schedule_data)
                                                    if match:
                                                        schedule["start_time"] = match.group(1)
                                                        schedule["end_time"] = match.group(2)
                                                        break
                                    
                                    # Look for areas of interest
                                    areas = []
                                    areas_paths = [
                                        "areas_of_interest",
                                        "topics",
                                        "tags",
                                        "session.areas_of_interest",
                                        "event.areas_of_interest",
                                        "metadata.areas_of_interest"
                                    ]
                                    for path in areas_paths:
                                        areas_data = self._get_nested_value(location, path)
                                        if areas_data:
                                            if isinstance(areas_data, list):
                                                areas.extend(areas_data)
                                            elif isinstance(areas_data, str):
                                                areas.extend([area.strip() for area in areas_data.split(",")])
                                    
                                    # If we found any metadata, return it
                                    if metadata or schedule or areas:
                                        return {
                                            "metadata": metadata,
                                            "schedule": schedule,
                                            "areas_of_interest": list(set(areas))  # Remove duplicates
                                        }
                except Exception as e:
                    logger.debug(f"Error parsing script element: {e}")
            
            return None
        except Exception as e:
            logger.error(f"Error extracting Next.js data: {e}")
            return None
    
    def _get_nested_value(self, obj: Dict, path: str) -> Any:
        """Get a value from a nested dictionary using a dot-separated path."""
        try:
            current = obj
            for key in path.split('.'):
                if isinstance(current, dict):
                    current = current.get(key, {})
                else:
                    return None
            return current if current != {} else None
        except Exception:
            return None

    def clean_text(self, text: str) -> str:
        """Clean up text by removing redundant whitespace and unwanted content."""
        # Remove "RETURN TO ALL SESSIONS" and similar navigation text
        text = re.sub(r'RETURN TO ALL SESSIONS.*$', '', text, flags=re.MULTILINE)
        # Remove "IMAGE COMING SOON" placeholders
        text = re.sub(r'IMAGE COMING SOON\n?', '', text)
        # Remove speaker information (lines containing names and titles)
        text = re.sub(r'\n[^/\n]+(?:\n/[^\n]+\n[^\n]+)*(?:\n|$)', '\n', text)
        # Remove redundant newlines and whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text

    def extract_speakers_from_text(self, text: str) -> List[str]:
        """Extract speaker names and titles from text."""
        speakers = []
        seen_names = set()  # Track unique names to avoid duplicates
        
        # Look for patterns like "/Title\nCompany" or "/Title" at the end of the text
        speaker_pattern = r'/([^/\n]+)(?:\n([^/\n]+))?$'
        matches = re.finditer(speaker_pattern, text)
        
        for match in matches:
            title = match.group(1).strip()
            company = match.group(2).strip() if match.group(2) else ""
            
            # Skip if title is empty or contains placeholder text
            if not title or title.startswith(('IMAGE COMING SOON', 'RETURN TO ALL')):
                continue
            
            # Clean up title and company
            title_parts = title.split()
            clean_title = ' '.join(dict.fromkeys(title_parts))
            
            if company:
                company_parts = company.split()
                clean_company = ' '.join(dict.fromkeys(company_parts))
                speakers.append(f"{clean_title}, {clean_company}")
            else:
                speakers.append(clean_title)
        
        return speakers

    def extract_metadata_from_dom(self, element: webdriver.remote.webelement.WebElement, selector_prefix: str, field_name: str) -> str:
        """Extract metadata from DOM using various selectors."""
        # First try to find the table-like structure with more specific selectors
        table_selectors = [
            # Look for table cells with specific text
            f'tr:has(th:contains("{field_name.upper()}")) td',
            # Look for table cells with specific classes and text
            f'tr th.text-base.font-medium.uppercase:contains("{field_name.upper()}") + td',
            f'tr:has(th.text-base.font-medium.uppercase:contains("{field_name.upper()}")) td.text-base.font-normal.uppercase'
        ]
        
        for selector in table_selectors:
            try:
                elements = element.find_elements(By.CSS_SELECTOR, selector)
                for el in elements:
                    text = el.text.strip()
                    if text and not text.startswith(('IMAGE COMING SOON', 'RETURN TO ALL')):
                        # Clean up text
                        text = re.sub(r'^(Track|Level|Type|Industry|Areas|Topics|Technologies|Duration|Experience):\s*', '', text, flags=re.IGNORECASE)
                        text = re.sub(r'menu_link_content--.*$', '', text).strip()
                        if text:
                            # Clean session type if this is the type field
                            if field_name == 'type':
                                text = self.clean_session_type(text)
                            return text
            except Exception as e:
                logger.debug(f"Error extracting {field_name} with selector {selector}: {e}")
        
        # If table structure not found, try looking for text that matches common patterns for this field
        try:
            # Get all text content
            text_content = element.text
            
            # Define patterns for each field type
            patterns = {
                'track': [
                    r'Track:\s*([^\n]+)',
                    r'TRACK:\s*([^\n]+)'
                ],
                'level': [
                    r'Level:\s*([^\n]+)',
                    r'LEVEL:\s*([^\n]+)',
                    r'Skill Level:\s*([^\n]+)',
                    r'SKILL LEVEL:\s*([^\n]+)'
                ],
                'type': [
                    r'Type:\s*([^\n]+)',
                    r'TYPE:\s*([^\n]+)'
                ],
                'industry': [
                    r'Industry:\s*([^\n]+)',
                    r'INDUSTRY:\s*([^\n]+)'
                ],
                'technologies': [
                    r'Technologies:\s*([^\n]+)',
                    r'TECHNOLOGIES:\s*([^\n]+)'
                ],
                'duration': [
                    r'Duration:\s*([^\n]+)',
                    r'DURATION:\s*([^\n]+)'
                ],
                'experience': [
                    r'Experience:\s*([^\n]+)',
                    r'EXPERIENCE:\s*([^\n]+)'
                ]
            }
            
            # Try each pattern for the current field
            if field_name in patterns:
                for pattern in patterns[field_name]:
                    match = re.search(pattern, text_content, re.IGNORECASE)
                    if match:
                        text = match.group(1) if len(match.groups()) > 0 else match.group(0)
                        text = text.strip()
                        if text:
                            # Clean session type if this is the type field
                            if field_name == 'type':
                                text = self.clean_session_type(text)
                            return text
        except Exception as e:
            logger.debug(f"Error inferring {field_name} from content: {e}")
        
        return ""

    def extract_session_data_from_dom(self, session_url: str = "") -> List[Dict]:
        """Extract session data directly from the DOM structure."""
        try:
            sessions = []
            
            # Wait for any content to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # Wait a bit longer for dynamic content
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Timeout waiting for page to load: {e}")
                return []
            
            # Look for session details container
            session_elements = self.driver.find_elements(By.CSS_SELECTOR, 'article, main')
            
            if not session_elements:
                logger.warning("No session elements found in DOM")
                return []
            
            for element in session_elements:
                try:
                    # Use URL path segment as session ID
                    session_id = session_url.split("/")[-1] if session_url else str(uuid.uuid4())
                    
                    session_data = {
                        "session_id": session_id,
                        "title": "",
                        "description": "",
                        "track": "",
                        "level": "",
                        "type": "",
                        "industry": "",
                        "technologies": [],
                        "duration": "",
                        "experience": "",
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
                    title_element = element.find_elements(By.CSS_SELECTOR, 'h1')
                    if title_element:
                        session_data["title"] = title_element[0].text.strip()
                    
                    # Extract description
                    desc_elements = element.find_elements(By.CSS_SELECTOR, 'div[class*="content"], div[class*="description"], p')
                    if desc_elements:
                        full_text = " ".join(e.text.strip() for e in desc_elements if e.text.strip())
                        if full_text:
                            # Extract speakers from the text
                            session_data["speakers"] = self.extract_speakers_from_text(full_text)
                            # Clean up the description text
                            session_data["description"] = self.clean_text(full_text)
                    
                    # Extract metadata from the table structure
                    try:
                        # Look for the metadata table
                        logger.debug("Looking for metadata table...")
                        table_rows = element.find_elements(By.CSS_SELECTOR, 'tr')
                        logger.debug(f"Found {len(table_rows)} table rows")
                        
                        for row in table_rows:
                            try:
                                # Get the header and value cells
                                header = row.find_element(By.TAG_NAME, 'th').text.strip().upper()
                                value = row.find_element(By.TAG_NAME, 'td').text.strip()
                                logger.debug(f"Found metadata: {header} = {value}")
                                
                                # Map the header to the appropriate field
                                if header == 'EXPERIENCE':
                                    session_data['experience'] = value
                                elif header == 'TYPE':
                                    session_data['type'] = value
                                elif header == 'TRACK':
                                    session_data['track'] = value
                                elif header == 'INDUSTRY':
                                    session_data['industry'] = value
                                elif header == 'TECHNOLOGIES':
                                    # Split technologies by comma and clean each one
                                    session_data['technologies'] = [tech.strip() for tech in value.split(',') if tech.strip()]
                                elif header in ['SKILL LEVEL', 'LEVEL']:
                                    session_data['level'] = value
                                elif header == 'DURATION':
                                    session_data['duration'] = value
                            except Exception as e:
                                logger.debug(f"Error processing table row: {e}")
                                continue
                    except Exception as e:
                        logger.debug(f"Error extracting metadata table: {e}")
                    
                    # Extract areas of interest from description text
                    description_text = session_data["description"]
                    if description_text:
                        # Look for common data/AI topics in the description
                        topics = [
                            "Data Engineering", "Data Science", "Machine Learning", "AI", "Analytics",
                            "Business Intelligence", "Data Governance", "Data Quality", "Data Security",
                            "Data Privacy", "Data Lake", "Data Warehouse", "ETL", "ELT", "Streaming",
                            "Real-time", "Batch Processing", "Data Modeling", "Data Architecture",
                            "Data Integration", "Data Pipeline", "Data Mesh", "Data Fabric",
                            "Delta Lake", "Apache Spark", "SQL", "Python", "Scala"
                        ]
                        
                        found_topics = []
                        for topic in topics:
                            if topic.lower() in description_text.lower():
                                found_topics.append(topic)
                        
                        if found_topics:
                            session_data["areas_of_interest"].extend(found_topics)
                            session_data["areas_of_interest"] = list(set(session_data["areas_of_interest"]))
                    
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
    scraper = DaisScraper(preview_mode=args.preview, preview_count=args.preview_count)
    sessions = scraper.fetch_sessions()
    if sessions:
        scraper.save_sessions(sessions)
        print(f"Successfully saved {len(sessions)} sessions")
    else:
        print("No sessions were found or saved")

if __name__ == "__main__":
    main() 