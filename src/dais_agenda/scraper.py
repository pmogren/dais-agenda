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
                    },
                    "description": description
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
            
            # Define the order of fields
            field_order = [
                "session_id",
                "title",
                "track",
                "level",
                "type",
                "industry",
                "category",
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
            
            # Save all sessions
            all_sessions_file = self.sessions_dir / "sessions_.jsonl"
            with open(all_sessions_file, "w") as f:
                for session in sessions:
                    ordered = ordered_session(session)
                    json.dump(ordered, f)
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
                    nextjs_data = self.extract_nextjs_data()
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
                    logger.info("Found data in JavaScript variables")
                    
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
        
        # Look for patterns like "Name\n/Title\nCompany" or "Name\n/Title"
        speaker_pattern = r'([^\n/]+)\n/([^\n]+)(?:\n([^\n/]+))?'
        matches = re.finditer(speaker_pattern, text)
        
        for match in matches:
            name = match.group(1).strip()
            title = match.group(2).strip()
            company = match.group(3).strip() if match.group(3) else ""
            
            # Skip if name is a placeholder or navigation text
            if (name.startswith('IMAGE COMING SOON') or 
                name.startswith('RETURN TO ALL SESSIONS') or
                not name):
                continue
            
            # Clean up name by removing duplicates within it
            name_parts = name.split()
            clean_name = ' '.join(dict.fromkeys(name_parts))
            
            # Clean up company name by removing duplicates
            if company:
                company_parts = company.split()
                clean_company = ' '.join(dict.fromkeys(company_parts))
            else:
                # Try to extract company from title if not provided separately
                company_match = re.search(r',\s*([^,]+)$', title)
                if company_match:
                    clean_company = company_match.group(1).strip()
                    title = title[:company_match.start()].strip()
                else:
                    clean_company = ""
            
            # Create unique speaker identifier
            speaker_key = f"{clean_name}:{clean_company}"
            
            # Only add if we haven't seen this speaker before
            if speaker_key not in seen_names:
                seen_names.add(speaker_key)
                if clean_company:
                    speakers.append(f"{clean_name} ({title}, {clean_company})")
                else:
                    speakers.append(f"{clean_name} ({title})")
        
        return speakers

    def extract_metadata_from_dom(self, element: webdriver.remote.webelement.WebElement, selector_prefix: str, field_name: str) -> str:
        """Extract metadata from DOM using various selectors."""
        selectors = [
            # Direct class matches
            f'{selector_prefix}[class*="{field_name}"]',
            f'{selector_prefix}[data-test*="{field_name}"]',
            f'{selector_prefix}[data-type*="{field_name}"]',
            
            # Label + value patterns
            f'[class*="{field_name}-label"] + {selector_prefix}',
            f'[data-test*="{field_name}-label"] + {selector_prefix}',
            f'[data-type*="{field_name}-label"] + {selector_prefix}',
            
            # Common metadata patterns
            f'dt[class*="{field_name}"] + dd',
            f'th[class*="{field_name}"] + td',
            f'div[class*="metadata"] {selector_prefix}[class*="{field_name}"]',
            f'div[class*="details"] {selector_prefix}[class*="{field_name}"]',
            f'div[class*="info"] {selector_prefix}[class*="{field_name}"]',
            
            # Attribute-based selectors
            f'{selector_prefix}[aria-label*="{field_name}"]',
            f'{selector_prefix}[title*="{field_name}"]',
            f'{selector_prefix}[name*="{field_name}"]',
            
            # Common class patterns
            f'.session-{field_name}',
            f'.event-{field_name}',
            f'.{field_name}-value',
            f'.{field_name}-text',
            
            # Additional specific selectors for track
            f'div[class*="track"] {selector_prefix}',
            f'span[class*="track"]',
            f'div[class*="session-track"]',
            f'div[class*="event-track"]',
            
            # Additional specific selectors for industry
            f'div[class*="industry"] {selector_prefix}',
            f'span[class*="industry"]',
            f'div[class*="session-industry"]',
            f'div[class*="event-industry"]',
            
            # Additional specific selectors for category
            f'div[class*="category"] {selector_prefix}',
            f'span[class*="category"]',
            f'div[class*="session-category"]',
            f'div[class*="event-category"]',
            
            # Additional specific selectors for areas of interest
            f'div[class*="areas"] {selector_prefix}',
            f'span[class*="areas"]',
            f'div[class*="session-areas"]',
            f'div[class*="event-areas"]',
            f'div[class*="topics"] {selector_prefix}',
            f'span[class*="topics"]',
            f'div[class*="session-topics"]',
            f'div[class*="event-topics"]'
        ]
        
        # Add variations with capitalized field name
        field_name_cap = field_name.capitalize()
        selectors.extend([
            f'{selector_prefix}[class*="{field_name_cap}"]',
            f'{selector_prefix}[data-test*="{field_name_cap}"]',
            f'{selector_prefix}[data-type*="{field_name_cap}"]',
            f'.session{field_name_cap}',
            f'.event{field_name_cap}'
        ])
        
        # Try each selector
        for selector in selectors:
            try:
                elements = element.find_elements(By.CSS_SELECTOR, selector)
                for el in elements:
                    text = el.text.strip()
                    if text and not text.startswith(('IMAGE COMING SOON', 'RETURN TO ALL')):
                        # Clean up text by removing any label prefixes
                        text = re.sub(r'^(Track|Level|Type|Industry|Category|Areas|Topics):\s*', '', text, flags=re.IGNORECASE)
                        # Clean up text by removing any Drupal-specific content types
                        text = re.sub(r'menu_link_content--.*$', '', text).strip()
                        if text:
                            return text
            except Exception as e:
                logger.debug(f"Error extracting {field_name} with selector {selector}: {e}")
        
        # Try looking for text that matches common patterns for this field
        try:
            # Get all text content
            text_content = element.text
            
            # Define patterns for each field type
            patterns = {
                'track': [
                    r'Track:\s*([^\n]+)',
                    r'Session Track:\s*([^\n]+)',
                    r'Event Track:\s*([^\n]+)',
                    r'(?:Data Engineering|Data Science|Machine Learning|AI|Analytics|Business|Technical|Strategy)'
                ],
                'level': [
                    r'Level:\s*([^\n]+)',
                    r'Difficulty:\s*([^\n]+)',
                    r'Experience Level:\s*([^\n]+)',
                    r'(?:Beginner|Intermediate|Advanced|Expert)'
                ],
                'type': [
                    r'Type:\s*([^\n]+)',
                    r'Session Type:\s*([^\n]+)',
                    r'Format:\s*([^\n]+)',
                    r'(?:Keynote|Workshop|Breakout|Panel|Tutorial)'
                ],
                'industry': [
                    r'Industry:\s*([^\n]+)',
                    r'Sector:\s*([^\n]+)',
                    r'Vertical:\s*([^\n]+)',
                    r'(?:Financial Services|Healthcare|Retail|Manufacturing|Technology|Education|Government)'
                ],
                'category': [
                    r'Category:\s*([^\n]+)',
                    r'Topic:\s*([^\n]+)',
                    r'Theme:\s*([^\n]+)',
                    r'(?:Data Engineering|Data Science|Machine Learning|AI|Analytics|Business|Technical|Strategy)'
                ]
            }
            
            # Try each pattern for the current field
            if field_name in patterns:
                for pattern in patterns[field_name]:
                    match = re.search(pattern, text_content, re.IGNORECASE)
                    if match:
                        text = match.group(1) if len(match.groups()) > 0 else match.group(0)
                        text = text.strip()
                        if text and not text.startswith(('IMAGE COMING SOON', 'RETURN TO ALL')):
                            return text
        except Exception as e:
            logger.debug(f"Error extracting {field_name} from text content: {e}")
        
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
            
            # Try to find Next.js data structure first
            nextjs_data = self.extract_nextjs_data()
            if nextjs_data:
                logger.info("Found Next.js data structure")
            
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
                    
                    # Extract description - look for content sections
                    description_selectors = [
                        'div[class*="content"]', 'div[class*="description"]', 'div[class*="abstract"]',
                        'div[class*="summary"]', 'div[class*="text"]', 'p'
                    ]
                    for selector in description_selectors:
                        try:
                            desc_elements = element.find_elements(By.CSS_SELECTOR, selector)
                            if desc_elements:
                                # Get the full text including speaker information
                                full_text = " ".join(e.text.strip() for e in desc_elements if e.text.strip())
                                if full_text:
                                    # Extract speakers from the text
                                    session_data["speakers"] = self.extract_speakers_from_text(full_text)
                                    # Clean up the description text
                                    session_data["description"] = self.clean_text(full_text)
                                    break
                        except Exception as e:
                            logger.debug(f"Error extracting description with selector {selector}: {e}")
                    
                    # Extract metadata fields using common selectors
                    for field in ['track', 'level', 'type', 'industry', 'category']:
                        # First try to get from Next.js data
                        if nextjs_data and "metadata" in nextjs_data:
                            value = nextjs_data["metadata"].get(field, "")
                            if value:
                                session_data[field] = value
                                continue
                        
                        # If not found in Next.js data, try DOM selectors
                        value = self.extract_metadata_from_dom(element, 'div', field)
                        if not value:
                            value = self.extract_metadata_from_dom(element, 'span', field)
                        session_data[field] = value
                    
                    # Extract areas of interest
                    # First try to get from Next.js data
                    if nextjs_data and "areas_of_interest" in nextjs_data:
                        areas = nextjs_data["areas_of_interest"]
                        if areas:
                            session_data["areas_of_interest"] = areas
                    else:
                        # If not found in Next.js data, try DOM selectors
                        areas_selectors = [
                            'div[class*="areas"]', 'div[class*="topics"]', 'div[class*="tags"]',
                            'span[class*="areas"]', 'span[class*="topics"]', 'span[class*="tags"]',
                            '[data-type="areas"]', '[data-test="session-areas"]',
                            'ul[class*="tags"] li', 'div[class*="tag-list"] span',
                            'div[class*="session-areas"]', 'div[class*="event-areas"]',
                            'div[class*="session-topics"]', 'div[class*="event-topics"]',
                            'div[class*="session-tags"]', 'div[class*="event-tags"]',
                            'div[class*="metadata"] div[class*="areas"]',
                            'div[class*="metadata"] div[class*="topics"]',
                            'div[class*="metadata"] div[class*="tags"]'
                        ]
                        
                        # Also look for areas in the description text
                        description_text = session_data["description"]
                        if description_text:
                            # Look for common data/AI topics in the description
                            topics = [
                                "Data Engineering", "Data Science", "Machine Learning", "AI", "Analytics",
                                "Business Intelligence", "Data Governance", "Data Quality", "Data Security",
                                "Data Privacy", "Data Lake", "Data Warehouse", "ETL", "ELT", "Streaming",
                                "Real-time", "Batch Processing", "Data Modeling", "Data Architecture",
                                "Data Integration", "Data Pipeline", "Data Mesh", "Data Fabric",
                                "Delta Lake", "Apache Spark", "SQL", "Python", "R", "Scala",
                                "Deep Learning", "NLP", "Computer Vision", "Recommendation Systems",
                                "Time Series", "Anomaly Detection", "Feature Engineering",
                                "Model Deployment", "MLOps", "Model Monitoring", "Model Governance"
                            ]
                            
                            found_topics = []
                            for topic in topics:
                                if topic.lower() in description_text.lower():
                                    found_topics.append(topic)
                            
                            if found_topics:
                                session_data["areas_of_interest"].extend(found_topics)
                        
                        for selector in areas_selectors:
                            try:
                                areas_elements = element.find_elements(By.CSS_SELECTOR, selector)
                                if areas_elements:
                                    areas = []
                                    for area_element in areas_elements:
                                        text = area_element.text.strip()
                                        if text and not text.startswith(('IMAGE COMING SOON', 'RETURN TO ALL')):
                                            # Split by common delimiters
                                            split_areas = re.split(r'[,;|]|\s+and\s+', text)
                                            areas.extend([area.strip() for area in split_areas if area.strip()])
                                    if areas:
                                        session_data["areas_of_interest"].extend(areas)
                            except Exception as e:
                                logger.debug(f"Error extracting areas with selector {selector}: {e}")
                        
                        # Remove duplicates and empty values
                        session_data["areas_of_interest"] = list(set(
                            area for area in session_data["areas_of_interest"] 
                            if area and not area.startswith(('IMAGE COMING SOON', 'RETURN TO ALL'))
                        ))
                    
                    # Extract schedule information
                    # First try to get from Next.js data
                    if nextjs_data and "schedule" in nextjs_data:
                        schedule = nextjs_data["schedule"]
                        if schedule:
                            session_data["schedule"].update(schedule)
                    else:
                        # If not found in Next.js data, try DOM selectors
                        schedule_selectors = [
                            'div[class*="schedule"]', 'div[class*="time"]', 'div[class*="date"]',
                            'span[class*="schedule"]', 'span[class*="time"]', 'span[class*="date"]',
                            '[data-type="schedule"]', '[data-test="session-schedule"]',
                            '[class*="datetime"]', '[class*="session-time"]'
                        ]
                        for selector in schedule_selectors:
                            try:
                                schedule_elements = element.find_elements(By.CSS_SELECTOR, selector)
                                if schedule_elements:
                                    schedule_text = schedule_elements[0].text.strip()
                                    if schedule_text:
                                        # Look for day
                                        day_patterns = [
                                            r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)',
                                            r'\d{1,2}/\d{1,2}(?:/\d{2,4})?',
                                            r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:st|nd|rd|th)?(?:,? \d{4})?'
                                        ]
                                        for pattern in day_patterns:
                                            match = re.search(pattern, schedule_text, re.IGNORECASE)
                                            if match:
                                                session_data["schedule"]["day"] = match.group(0)
                                                break
                                        
                                        # Look for time
                                        time_patterns = [
                                            r'(\d{1,2}:\d{2}\s*[AP]M)\s*[-–]\s*(\d{1,2}:\d{2}\s*[AP]M)',
                                            r'(\d{1,2}(?::\d{2})?\s*[AP]M)\s*[-–]\s*(\d{1,2}(?::\d{2})?\s*[AP]M)'
                                        ]
                                        for pattern in time_patterns:
                                            match = re.search(pattern, schedule_text)
                                            if match:
                                                session_data["schedule"]["start_time"] = match.group(1)
                                                session_data["schedule"]["end_time"] = match.group(2)
                                                break
                                        
                                        # Look for room/location
                                        room_patterns = [
                                            r'(?:Room|Location|Venue):\s*([^\n,]+)',
                                            r'(?:Room|Location|Venue)\s+([A-Z0-9][^\n,]*)',
                                            r'(?:^|\s)(?:Room|Location|Venue)\s+([^\n,]+)'
                                        ]
                                        for pattern in room_patterns:
                                            match = re.search(pattern, schedule_text, re.IGNORECASE)
                                            if match:
                                                session_data["schedule"]["room"] = match.group(1).strip()
                                                break
                                    break
                            except Exception as e:
                                logger.debug(f"Error extracting schedule with selector {selector}: {e}")
                    
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