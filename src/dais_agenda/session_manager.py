import json
from pathlib import Path
from typing import Dict, List, Optional, Set
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SessionManager:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.sessions_dir = self.data_dir / "sessions"
        self.user_dir = self.data_dir / "user"
        
        # Create directories if they don't exist
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.user_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize data
        self.sessions_df = self._load_all_sessions()
        self.ratings_df = self._load_user_data("ratings.jsonl")
        self.tags_df = self._load_user_data("tags.jsonl")

    def _load_all_sessions(self) -> pd.DataFrame:
        """Load all session data from JSONL files."""
        all_sessions = []
        for jsonl_file in self.sessions_dir.glob("sessions_*.jsonl"):
            try:
                df = pd.read_json(jsonl_file, lines=True)
                all_sessions.append(df)
            except Exception as e:
                logger.error(f"Error loading {jsonl_file}: {e}")
        
        if not all_sessions:
            return pd.DataFrame()
        
        return pd.concat(all_sessions, ignore_index=True)

    def _load_user_data(self, filename: str) -> pd.DataFrame:
        """Load user data (ratings or tags) from JSONL file."""
        file_path = self.user_dir / filename
        if not file_path.exists():
            # Initialize with proper dtypes
            if filename == "ratings.jsonl":
                return pd.DataFrame({
                    "session_id": pd.Series(dtype="string"),
                    "rating": pd.Series(dtype="float64"),
                    "notes": pd.Series(dtype="string"),
                    "timestamp": pd.Series(dtype="datetime64[ns, UTC]"),
                    "interest_level": pd.Series(dtype="float64"),
                    "interest_notes": pd.Series(dtype="string"),
                    "interest_timestamp": pd.Series(dtype="datetime64[ns, UTC]")
                })
            else:  # tags.jsonl
                return pd.DataFrame({
                    "session_id": pd.Series(dtype="string"),
                    "tags": pd.Series(dtype="object"),  # List of strings
                    "timestamp": pd.Series(dtype="datetime64[ns, UTC]")
                })
        
        try:
            df = pd.read_json(file_path, lines=True)
            # Ensure proper dtypes after loading
            if filename == "ratings.jsonl":
                df["session_id"] = df["session_id"].astype("string")
                df["rating"] = df["rating"].astype("float64")
                df["notes"] = df["notes"].astype("string")
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
                df["interest_level"] = df["interest_level"].astype("float64")
                df["interest_notes"] = df["interest_notes"].astype("string")
                df["interest_timestamp"] = pd.to_datetime(df["interest_timestamp"], utc=True)
            else:  # tags.jsonl
                df["session_id"] = df["session_id"].astype("string")
                df["tags"] = df["tags"].astype("object")
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return pd.DataFrame()

    def _save_user_data(self, df: pd.DataFrame, filename: str):
        """Save user data (ratings or tags) to JSONL file."""
        file_path = self.user_dir / filename
        try:
            df.to_json(file_path, orient="records", lines=True)
        except Exception as e:
            logger.error(f"Error saving to {file_path}: {e}")

    def get_sessions_by_track(self, track: str) -> pd.DataFrame:
        """Get all sessions for a specific track."""
        return self.sessions_df[self.sessions_df["track"] == track]

    def get_sessions_by_level(self, level: str) -> pd.DataFrame:
        """Get all sessions for a specific level."""
        return self.sessions_df[self.sessions_df["level"] == level]

    def get_sessions_by_speaker(self, speaker: str) -> pd.DataFrame:
        """Get all sessions by a specific speaker."""
        return self.sessions_df[self.sessions_df["speakers"].apply(lambda x: speaker in x)]

    def search_sessions(self, query: str) -> pd.DataFrame:
        """Search sessions by title or description."""
        query = query.lower()
        return self.sessions_df[
            self.sessions_df["title"].str.lower().str.contains(query) |
            self.sessions_df["description"].str.lower().str.contains(query)
        ]

    def get_session_with_user_data(self, session_id: str) -> Optional[Dict]:
        """Get a session with its user data (ratings, interest, and tags)."""
        # First try exact match
        session = self.sessions_df[self.sessions_df["session_id"] == session_id]
        
        # If no exact match, try prefix match
        if session.empty:
            matching_sessions = self.sessions_df[self.sessions_df["session_id"].str.startswith(session_id)]
            if len(matching_sessions) == 1:
                session = matching_sessions
            elif len(matching_sessions) > 1:
                logger.warning(f"Multiple sessions found with prefix '{session_id}': {', '.join(matching_sessions['session_id'])}")
                return None
        
        if session.empty:
            return None

        session_data = session.iloc[0].to_dict()
        
        # Add rating and interest if exists
        if not self.ratings_df.empty and session_id in self.ratings_df["session_id"].values:
            rating_data = self.ratings_df[self.ratings_df["session_id"] == session_id].iloc[0]
            if pd.notna(rating_data.get("rating")):
                session_data["user_rating"] = rating_data["rating"]
                session_data["user_notes"] = rating_data["notes"]
            if pd.notna(rating_data.get("interest_level")):
                session_data["user_interest"] = rating_data["interest_level"]
                session_data["user_interest_notes"] = rating_data["interest_notes"]
        
        # Add tags if exist
        if not self.tags_df.empty and session_id in self.tags_df["session_id"].values:
            tags_data = self.tags_df[self.tags_df["session_id"] == session_id].iloc[0]
            session_data["user_tags"] = tags_data["tags"]
        
        return session_data

    def find_session_by_prefix(self, prefix: str) -> Optional[str]:
        """Find a session ID by prefix. Returns the full session ID if exactly one match is found."""
        matching_sessions = self.sessions_df[self.sessions_df["session_id"].str.startswith(prefix)]
        if len(matching_sessions) == 1:
            return matching_sessions.iloc[0]["session_id"]
        elif len(matching_sessions) > 1:
            logger.warning(f"Multiple sessions found with prefix '{prefix}': {', '.join(matching_sessions['session_id'])}")
        return None

    def add_rating(self, session_id: str, rating: float, notes: str = "") -> str:
        """Add or update a rating for a session."""
        # First try exact match
        if session_id not in self.sessions_df["session_id"].values:
            # If not found, try prefix match
            full_session_id = self.find_session_by_prefix(session_id)
            if not full_session_id:
                raise ValueError(f"Session not found: {session_id}")
            session_id = full_session_id
        
        # Create new rating entry
        new_rating = {
            "session_id": session_id,
            "rating": rating,
            "notes": notes,
            "timestamp": pd.Timestamp.utcnow()
        }

        # Update or add rating
        if not self.ratings_df.empty and session_id in self.ratings_df["session_id"].values:
            self.ratings_df.loc[self.ratings_df["session_id"] == session_id] = new_rating
        else:
            self.ratings_df = pd.concat([self.ratings_df, pd.DataFrame([new_rating])], ignore_index=True)

        self._save_user_data(self.ratings_df, "ratings.jsonl")
        return session_id

    def add_interest(self, session_id: str, interest_level: float, notes: str = "") -> str:
        """Add or update interest level for a session."""
        # First try exact match
        if session_id not in self.sessions_df["session_id"].values:
            # If not found, try prefix match
            full_session_id = self.find_session_by_prefix(session_id)
            if not full_session_id:
                raise ValueError(f"Session not found: {session_id}")
            session_id = full_session_id
        
        # If interest level is 0, remove the interest
        if interest_level == 0:
            self.remove_interest(session_id)
            return
        
        # Create new interest entry
        new_interest = {
            "session_id": session_id,
            "interest_level": interest_level,
            "notes": notes,
            "timestamp": pd.Timestamp.utcnow()
        }

        # Update or add interest
        if not self.ratings_df.empty and session_id in self.ratings_df["session_id"].values:
            self.ratings_df.loc[self.ratings_df["session_id"] == session_id, "interest_level"] = interest_level
            self.ratings_df.loc[self.ratings_df["session_id"] == session_id, "interest_notes"] = notes
            self.ratings_df.loc[self.ratings_df["session_id"] == session_id, "interest_timestamp"] = pd.Timestamp.utcnow()
        else:
            # Add new row with both rating and interest fields
            new_row = {
                "session_id": session_id,
                "rating": None,
                "notes": "",
                "timestamp": None,
                "interest_level": interest_level,
                "interest_notes": notes,
                "interest_timestamp": pd.Timestamp.utcnow()
            }
            self.ratings_df = pd.concat([self.ratings_df, pd.DataFrame([new_row])], ignore_index=True)

        self._save_user_data(self.ratings_df, "ratings.jsonl")
        return session_id

    def remove_interest(self, session_id: str) -> str:
        """Remove interest level for a session."""
        # First try exact match
        if session_id not in self.sessions_df["session_id"].values:
            # If not found, try prefix match
            full_session_id = self.find_session_by_prefix(session_id)
            if not full_session_id:
                raise ValueError(f"Session not found: {session_id}")
            session_id = full_session_id
        
        # If ratings DataFrame is empty, there's nothing to remove
        if self.ratings_df.empty:
            return session_id
        
        # Remove interest level but keep rating if it exists
        mask = self.ratings_df["session_id"] == session_id
        if not self.ratings_df[mask].empty:
            self.ratings_df.loc[mask, "interest_level"] = None
            self.ratings_df.loc[mask, "interest_notes"] = None
            self.ratings_df.loc[mask, "interest_timestamp"] = None

        self._save_user_data(self.ratings_df, "ratings.jsonl")
        return session_id

    def remove_rating(self, session_id: str) -> str:
        """Remove all ratings for a session."""
        # First try exact match
        if session_id not in self.sessions_df["session_id"].values:
            # If not found, try prefix match
            full_session_id = self.find_session_by_prefix(session_id)
            if not full_session_id:
                raise ValueError(f"Session not found: {session_id}")
            session_id = full_session_id
        
        # If ratings DataFrame is empty, there's nothing to remove
        if self.ratings_df.empty:
            return session_id
        
        # Remove all ratings but keep tags
        self.ratings_df = self.ratings_df[self.ratings_df["session_id"] != session_id]

        self._save_user_data(self.ratings_df, "ratings.jsonl")
        return session_id

    def add_tags(self, session_id: str, tags: List[str]):
        """Add custom tags to a session."""
        # Try to find exact session ID first
        if session_id not in self.sessions_df["session_id"].values:
            # If not found, try prefix match
            full_session_id = self.find_session_by_prefix(session_id)
            if full_session_id:
                session_id = full_session_id
            else:
                logger.error(f"Session {session_id} not found")
                return

        # Create new tags entry
        new_tags = {
            "session_id": session_id,
            "tags": tags,
            "timestamp": pd.Timestamp.utcnow()
        }

        # Update or add tags
        if not self.tags_df.empty and session_id in self.tags_df["session_id"].values:
            # Get existing tags
            existing_tags = self.tags_df.loc[self.tags_df["session_id"] == session_id, "tags"].iloc[0]
            # Combine with new tags and remove duplicates
            combined_tags = list(set(existing_tags + tags))
            # Update the tags
            mask = self.tags_df["session_id"] == session_id
            self.tags_df.loc[mask, "tags"] = pd.Series([combined_tags], index=self.tags_df[mask].index)
            self.tags_df.loc[mask, "timestamp"] = pd.Timestamp.utcnow()
        else:
            self.tags_df = pd.concat([self.tags_df, pd.DataFrame([new_tags])], ignore_index=True)

        self._save_user_data(self.tags_df, "tags.jsonl")

    def remove_tags(self, session_id: str, tags_to_remove: List[str]):
        """Remove specific tags from a session."""
        # Try to find exact session ID first
        if session_id not in self.sessions_df["session_id"].values:
            # If not found, try prefix match
            full_session_id = self.find_session_by_prefix(session_id)
            if full_session_id:
                session_id = full_session_id
            else:
                logger.error(f"Session {session_id} not found")
                return

        # If no tags exist for this session, nothing to remove
        if self.tags_df.empty or session_id not in self.tags_df["session_id"].values:
            return

        # Get current tags
        mask = self.tags_df["session_id"] == session_id
        current_tags = self.tags_df.loc[mask, "tags"].iloc[0]
        
        # Remove specified tags
        updated_tags = [tag for tag in current_tags if tag not in tags_to_remove]
        
        # Update tags entry
        self.tags_df.loc[mask, "tags"] = pd.Series([updated_tags], index=self.tags_df[mask].index)
        self.tags_df.loc[mask, "timestamp"] = pd.Timestamp.utcnow()
        
        self._save_user_data(self.tags_df, "tags.jsonl")

    def get_recommendations(self, min_rating: float = 4) -> pd.DataFrame:
        """Get recommended sessions based on ratings."""
        if self.ratings_df.empty:
            return pd.DataFrame()

        # Get highly rated sessions
        highly_rated = self.ratings_df[self.ratings_df["rating"] >= min_rating]
        if highly_rated.empty:
            return pd.DataFrame()

        # Get sessions with similar tracks or levels
        recommended_sessions = []
        for _, rating in highly_rated.iterrows():
            session = self.sessions_df[self.sessions_df["session_id"] == rating["session_id"]].iloc[0]
            similar_sessions = self.sessions_df[
                (self.sessions_df["track"] == session["track"]) |
                (self.sessions_df["level"] == session["level"])
            ]
            recommended_sessions.append(similar_sessions)

        if not recommended_sessions:
            return pd.DataFrame()

        recommendations = pd.concat(recommended_sessions, ignore_index=True)
        return recommendations.drop_duplicates() 