from dataclasses import dataclass, field
from typing import List, Dict, Optional
import json
from pathlib import Path
import uuid

@dataclass
class UserRating:
    session_id: str
    rating: int  # 1-5 scale
    notes: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    user_id: str = field(default_factory=lambda: str(uuid.uuid4()))

class UserDataManager:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.ratings_file = data_dir / "user_ratings.jsonl"
        self.tags_file = data_dir / "user_tags.jsonl"
        self._ensure_files_exist()

    def _ensure_files_exist(self):
        """Ensure the data files exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.ratings_file.touch(exist_ok=True)
        self.tags_file.touch(exist_ok=True)

    def add_rating(self, rating: UserRating) -> None:
        """Add a new rating for a session."""
        with open(self.ratings_file, "a") as f:
            json.dump({
                "session_id": rating.session_id,
                "rating": rating.rating,
                "notes": rating.notes,
                "tags": rating.tags,
                "user_id": rating.user_id
            }, f)
            f.write("\n")

    def get_ratings(self, session_id: Optional[str] = None) -> List[UserRating]:
        """Get all ratings, optionally filtered by session_id."""
        ratings = []
        with open(self.ratings_file, "r") as f:
            for line in f:
                data = json.loads(line)
                if session_id is None or data["session_id"] == session_id:
                    ratings.append(UserRating(
                        session_id=data["session_id"],
                        rating=data["rating"],
                        notes=data["notes"],
                        tags=data["tags"],
                        user_id=data["user_id"]
                    ))
        return ratings

    def get_average_rating(self, session_id: str) -> float:
        """Get the average rating for a session."""
        ratings = self.get_ratings(session_id)
        if not ratings:
            return 0.0
        return sum(r.rating for r in ratings) / len(ratings)

    def get_session_tags(self, session_id: str) -> List[str]:
        """Get all unique tags for a session."""
        tags = set()
        for rating in self.get_ratings(session_id):
            tags.update(rating.tags)
        return list(tags)

    def get_all_tags(self) -> Dict[str, int]:
        """Get all unique tags and their usage count."""
        tag_counts = {}
        with open(self.ratings_file, "r") as f:
            for line in f:
                data = json.loads(line)
                for tag in data["tags"]:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
        return tag_counts

    def update_rating(self, rating: UserRating) -> None:
        """Update an existing rating."""
        # Read all ratings
        ratings = self.get_ratings()
        
        # Filter out the old rating
        ratings = [r for r in ratings if not (r.session_id == rating.session_id and r.user_id == rating.user_id)]
        
        # Add the new rating
        ratings.append(rating)
        
        # Write all ratings back
        with open(self.ratings_file, "w") as f:
            for r in ratings:
                json.dump({
                    "session_id": r.session_id,
                    "rating": r.rating,
                    "notes": r.notes,
                    "tags": r.tags,
                    "user_id": r.user_id
                }, f)
                f.write("\n")

    def delete_rating(self, session_id: str, user_id: str) -> None:
        """Delete a rating for a session."""
        # Read all ratings
        ratings = self.get_ratings()
        
        # Filter out the rating to delete
        ratings = [r for r in ratings if not (r.session_id == session_id and r.user_id == user_id)]
        
        # Write remaining ratings back
        with open(self.ratings_file, "w") as f:
            for r in ratings:
                json.dump({
                    "session_id": r.session_id,
                    "rating": r.rating,
                    "notes": r.notes,
                    "tags": r.tags,
                    "user_id": r.user_id
                }, f)
                f.write("\n") 