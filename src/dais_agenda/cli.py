import typer
from typing import Optional, List
from pathlib import Path
import json
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.markdown import Markdown
from datetime import datetime
import logging
import click
from .user_data import UserDataManager, UserRating

from .session_manager import SessionManager

app = typer.Typer()
console = Console()
logger = logging.getLogger(__name__)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

@app.command()
def list(
    track: Optional[str] = typer.Option(None, help="Filter sessions by track"),
    level: Optional[str] = typer.Option(None, help="Filter sessions by level"),
    speaker: Optional[str] = typer.Option(None, help="Filter sessions by speaker"),
    search: Optional[str] = typer.Option(None, help="Search sessions by title or description"),
    show_details: bool = typer.Option(False, "--details", help="Show detailed session information")
):
    """List sessions with optional filtering."""
    setup_logging()
    manager = SessionManager()
    
    # Get filtered sessions
    if track:
        sessions = manager.get_sessions_by_track(track)
    elif level:
        sessions = manager.get_sessions_by_level(level)
    elif speaker:
        sessions = manager.get_sessions_by_speaker(speaker)
    elif search:
        sessions = manager.search_sessions(search)
    else:
        sessions = manager.sessions_df
    
    if sessions.empty:
        console.print("[yellow]No sessions found matching the criteria.[/yellow]")
        return
    
    # Create table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("ID", style="dim")
    table.add_column("Title")
    table.add_column("Track")
    table.add_column("Level")
    table.add_column("Time")
    table.add_column("Room")
    
    if show_details:
        table.add_column("Description")
        table.add_column("Speakers")
    
    # Add rows
    for _, session in sessions.iterrows():
        # Get user data if available
        session_data = manager.get_session_with_user_data(session["session_id"])
        
        # Format time
        schedule = session.get("schedule", {})
        time_str = f"{schedule.get('start_time', '')} - {schedule.get('end_time', '')}"
        
        # Add rating indicator if available
        title = session["title"]
        if session_data and "user_rating" in session_data:
            title = f"{title} [yellow]({session_data['user_rating']}★)[/yellow]"
        
        # Add tags if available
        if session_data and "user_tags" in session_data:
            tags = " ".join(f"[blue]#{tag}[/blue]" for tag in session_data["user_tags"])
            title = f"{title}\n{tags}"
        
        row = [
            session["session_id"],
            title,
            session["track"],
            session["level"],
            time_str,
            schedule.get("room", "")
        ]
        
        if show_details:
            row.extend([
                session["description"],
                "\n".join(session["speakers"])
            ])
        
        table.add_row(*row)
    
    # Display table
    console.print(table)

@app.command()
def rate(
    session_id: str = typer.Argument(..., help="Session ID to rate"),
    rating: int = typer.Argument(..., help="Rating (1-5)"),
    notes: str = typer.Option("", help="Optional notes about the session")
):
    """Rate a session."""
    setup_logging()
    manager = SessionManager()
    
    if rating < 1 or rating > 5:
        console.print("[red]Rating must be between 1 and 5[/red]")
        raise typer.Exit(1)
    
    manager.add_rating(session_id, rating, notes)
    console.print(f"[green]Successfully rated session {session_id} with {rating} stars[/green]")

@app.command()
def tag(
    session_id: str = typer.Argument(..., help="Session ID to tag"),
    tags: List[str] = typer.Argument(..., help="Tags to add (comma-separated)")
):
    """Add tags to a session."""
    setup_logging()
    manager = SessionManager()
    
    manager.add_tags(session_id, tags)
    console.print(f"[green]Successfully added tags to session {session_id}: {', '.join(tags)}[/green]")

@app.command()
def recommend(
    min_rating: int = typer.Option(4, help="Minimum rating to consider for recommendations"),
    limit: int = typer.Option(10, help="Maximum number of recommendations to show")
):
    """Get personalized session recommendations."""
    setup_logging()
    manager = SessionManager()
    
    recommendations = manager.get_recommendations(min_rating)
    if recommendations.empty:
        console.print("[yellow]No recommendations available. Try rating some sessions first.[/yellow]")
        return
    
    # Create table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("ID", style="dim")
    table.add_column("Title")
    table.add_column("Track")
    table.add_column("Level")
    table.add_column("Time")
    table.add_column("Room")
    
    # Add rows
    for _, session in recommendations.head(limit).iterrows():
        # Get user data if available
        session_data = manager.get_session_with_user_data(session["session_id"])
        
        # Format time
        schedule = session.get("schedule", {})
        time_str = f"{schedule.get('start_time', '')} - {schedule.get('end_time', '')}"
        
        # Add rating indicator if available
        title = session["title"]
        if session_data and "user_rating" in session_data:
            title = f"{title} [yellow]({session_data['user_rating']}★)[/yellow]"
        
        # Add tags if available
        if session_data and "user_tags" in session_data:
            tags = " ".join(f"[blue]#{tag}[/blue]" for tag in session_data["user_tags"])
            title = f"{title}\n{tags}"
        
        table.add_row(
            session["session_id"],
            title,
            session["track"],
            session["level"],
            time_str,
            schedule.get("room", "")
        )
    
    # Display table
    console.print(Panel.fit(table, title="Recommended Sessions"))

@app.command()
def scrape():
    """Scrape the latest session data from the Databricks website."""
    setup_logging()
    from .scraper import DaisScraper
    
    scraper = DaisScraper()
    sessions = scraper.fetch_sessions()
    if sessions:
        scraper.save_sessions(sessions)
        console.print(f"[green]Successfully saved {len(sessions)} sessions[/green]")
    else:
        console.print("[red]No sessions were found or saved[/red]")

@click.group()
@click.option('--data-dir', type=click.Path(), default='data/user_data',
              help='Directory to store user data')
@click.pass_context
def cli(ctx, data_dir):
    """Manage your Data + AI Summit session ratings and tags."""
    ctx.obj = UserDataManager(Path(data_dir))

@cli.command()
@click.argument('session_id')
@click.argument('rating', type=click.IntRange(1, 5))
@click.option('--notes', help='Additional notes about the session')
@click.option('--tags', help='Comma-separated list of tags')
@click.pass_obj
def rate(manager: UserDataManager, session_id: str, rating: int, notes: Optional[str], tags: Optional[str]):
    """Rate a session and optionally add tags."""
    tag_list = [t.strip() for t in tags.split(',')] if tags else []
    user_rating = UserRating(
        session_id=session_id,
        rating=rating,
        notes=notes,
        tags=tag_list
    )
    manager.add_rating(user_rating)
    click.echo(f"Added rating for session {session_id}")

@cli.command()
@click.argument('session_id')
@click.pass_obj
def show_ratings(manager: UserDataManager, session_id: str):
    """Show all ratings for a session."""
    ratings = manager.get_ratings(session_id)
    if not ratings:
        click.echo(f"No ratings found for session {session_id}")
        return
    
    click.echo(f"\nRatings for session {session_id}:")
    for rating in ratings:
        click.echo(f"Rating: {rating.rating}/5")
        if rating.notes:
            click.echo(f"Notes: {rating.notes}")
        if rating.tags:
            click.echo(f"Tags: {', '.join(rating.tags)}")
        click.echo("---")

@cli.command()
@click.argument('session_id')
@click.pass_obj
def show_tags(manager: UserDataManager, session_id: str):
    """Show all tags for a session."""
    tags = manager.get_session_tags(session_id)
    if not tags:
        click.echo(f"No tags found for session {session_id}")
        return
    
    click.echo(f"\nTags for session {session_id}:")
    for tag in tags:
        click.echo(f"- {tag}")

@cli.command()
@click.pass_obj
def list_tags(manager: UserDataManager):
    """List all tags and their usage count."""
    tag_counts = manager.get_all_tags()
    if not tag_counts:
        click.echo("No tags found")
        return
    
    click.echo("\nAll tags and their usage count:")
    for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True):
        click.echo(f"{tag}: {count}")

@cli.command()
@click.argument('session_id')
@click.argument('user_id')
@click.pass_obj
def delete_rating(manager: UserDataManager, session_id: str, user_id: str):
    """Delete a rating for a session."""
    manager.delete_rating(session_id, user_id)
    click.echo(f"Deleted rating for session {session_id}")

@cli.command()
@click.argument('session_id')
@click.argument('rating', type=click.IntRange(1, 5))
@click.option('--notes', help='Additional notes about the session')
@click.option('--tags', help='Comma-separated list of tags')
@click.option('--user-id', required=True, help='User ID of the rating to update')
@click.pass_obj
def update_rating(manager: UserDataManager, session_id: str, rating: int, notes: Optional[str], 
                 tags: Optional[str], user_id: str):
    """Update an existing rating."""
    tag_list = [t.strip() for t in tags.split(',')] if tags else []
    user_rating = UserRating(
        session_id=session_id,
        rating=rating,
        notes=notes,
        tags=tag_list,
        user_id=user_id
    )
    manager.update_rating(user_rating)
    click.echo(f"Updated rating for session {session_id}")

if __name__ == "__main__":
    app() 