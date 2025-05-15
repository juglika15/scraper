# models.py

from dataclasses import dataclass
from typing import List, Tuple

@dataclass
class MovieLink:
    url: str

@dataclass
class MovieDetails:
    url: str
    title: str
    poster_url: str
    genre: List[str]
    studio: str
    year: int
    directors: List[str]
    length: str
    countries: List[str]
    budget: str
    box_office: str
    plot: str
    actors: List[Tuple[str, str]]  # each tuple is (actor_name, image_url)
    api_url: str
