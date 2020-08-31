from dataclasses import dataclass


# Inspired by schema.org
@dataclass
class Thing:
    id: str


@dataclass
class Person(Thing):
    title: str
    first_name: str
    last_name: str
    age: int
    gender: str


@dataclass
class Transaction(Thing):
    person_id: str
    item_id: str


@dataclass
class Item(Thing):
    price: float
    category: str
