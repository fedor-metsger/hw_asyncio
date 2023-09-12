
import asyncio

import aiohttp
from more_itertools import chunked

from models import Session, SwapiPeople, engine

MAX_REQUESTS_CHUNK = 5

async def get_person_planet(hw_url):
    session = aiohttp.ClientSession()
    response = await session.get(hw_url)
    json_data = await response.json()
    await session.close()
    return json_data["name"]

async def get_person_films(film_url_list):
    session = aiohttp.ClientSession()
    films = []
    for url in film_url_list:
        response = await session.get(url)
        json_data = await response.json()
        films.append(json_data["title"])
    await session.close()
    return str(films)[1:-1]

async def get_person_vehicles(vehicle_url_list):
    session = aiohttp.ClientSession()
    vehicles = []
    for url in vehicle_url_list:
        response = await session.get(url)
        json_data = await response.json()
        vehicles.append(json_data["name"])
    await session.close()
    return str(vehicles)[1:-1]

async def get_person_species(specie_url_list):
    session = aiohttp.ClientSession()
    species = []
    for url in specie_url_list:
        response = await session.get(url)
        json_data = await response.json()
        species.append(json_data["name"])
    await session.close()
    return str(species)[1:-1]

async def get_person_starships(starship_url_list):
    session = aiohttp.ClientSession()
    starships = []
    for url in starship_url_list:
        response = await session.get(url)
        json_data = await response.json()
        starships.append(json_data["name"])
    await session.close()
    return str(starships)[1:-1]

async def insert_people(people_list_json):
    for i in range(len(people_list_json) - 1, -1, -1):
        if not "name" in people_list_json[i]:
            del people_list_json[i]
    if not people_list_json:
        return
    people_list = []
    for person in people_list_json:
        hw = await get_person_planet(person["homeworld"])
        films = await get_person_films(person["films"])
        vehicles = await get_person_vehicles(person["vehicles"])
        species = await get_person_species(person["species"])
        starships = await get_person_starships(person["starships"])
        people_list.append(SwapiPeople(
            birth_year = person["birth_year"],
            eye_color = person["eye_color"],
            films = films,
            gender = person["gender"],
            hair_color = person["hair_color"],
            height = person["height"],
            homeworld = hw,
            mass = person["mass"],
            name = person["name"],
            skin_color = person["skin_color"],
            species = species,
            starships = starships,
            vehicles = vehicles
        ))
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_people(people_id):

    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.py4e.com/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data

async def main():
    engine.begin()
    for person_ids_chunk in chunked(range(1, 100), MAX_REQUESTS_CHUNK):
        person_coros = [get_people(person_id) for person_id in person_ids_chunk]
        people = await asyncio.gather(*person_coros)
        insert_people_coro = insert_people(people)
        asyncio.create_task(insert_people_coro)

    main_task = asyncio.current_task()
    insets_tasks = asyncio.all_tasks() - {main_task}
    await asyncio.gather(*insets_tasks)

if __name__ == "__main__":
    asyncio.run(main())
