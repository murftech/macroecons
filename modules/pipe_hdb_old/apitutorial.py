import requests

# Define the URL of the Pokémon you want to fetch data for
url = 'https://pokeapi.co/api/v2/pokemon/pikachu'

# Send a GET request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    print(f"Name: {data['name']}")
    print(f"Height: {data['height']} decimeters")
    print(f"Weight: {data['weight']} hectograms")
    print(f"Types: {[type['type']['name'] for type in data['types']]}")
else:
    print(f"Error: Unable to retrieve data (Status Code: {response.status_code})")

type(data)

len(data)

keys_list = list(data.keys())
keys_list

data['abilities']

# Access the list of abilities and extract the ability names
abilities_list = [ability['ability']['name'] for ability in data['abilities']]

# Print the list of abilities
print(abilities_list)