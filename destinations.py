"""Bristol Airport (BRS) destinations — verified via Google Flights."""

DESTINATIONS = {
    "BRS": {
        "name": "Bristol",
        "routes": {
            "ACE": "Lanzarote",
            "AGP": "Malaga",
            "ALC": "Alicante",
            "AMS": "Amsterdam",
            "AYT": "Antalya",
            "BCN": "Barcelona",
            "BER": "Berlin",
            "BFS": "Belfast",
            "BUD": "Budapest",
            "CDG": "Paris CDG",
            "CFU": "Corfu",
            "DBV": "Dubrovnik",
            "DLM": "Dalaman",
            "DUB": "Dublin",
            "EDI": "Edinburgh",
            "FAO": "Faro",
            "FCO": "Rome",
            "FUE": "Fuerteventura",
            "GLA": "Glasgow",
            "GVA": "Geneva",
            "HER": "Heraklion",
            "IBZ": "Ibiza",
            "JER": "Jersey",
            "KRK": "Krakow",
            "LIS": "Lisbon",
            "LPA": "Gran Canaria",
            "MLA": "Malta",
            "NCL": "Newcastle",
            "PFO": "Paphos",
            "PMI": "Palma",
            "PRG": "Prague",
            "RAK": "Marrakech",
            "RHO": "Rhodes",
            "SOF": "Sofia",
            "SPU": "Split",
            "TFS": "Tenerife",
            "VRN": "Verona",
            "ZTH": "Zakynthos",
        },
    },
}


def get_destinations(airport: str) -> dict:
    entry = DESTINATIONS.get(airport, {})
    return entry.get("routes", {})


def get_airport_name(airport: str) -> str:
    entry = DESTINATIONS.get(airport, {})
    return entry.get("name", airport)
