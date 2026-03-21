"""Airport destination lists for UK airports."""

DESTINATIONS = {
    "BRS": {
        "name": "Bristol",
        "routes": {
            "AGP": "Malaga",
            "ALC": "Alicante",
            "ACE": "Lanzarote",
            "ATH": "Athens",
            "BCN": "Barcelona",
            "BFS": "Belfast",
            "BHD": "Belfast City",
            "CDG": "Paris CDG",
            "CFU": "Corfu",
            "DBV": "Dubrovnik",
            "DUB": "Dublin",
            "EDI": "Edinburgh",
            "FAO": "Faro",
            "FCO": "Rome",
            "FNC": "Funchal",
            "FUE": "Fuerteventura",
            "GLA": "Glasgow",
            "GNB": "Grenoble",
            "GVA": "Geneva",
            "HRG": "Hurghada",
            "INN": "Innsbruck",
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
            "SPU": "Split",
            "SSH": "Sharm el Sheikh",
            "SZG": "Salzburg",
            "TFS": "Tenerife",
            "TIA": "Tirana",
            "VRN": "Verona",
        },
    },
}


def get_destinations(airport: str) -> dict:
    """Get the destination dict for an airport."""
    entry = DESTINATIONS.get(airport, {})
    return entry.get("routes", {})


def get_airport_name(airport: str) -> str:
    """Get the display name for an airport."""
    entry = DESTINATIONS.get(airport, {})
    return entry.get("name", airport)
