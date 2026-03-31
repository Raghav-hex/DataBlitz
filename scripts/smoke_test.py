"""Live API smoke test — FRED, NOAA, BLS (no key needed)."""
import asyncio
import httpx
import os
import sys
sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()


async def test_fred(client):
    key = os.getenv("FRED_API_KEY", "")
    if not key:
        return "SKIP (no key)"
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "UNRATE", "api_key": key,
        "file_type": "json", "limit": 3, "sort_order": "desc",
    }
    r = await client.get(url, params=params)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    return [(o["date"], o["value"]) for o in obs if o["value"] != "."]


async def test_noaa(client):
    token = os.getenv("NOAA_CDO_TOKEN", "")
    if not token:
        return "SKIP (no token)"
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    params = {
        "datasetid": "GSOM", "datatypeid": "TAVG",
        "stationid": "GHCND:USW00094728",
        "startdate": "2024-01-01", "enddate": "2024-06-01",
        "limit": 3, "units": "metric", "includemetadata": "false",
    }
    r = await client.get(url, params=params, headers={"token": token})
    r.raise_for_status()
    results = r.json().get("results", [])
    return [(res["date"][:7], res["value"]) for res in results]


async def test_bls(client):
    url = "https://api.bls.gov/publicAPI/v1/timeseries/data/LNS14000000"
    r = await client.get(url)
    r.raise_for_status()
    data = r.json().get("Results", {}).get("series", [{}])[0].get("data", [])[:3]
    return [(d["year"] + "-" + d["period"], d["value"] + "%") for d in data]


async def test_bcb(client):
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados"
    params = {"formato": "json", "dataInicial": "01/01/2024", "dataFinal": "01/06/2024"}
    r = await client.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    return [(d["data"], d["valor"]) for d in data[:3]]


async def test_worldbank(client):
    url = "https://api.worldbank.org/v2/country/IN/indicator/NY.GDP.MKTP.KD.ZG"
    params = {"format": "json", "mrv": 3, "per_page": 3}
    r = await client.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    rows = data[1] if len(data) > 1 else []
    return [(row["date"], row["value"]) for row in rows if row.get("value") is not None]


async def main():
    print("\n" + "=" * 55)
    print("  DataBlitz — Live API Smoke Tests")
    print("=" * 55)

    tests = [
        ("FRED (USA economic)", test_fred),
        ("NOAA (USA climate)", test_noaa),
        ("BLS (USA labor, no key)", test_bls),
        ("BCB (Brazil SELIC, no key)", test_bcb),
        ("World Bank India GDP (no key)", test_worldbank),
    ]

    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        for name, fn in tests:
            try:
                result = await fn(client)
                if isinstance(result, str):
                    print(f"  ⏭  {name}: {result}")
                else:
                    print(f"  ✓  {name}: {result[:2]}...")
            except Exception as exc:
                print(f"  ✗  {name}: {exc}")

    print("=" * 55 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
