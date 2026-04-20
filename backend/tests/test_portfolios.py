import pytest

API_URL = "api/v1/portfolios/"


@pytest.mark.asyncio
async def test_create_portfolio(client):
    payload = {
        "client_id": "8f9510f8-a628-4c3a-9943-deab3c6a2b6b",
        "name": "Internacional",
    }

    response = await client.post(API_URL, json=payload)

    assert response.status_code == 201

    data = response.json()

    assert data["client_id"] == "8f9510f8-a628-4c3a-9943-deab3c6a2b6b"
    assert data["name"] == "Internacional"


@pytest.mark.asyncio
async def test_get_portfolios(client):
    response = await client.get(API_URL)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_portfolio(client):
    payload = {
        "client_id": "8f9510f8-a628-4c3a-9943-deab3c6a2b6b",
        "name": "Internacional",
    }

    created_portfolio = await client.post(API_URL, json=payload)
    print(created_portfolio.json())
    portfolio_id = created_portfolio.json()["id"]

    response = await client.get(f"{API_URL}{portfolio_id}")

    assert response.status_code == 200
    assert response.json()["name"] == "Internacional"


@pytest.mark.asyncio
async def test_get_portfolio_not_found(client):
    response = await client.get(f"{API_URL}8f9510f8-a528-4c3a-9943-deab3c6a2b6b")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_put_portfolio(client):
    payload = {
        "client_id": "8f9510f8-a628-4c3a-9943-deab3c6a2b6b",
        "name": "Internacional",
    }

    portfolio_created = await client.post(API_URL, json=payload)

    portfolio_id = portfolio_created.json()["id"]

    update = {"name": "Arrojado"}

    response = await client.put(f"{API_URL}{portfolio_id}", json=update)

    assert response.status_code == 202
    assert response.json()["name"] == "Arrojado"


@pytest.mark.asyncio
async def test_delete_portfolio(client):
    payload = {
        "client_id": "8f9510f8-a628-4c3a-9943-deab3c6a2b6b",
        "name": "Internacional",
    }

    portfolio_created = await client.post(API_URL, json=payload)

    portfolio_id = portfolio_created.json()["id"]

    response = await client.delete(f"{API_URL}{portfolio_id}")

    assert response.status_code == 204
