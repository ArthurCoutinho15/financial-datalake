import pytest 

API_URL = "api/v1/positions/"

@pytest.mark.asyncio
async def test_create_position(client):
    payload = {
        "portfolio_id": "44c37ab6-5f2c-4e41-a431-cf063cac22d3",
        "ticker": "AMZN",
        "asset_type": "stock",
        "quantity": 913.022164,
        "avg_price_brl": 829.340823
    }
    
    response = await client.post(API_URL, json=payload)
    
    assert response.status_code == 201 
    
    data = response.json()
    
    assert data["portfolio_id"] == "44c37ab6-5f2c-4e41-a431-cf063cac22d3"
    assert data["ticker"] == "AMZN"
    
@pytest.mark.asyncio 
async def test_get_positions(client):
    response = await client.get(API_URL)
    
    assert response.status_code == 200 
    assert isinstance(response.json(), list)
    
@pytest.mark.asyncio
async def test_get_portfolio(client):
    payload = {
        "portfolio_id": "44c37ab6-5f2c-4e41-a431-cf063cac22d3",
        "ticker": "AMZN",
        "asset_type": "stock",
        "quantity": 913.022164,
        "avg_price_brl": 829.340823
    }
    
    created_position = await client.post(API_URL, json=payload)
    position_id = created_position.json()["id"]
    
    response = await client.get(f"{API_URL}{position_id}")
    
    assert response.status_code == 200
    assert response.json()["ticker"] == "AMZN"

@pytest.mark.asyncio
async def test_get_position_not_found(client):
    response = await client.get(f"{API_URL}44c37ab6-5f2c-4e41-a431-cf063cac22d2")
    
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_put_position(client):
    payload = {
        "portfolio_id": "44c37ab6-5f2c-4e41-a431-cf063cac22d3",
        "ticker": "AMZN",
        "asset_type": "stock",
        "quantity": 913.022164,
        "avg_price_brl": 829.340823
    }
    
    created_position = await client.post(API_URL, json=payload)
    position_id = created_position.json()["id"]
    
    update = {"ticker": "META"}
    
    response = await client.put(f"{API_URL}{position_id}", json=update)
    
    assert response.status_code == 202
    assert response.json()["ticker"] == "META"

@pytest.mark.asyncio
async def test_delete_position(client):
    payload = {
        "portfolio_id": "44c37ab6-5f2c-4e41-a431-cf063cac22d3",
        "ticker": "AMZN",
        "asset_type": "stock",
        "quantity": 913.022164,
        "avg_price_brl": 829.340823
    }
    
    created_position = await client.post(API_URL, json=payload)
    position_id = created_position.json()["id"]
    
    response = await client.delete(f"{API_URL}{position_id}")
    
    assert response.status_code == 204