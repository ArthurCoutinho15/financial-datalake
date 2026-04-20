import pytest
from datetime import datetime

API_URL = "api/v1/transactions/"


@pytest.mark.asyncio
async def test_create_transaction(client):
    payload = {
        "position_id": "44c37ab6-5f2c-4e41-a431-cf063cac22d3",
        "type": "buy",
        "quantity": 100.5,
        "price_brl": 829.340823,
        "executed_at": "2026-04-20T10:00:00"
    }

    response = await client.post(API_URL, json=payload)

    assert response.status_code == 201

    data = response.json()

    assert data["position_id"] == "44c37ab6-5f2c-4e41-a431-cf063cac22d3"
    assert data["type"] == "buy"


@pytest.mark.asyncio
async def test_get_transactions(client):
    response = await client.get(API_URL)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_transaction(client):
    payload = {
        "position_id": "44c37ab6-5f2c-4e41-a431-cf063cac22d3",
        "type": "buy",
        "quantity": 100.5,
        "price_brl": 829.340823,
        "executed_at": "2026-04-20T10:00:00"
    }

    created_transaction = await client.post(API_URL, json=payload)
    transaction_id = created_transaction.json()["id"]

    response = await client.get(f"{API_URL}{transaction_id}")

    assert response.status_code == 200
    assert response.json()["type"] == "buy"


@pytest.mark.asyncio
async def test_get_transaction_not_found(client):
    response = await client.get(f"{API_URL}44c37ab6-5f2c-4e41-a431-cf063cac22d2")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_put_transaction(client):
    payload = {
        "position_id": "44c37ab6-5f2c-4e41-a431-cf063cac22d3",
        "type": "buy",
        "quantity": 100.5,
        "price_brl": 829.340823,
        "executed_at": "2026-04-20T10:00:00"
    }

    created_transaction = await client.post(API_URL, json=payload)
    transaction_id = created_transaction.json()["id"]

    update = {
        "type": "sell",
        "quantity": 50.0,
        "price_brl": 900.0,
        "executed_at": "2026-04-21T10:00:00"
    }

    response = await client.put(f"{API_URL}{transaction_id}", json=update)

    assert response.status_code == 202
    assert response.json()["type"] == "sell"


@pytest.mark.asyncio
async def test_delete_transaction(client):
    payload = {
        "position_id": "44c37ab6-5f2c-4e41-a431-cf063cac22d3",
        "type": "buy",
        "quantity": 100.5,
        "price_brl": 829.340823,
        "executed_at": "2026-04-20T10:00:00"
    }

    created_transaction = await client.post(API_URL, json=payload)
    transaction_id = created_transaction.json()["id"]

    response = await client.delete(f"{API_URL}{transaction_id}")

    assert response.status_code == 204
