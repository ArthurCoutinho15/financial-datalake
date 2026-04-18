import pytest

API_URL = "api/v1/clients/"


@pytest.mark.asyncio
async def test_create_client(client):
    payload = {
        "name": "Teste",
        "email": "teste@email.com",
        "cpf": "11111111111",
        "phone": "111111111111",
        "city": "Teste",
        "state": "TT",
    }

    response = await client.post(API_URL, json=payload)

    assert response.status_code == 201

    data = response.json()

    print(data)

    assert data["name"] == "Teste"
    assert data["email"] == "teste@email.com"


@pytest.mark.asyncio
async def test_get_clients(client):
    response = await client.get(API_URL)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_client(client):
    payload = {
        "name": "Teste",
        "email": "teste@email.com",
        "cpf": "11111111111",
        "phone": "111111111111",
        "city": "Teste",
        "state": "TT",
    }

    created_client = await client.post(API_URL, json=payload)
    print(created_client.json())
    client_id = created_client.json()["id"]

    response = await client.get(f"{API_URL}{client_id}")

    assert response.status_code == 200
    assert response.json()["name"] == "Teste"


@pytest.mark.asyncio
async def test_get_client_not_found(client):
    response = await client.get("/3fa8")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_put_client(client):
    payload = {
        "name": "Teste",
        "email": "teste@email.com",
        "cpf": "11111111111",
        "phone": "111111111111",
        "city": "Teste",
        "state": "TT",
    }

    created_client = await client.post(API_URL, json=payload)
    print(created_client.json())
    client_id = created_client.json()["id"]

    update = {"name": "Arthur"}

    response = await client.put(f"{API_URL}{client_id}", json=update)

    assert response.status_code == 202
    assert response.json()["name"] == "Arthur"


@pytest.mark.asyncio
async def test_delete_client(client):
    payload = {
        "name": "Teste",
        "email": "teste@email.com",
        "cpf": "11111111111",
        "phone": "111111111111",
        "city": "Teste",
        "state": "TT",
    }

    created_client = await client.post(API_URL, json=payload)
    print(created_client.json())
    client_id = created_client.json()["id"]

    response = await client.delete(f"{API_URL}{client_id}")

    assert response.status_code == 204
