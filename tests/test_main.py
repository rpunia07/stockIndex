import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
import pandas as pd

from app.main import app
from app.database import Database
from app.data_fetcher import DataFetcher

@pytest.fixture
def test_client():
    return TestClient(app)

@pytest.fixture
def mock_db():
    # Create a test database
    db = Database("test_market_data.db")
    yield db
    db.close()

def test_build_index(test_client):
    # Test building index for a date range
    response = test_client.post(
        "/build-index",
        params={
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        }
    )
    assert response.status_code == 200
    assert response.json()["message"] == "Index built successfully"

def test_get_index_performance(test_client):
    # Test getting index performance
    response = test_client.get(
        "/index-performance",
        params={
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:  # If there's data
        assert all(isinstance(item.get("daily_return"), (int, float)) for item in data)

def test_get_index_composition(test_client):
    # Test getting index composition
    response = test_client.get(
        "/index-composition",
        params={"date": "2023-01-01"}
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:  # If there's data
        assert all(isinstance(item.get("weight"), (int, float)) for item in data)

def test_data_fetcher():
    fetcher = DataFetcher()
    symbols = asyncio.run(fetcher.fetch_symbols())
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert all(isinstance(s, str) for s in symbols)

def test_database_operations(mock_db):
    # Test inserting and retrieving market data
    test_data = [{
        "date": "2023-01-01",
        "symbol": "TEST",
        "price": 100.0,
        "market_cap": 1000000.0,
        "volume": 1000
    }]
    
    mock_db.insert_market_data(test_data)
    
    # Test building index
    mock_db.build_index("2023-01-01", "2023-01-01")
    
    # Test getting composition
    composition = mock_db.get_composition("2023-01-01")
    assert isinstance(composition, list)

def test_export_data(test_client):
    # Test data export functionality
    response = test_client.post(
        "/export-data",
        params={
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        }
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
