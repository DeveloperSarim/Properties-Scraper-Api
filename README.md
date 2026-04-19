# 🏠 Saudi Property Scraper API

<div align="center">

[![FastAPI](https://img.shields.io/badge/FastAPI-0.111.0-009688?style=for-the-badge&logo=fastapi)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=for-the-badge)](https://github.com/DeveloperSarim/Properties-Scraper-Api)

**Real-time Property Scraping API for Saudi Arabia Markets** 🇸🇦

[Features](#-features) • [Quick Start](#-quick-start) • [Documentation](#-api-documentation) • [Deployment](#-deployment) • [Contributing](#-contributing)

</div>

---

## 📋 Table of Contents

- [🎯 Overview](#overview)
- [✨ Features](#-features)
- [🛠️ Tech Stack](#️-tech-stack)
- [📦 Installation](#-installation)
- [⚙️ Configuration](#️-configuration)
- [🚀 Quick Start](#-quick-start)
- [📚 API Documentation](#-api-documentation)
- [🌐 Deployment Guide](#-deployment)
- [👨‍💻 Development Guide](#️-development-guide)
- [📖 Examples](#-examples)
- [🤝 Contributing](#-contributing)
- [📝 License](#-license)
- [💬 Support](#-support)

---

## 🎯 Overview

**Saudi Property Scraper API** is a high-performance, asynchronous web scraping API built with **FastAPI** that aggregates property listings from major Saudi Arabian real estate platforms including **Bayut**, **Aqar**, **PropertyFinder**, **Wasalt**, and 10+ additional marketplace platforms. 

Designed for developers and real estate professionals, this API provides real-time access to comprehensive property data across all major Saudi cities, enabling you to build powerful real estate applications, market analysis tools, and property comparison platforms.

**Key Highlights:**
- ⚡ **Ultra-Fast**: Asynchronous processing with streaming responses
- 🔄 **Multi-Platform**: Aggregate from 13+ verified real estate platforms (Bayut, Aqar, PropertyFinder, Wasalt, Sakani, Haraj, OpenSooq, and more)
- 🌍 **Nationwide Coverage**: Access properties across all 20+ Saudi cities
- 🔐 **Secure**: CORS-enabled with built-in security headers
- 📊 **Real-time Data**: Live property listings with instant updates
- 🚀 **Production-Ready**: Enterprise-grade performance and reliability

---

## ✨ Features

### 🔍 Core Features

| Feature | Description | Status |
|---------|-------------|--------|
| **Multi-Platform Scraping** | Aggregate from 13+ real estate platforms | ✅ Active |
| **City-Based Search** | Support for 20+ Saudi cities | ✅ Implemented |
| **District Filtering** | Search by specific neighborhoods | ✅ Active |
| **Real-time Data** | Live property listings updates | ✅ Active |
| **Streaming API** | Efficient streaming responses | ✅ Optimized |
| **CORS Support** | Cross-origin resource sharing enabled | ✅ Enabled |
| **Async Processing** | High-concurrency request handling | ✅ Optimized |
| **Error Handling** | Comprehensive error management | ✅ Robust |
| **Rate Limiting Ready** | Built for high-volume requests | ✅ Ready |
| **Documentation** | Interactive API documentation | ✅ Built-in |

### 🏙️ Supported Cities

**20 Major Cities Including:**
- 🏛️ **Riyadh** (Largest market)
- 🌊 **Jeddah** (Red Sea coast)
- 📍 **Dammam** (Eastern Province)
- 🕌 **Mecca** & **Medina**
- 🏔️ **Abha**, **Taif**
- ⛽ **Al Khobar**, **Dhahran**
- 🏗️ **Al Jubail**, **Yanbu**
- ✈️ **Tabuk**, **Buraidah**, **Hail**, **Khamis Mushait**, **Najran**, **Jazan**
- 🌅 **Al Ahsa**, **Al Qatif**, **Al Ula**, + more...

### 📊 Property Types & Platforms

**Property Types:** Apartments, Villas, Houses, Land, Offices, Commercial Spaces

**13+ Supported Platforms:**
1. 🏛️ **Bayut** - Primary marketplace (via Algolia API)
2. 🔍 **Aqar** - Arabic platform with RSC streaming
3. 🏘️ **PropertyFinder** - Major listing portal
4. 🌐 **Wasalt** - Next.js based platform
5. 🏠 **Sakani** - Government housing platform
6. 🎯 **Haraj** - Classified marketplace
7. 🛒 **OpenSooq** - Multi-category platform
8. 🌍 **Expatriates** - Expat-focused listings
9. 📱 **Mourjan** - Community marketplace
10. 🏢 **Satel** - Commercial compounds
11. 🔗 **Zaahib** - Property aggregator
12. 📍 **Bezaat** - Real estate portal
13. 🎁 **SaudiDeal** - Deal marketplace

---

## 🛠️ Tech Stack

### Backend Framework
- **FastAPI** v0.111.0 - Modern, fast web framework
- **Uvicorn** v0.29.0 - ASGI web server

### Web Scraping & HTTP
- **curl_cffi** v0.7.4 - Advanced HTTP requests with browser fingerprinting
- **BeautifulSoup4** v4.12.3 - HTML parsing and extraction
- **HTTPX** v0.27.0 - Asynchronous HTTP client
- **lxml** v5.2.1 - XML/HTML processing

### Utilities
- **python-dotenv** v1.0.1 - Environment variable management

### Deployment Options
- Docker & Docker Compose
- Cloud Platforms (AWS, Google Cloud, Azure, Heroku)
- Virtual Private Servers (VPS)
- Kubernetes

---

## 📦 Installation

### Prerequisites

- **Python 3.8+** or **Python 3.11+** (Recommended)
- **pip** (Python package manager)
- **Git** (for cloning the repository)
- **Virtual Environment** (recommended)

### Step 1: Clone Repository

```bash
# Clone the repository
git clone https://github.com/DeveloperSarim/Properties-Scraper-Api.git
cd Properties-Scraper-Api

# Or use HTTPS if SSH is not configured
git clone https://github.com/DeveloperSarim/Properties-Scraper-Api.git
```

### Step 2: Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/macOS
source venv/bin/activate

# On Windows
venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Install all required packages
pip install -r requirements.txt
```

### Step 4: Verify Installation

```bash
# Check if FastAPI is installed correctly
python -c "import fastapi; print(f'FastAPI v{fastapi.__version__}')"
```

---

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# FastAPI Configuration
APP_NAME=Saudi Property Scraper API
APP_VERSION=2.0.0
DEBUG=True

# Server Configuration
HOST=0.0.0.0
PORT=8000

# Scraping Configuration
TIMEOUT=30
MAX_RETRIES=3
USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36

# API Configuration
CORS_ORIGINS=["*"]
CORS_ALLOW_CREDENTIALS=True
CORS_ALLOW_METHODS=["*"]
CORS_ALLOW_HEADERS=["*"]

# Logging
LOG_LEVEL=INFO
```

### Load Environment Variables

```python
from dotenv import load_dotenv
import os

load_dotenv()

DEBUG = os.getenv("DEBUG", "True")
PORT = int(os.getenv("PORT", 8000))
```

---

## 🚀 Quick Start

### Running the API Server

```bash
# Activate virtual environment (if not already activated)
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate     # Windows

# Run the development server
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Expected Output:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete
```

### Access the API

- 🌐 **API Endpoint**: http://localhost:8000
- 📚 **Interactive Docs (Swagger UI)**: http://localhost:8000/docs
- 🔄 **Alternative Docs (ReDoc)**: http://localhost:8000/redoc
- 📋 **OpenAPI Schema**: http://localhost:8000/openapi.json

### Test the API

```bash
# Using curl
curl "http://localhost:8000/properties?city=riyadh&limit=10"

# Using Python
import httpx
async with httpx.AsyncClient() as client:
    response = await client.get("http://localhost:8000/properties")
    print(response.json())
```

---

## 📚 API Documentation

### Base URL
```
http://localhost:8000
```

### Available Endpoints

#### 1. Search Properties by City

**Endpoint:** `GET /properties`

**Query Parameters:**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `city` | string | City name (case-insensitive) | `riyadh`, `jeddah` |
| `limit` | integer | Number of results to return | `10`, `50` |
| `offset` | integer | Pagination offset | `0`, `10` |
| `district` | string (optional) | Specific district within city | `Al Malaz`, `Al Olaya` |
| `price_min` | integer (optional) | Minimum price in SAR | `100000` |
| `price_max` | integer (optional) | Maximum price in SAR | `1000000` |

**Example Requests:**

```bash
# Search properties in Riyadh (first 10 results)
curl "http://localhost:8000/properties?city=riyadh&limit=10"

# Search in specific district
curl "http://localhost:8000/properties?city=riyadh&district=Al%20Malaz&limit=20"

# With pagination
curl "http://localhost:8000/properties?city=jeddah&limit=25&offset=0"

# Price range filter
curl "http://localhost:8000/properties?city=riyadh&price_min=500000&price_max=2000000"
```

**Response Format (JSON):**

```json
{
  "status": "success",
  "city": "riyadh",
  "total_results": 1250,
  "returned": 10,
  "data": [
    {
      "id": "prop_12345",
      "title": "Modern Villa in Al Malaz",
      "city": "Riyadh",
      "district": "Al Malaz",
      "price": 1500000,
      "currency": "SAR",
      "property_type": "Villa",
      "bedrooms": 4,
      "bathrooms": 3,
      "area": 250,
      "area_unit": "sqm",
      "description": "Spacious modern villa...",
      "images": ["url1", "url2"],
      "location": {
        "latitude": 24.7136,
        "longitude": 46.6753
      },
      "posted_date": "2024-04-15T10:30:00Z",
      "source_url": "https://bayut.com/...",
      "source": "Bayut"
    }
  ]
}
```

#### 2. Search by District

**Endpoint:** `GET /properties/district/{city}/{district}`

```bash
curl "http://localhost:8000/properties/district/riyadh/Al%20Malaz"
```

#### 3. Nearby Properties

**Endpoint:** `GET /properties/nearby`

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `latitude` | float | Latitude coordinate |
| `longitude` | float | Longitude coordinate |
| `radius_km` | float | Search radius in kilometers |
| `limit` | integer | Maximum results |

```bash
curl "http://localhost:8000/properties/nearby?latitude=24.7136&longitude=46.6753&radius_km=5&limit=20"
```

#### 4. Stream Properties (Streaming Response)

**Endpoint:** `GET /stream/properties`

Supports real-time streaming of properties as they're being scraped.

```bash
curl "http://localhost:8000/stream/properties?city=riyadh&limit=100"
```

### Error Responses

```json
{
  "status": "error",
  "error_code": "INVALID_CITY",
  "message": "City 'unknown' is not supported",
  "supported_cities": ["riyadh", "jeddah", ...]
}
```

**Common Error Codes:**
- `400` - Bad Request (invalid parameters)
- `404` - City/Resource Not Found
- `429` - Rate Limited
- `500` - Internal Server Error

---

## 🌐 Deployment

### 📦 Option 1: Docker Deployment (Recommended)

#### Step 1: Create Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8000/health')"

# Run application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

#### Step 2: Create docker-compose.yml

```yaml
version: '3.8'

services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DEBUG=False
      - LOG_LEVEL=INFO
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    volumes:
      - ./logs:/app/logs
```

#### Step 3: Build and Run

```bash
# Build Docker image
docker build -t properties-scraper-api:latest .

# Run with Docker
docker run -d \
  --name properties-api \
  -p 8000:8000 \
  properties-scraper-api:latest

# Or use Docker Compose
docker-compose up -d

# View logs
docker logs -f properties-api
```

---

### ☁️ Option 2: Heroku Deployment

#### Step 1: Create Procfile

```
web: uvicorn main:app --host 0.0.0.0 --port $PORT
```

#### Step 2: Create runtime.txt

```
python-3.11.9
```

#### Step 3: Deploy to Heroku

```bash
# Login to Heroku
heroku login

# Create Heroku app
heroku create your-app-name

# Add buildpack
heroku buildpacks:add heroku/python

# Deploy
git push heroku main

# View logs
heroku logs --tail
```

---

### ☁️ Option 3: AWS EC2 Deployment

#### Step 1: Launch EC2 Instance

```bash
# SSH into your EC2 instance
ssh -i your-key.pem ubuntu@your-ec2-ip.compute.amazonaws.com

# Update system
sudo apt update && sudo apt upgrade -y

# Install Python and pip
sudo apt install -y python3.11 python3.11-venv python3-pip

# Install Git
sudo apt install -y git
```

#### Step 2: Clone and Setup

```bash
# Clone repository
git clone https://github.com/DeveloperSarim/Properties-Scraper-Api.git
cd Properties-Scraper-Api

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

#### Step 3: Setup Systemd Service

```bash
# Create service file
sudo nano /etc/systemd/system/properties-api.service
```

**Content:**

```ini
[Unit]
Description=Saudi Property Scraper API
After=network.target

[Service]
Type=notify
User=ubuntu
WorkingDirectory=/home/ubuntu/Properties-Scraper-Api
Environment="PATH=/home/ubuntu/Properties-Scraper-Api/venv/bin"
ExecStart=/home/ubuntu/Properties-Scraper-Api/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable properties-api
sudo systemctl start properties-api

# Check status
sudo systemctl status properties-api
```

---

### ☁️ Option 4: Google Cloud Platform (GCP)

#### Step 1: Create Cloud Run Service

```bash
# Authenticate with GCP
gcloud auth login

# Create project if needed
gcloud projects create properties-scraper-api

# Deploy to Cloud Run
gcloud run deploy properties-api \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

---

### ☁️ Option 5: DigitalOcean App Platform

```bash
# Create app.yaml
cat > app.yaml <<EOF
name: properties-scraper-api
services:
- name: api
  github:
    repo: your-username/Properties-Scraper-Api
    branch: main
  build_command: pip install -r requirements.txt
  run_command: uvicorn main:app --host 0.0.0.0 --port 8080
  envs:
  - key: DEBUG
    value: "False"
EOF

# Deploy
doctl apps create --spec app.yaml
```

---

### 🔒 Production Configuration

#### SSL/HTTPS with Nginx

```nginx
server {
    listen 443 ssl http2;
    server_name api.example.com;

    ssl_certificate /etc/letsencrypt/live/api.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

#### Enable SSL Certificate with Certbot

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot certonly --nginx -d api.example.com
```

---

## 👨‍💻 Development Guide

### Project Structure

```
Properties-Scraper-Api/
├── main.py                 # FastAPI application & main logic
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (create this)
├── .gitignore             # Git ignore file
├── README.md              # This file
├── Dockerfile             # Docker configuration
├── docker-compose.yml     # Docker Compose configuration
├── logs/                  # Application logs
├── venv/                  # Virtual environment
└── docs/                  # Documentation files
```

### Running in Development Mode

```bash
# With auto-reload and debug mode
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# With detailed logging
uvicorn main:app --reload --log-level debug
```

### Code Style & Linting

```bash
# Install development dependencies
pip install black flake8 mypy pytest

# Format code
black main.py

# Lint code
flake8 main.py

# Type checking
mypy main.py
```

### Testing

```bash
# Install pytest
pip install pytest pytest-asyncio httpx

# Create test_main.py
# Run tests
pytest -v

# With coverage
pip install pytest-cov
pytest --cov=main test_main.py
```

---

## 📖 Examples

### Example 1: Search Properties in Riyadh

```python
import httpx
import asyncio

async def search_riyadh():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "http://localhost:8000/properties",
            params={
                "city": "riyadh",
                "limit": 20
            }
        )
        properties = response.json()
        
        for prop in properties['data']:
            print(f"Title: {prop['title']}")
            print(f"Price: {prop['price']:,} SAR")
            print(f"Location: {prop['district']}, {prop['city']}")
            print("---")

asyncio.run(search_riyadh())
```

### Example 2: Search with Price Filter

```python
import httpx
import asyncio

async def search_with_price_filter():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "http://localhost:8000/properties",
            params={
                "city": "jeddah",
                "price_min": 500000,
                "price_max": 2000000,
                "limit": 50
            }
        )
        
        data = response.json()
        print(f"Found {data['total_results']} properties")

asyncio.run(search_with_price_filter())
```

### Example 3: Stream Properties in Real-time

```python
import httpx

def stream_properties():
    with httpx.stream(
        "GET",
        "http://localhost:8000/stream/properties",
        params={"city": "riyadh", "limit": 100}
    ) as response:
        for line in response.iter_lines():
            if line:
                print(f"Property: {line}")

stream_properties()
```

### Example 4: Find Nearby Properties

```python
import httpx
import asyncio

async def find_nearby():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "http://localhost:8000/properties/nearby",
            params={
                "latitude": 24.7136,
                "longitude": 46.6753,
                "radius_km": 5,
                "limit": 30
            }
        )
        
        nearby = response.json()
        for prop in nearby['data']:
            print(f"{prop['title']} - {prop['price']:,} SAR")

asyncio.run(find_nearby())
```

---

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

### Step 1: Fork the Repository

```bash
# Click "Fork" on GitHub
git clone https://github.com/YOUR_USERNAME/Properties-Scraper-Api.git
cd Properties-Scraper-Api
```

### Step 2: Create Feature Branch

```bash
git checkout -b feature/amazing-feature
```

### Step 3: Make Changes

```bash
# Make your improvements...
git add .
git commit -m "Add amazing feature"
```

### Step 4: Push and Create Pull Request

```bash
git push origin feature/amazing-feature
# Create Pull Request on GitHub
```

### Contribution Guidelines

- ✅ Write clear commit messages
- ✅ Add tests for new features
- ✅ Update documentation
- ✅ Follow PEP 8 style guide
- ✅ Add comments for complex logic
- ✅ Test thoroughly before submitting

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

```
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software")...
```

---

## 💬 Support

### Getting Help

- 📖 **Documentation**: Check [docs/](docs/) folder
- 🐛 **Bug Reports**: [Open an issue](https://github.com/DeveloperSarim/Properties-Scraper-Api/issues)
- 💡 **Feature Requests**: [Discussions](https://github.com/DeveloperSarim/Properties-Scraper-Api/discussions)
- 📧 **Email**: sarim.yaseen@aixsolutions.net

### Troubleshooting

#### ImportError: No module named 'fastapi'

```bash
# Ensure virtual environment is activated and dependencies installed
source venv/bin/activate
pip install -r requirements.txt
```

#### Connection Error on localhost:8000

```bash
# Check if port is in use
lsof -i :8000

# Use different port
uvicorn main:app --port 8001
```

#### Timeout Issues During Scraping

```env
# Increase timeout in .env
TIMEOUT=60
```

---

## 🚀 Performance Tips

### 1. Enable Caching

```python
from fastapi_cache2 import FastAPICache2
from fastapi_cache2.backends.redis import RedisBackend
```

### 2. Use Database for Results

Consider adding MongoDB or PostgreSQL for caching results.

### 3. Rate Limiting

```bash
pip install slowapi
```

### 4. Load Balancing

Use Nginx or HAProxy for load balancing across multiple instances.

---

## 📊 Analytics & Monitoring

### Health Check Endpoint

Add to main.py:

```python
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "version": "2.0.0"
    }
```

### Monitoring with PM2

```bash
npm install -g pm2

# Create ecosystem.config.js
cat > ecosystem.config.js <<EOF
module.exports = {
  apps: [{
    name: "properties-api",
    script: "uvicorn",
    args: "main:app --host 0.0.0.0 --port 8000",
    instances: 4,
    exec_mode: "cluster"
  }]
}
EOF

pm2 start ecosystem.config.js
```

---

## 🎯 Roadmap

- [x] Multi-platform scraping support
- [x] City-based search
- [x] Streaming API responses
- [ ] Advanced filtering options
- [ ] Machine learning price prediction
- [ ] Mobile app integration
- [ ] Real-time notifications
- [ ] Database integration

---

## 🙏 Acknowledgments

- **FastAPI** - Modern web framework
- **Bayut.sa** - Real estate data source
- **BeautifulSoup** - HTML parsing
- **curl_cffi** - Advanced HTTP requests
- Open-source community

---

## 📞 Contact & Social

- 🐙 **GitHub**: [@DeveloperSarim](https://github.com/DeveloperSarim)
- 📧 **Email**: sarim.yaseen@aixsolutions.net
- 🔗 **LinkedIn**: [Your LinkedIn Profile]
- 🐦 **Twitter**: [@SarimTools]

---

<div align="center">

**Made with ❤️ by DeveloperSarim**

⭐ If you find this project useful, please star it on [GitHub](https://github.com/DeveloperSarim/Properties-Scraper-Api)

[Back to Top](#-saudi-property-scraper-api)

</div>
