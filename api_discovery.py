"""
Kalshi API Endpoint Discovery
Discovers and tests all available public endpoints from Kalshi's API
"""

import json
import requests
import time
from datetime import datetime
from collections import defaultdict
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class KalshiAPIDiscovery:
    def __init__(self, verify_ssl=False):
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.verify_ssl = verify_ssl
        self.discovered_endpoints = []
        self.endpoint_details = {}
        self.failed_endpoints = []
        
    def test_endpoint(self, endpoint, params=None, method='GET'):
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method.upper() == 'GET':
                response = requests.get(
                    url, 
                    params=params or {}, 
                    verify=self.verify_ssl, 
                    timeout=10
                )
            elif method.upper() == 'POST':
                response = requests.post(
                    url, 
                    json=params or {}, 
                    verify=self.verify_ssl, 
                    timeout=10
                )
            else:
                return None
                
            return {
                'url': url,
                'status_code': response.status_code,
                'headers': dict(response.headers),
                'response_size': len(response.content),
                'content_type': response.headers.get('content-type', ''),
                'data': response.json() if response.headers.get('content-type', '').startswith('application/json') else None
            }
            
        except requests.exceptions.RequestException as e:
            return {
                'url': url,
                'error': str(e),
                'status_code': None
            }
        except json.JSONDecodeError:
            return {
                'url': url,
                'status_code': response.status_code,
                'error': 'Invalid JSON response',
                'content': response.text[:200]
            }
    
    def discover_endpoints(self):
        print("🔍 Discovering Kalshi API endpoints...")
        
        known_endpoints = [
            "/markets",
            "/events",
            "/series", 
            "/exchange_status",
            "/cached_user",
            "/login",
            "/logout",
            "/portfolio",
            "/positions",
            "/orders",
            "/fills",
            "/balance",
            "/user",
            "/user/balance",
            "/user/fills",
            "/user/orders",
            "/user/positions",
            "/settlements"
        ]
        
        resource_patterns = [
            "/markets/{ticker}",
            "/markets/{ticker}/orderbook", 
            "/markets/{ticker}/history",
            "/markets/{ticker}/orders",
            "/events/{event_ticker}",
            "/events/{event_ticker}/markets",
            "/series/{series_ticker}",
            "/series/{series_ticker}/events"
        ]
        
        for endpoint in known_endpoints:
            print(f"  Testing: {endpoint}")
            result = self.test_endpoint(endpoint)
            
            if result and result['status_code']:
                if result['status_code'] == 200:
                    self.discovered_endpoints.append(endpoint)
                    self.endpoint_details[endpoint] = result
                    print(f"    ✅ {result['status_code']}")
                elif result['status_code'] in [401, 403]:
                    self.discovered_endpoints.append(endpoint)
                    self.endpoint_details[endpoint] = result
                    print(f"    🔒 {result['status_code']} (Auth required)")
                elif result['status_code'] == 404:
                    print(f"    ❌ {result['status_code']} (Not found)")
                else:
                    self.endpoint_details[endpoint] = result
                    print(f"    ⚠️  {result['status_code']}")
            else:
                self.failed_endpoints.append(endpoint)
                print(f"    💥 Error: {result.get('error', 'Unknown')}")
            
            time.sleep(0.1)
        
        print(f"\n📊 Testing parameterized endpoints...")
        self.discover_parameterized_endpoints(resource_patterns)
        
        return self.discovered_endpoints
    
    def discover_parameterized_endpoints(self, patterns):
        sample_data = self.get_sample_identifiers()
        
        for pattern in patterns:
            print(f"  Testing pattern: {pattern}")
            
            if "{ticker}" in pattern:
                for ticker in sample_data.get('tickers', [])[:3]:
                    endpoint = pattern.replace("{ticker}", ticker)
                    result = self.test_endpoint(endpoint)
                    
                    if result and result['status_code'] == 200:
                        self.discovered_endpoints.append(pattern)
                        self.endpoint_details[pattern] = result
                        print(f"    ✅ Works with {ticker}")
                        break
                    elif result and result['status_code'] in [401, 403]:
                        self.discovered_endpoints.append(pattern)
                        self.endpoint_details[pattern] = result
                        print(f"    🔒 Auth required")
                        break
            
            if "{event_ticker}" in pattern:
                for event_ticker in sample_data.get('event_tickers', [])[:3]:
                    endpoint = pattern.replace("{event_ticker}", event_ticker)
                    result = self.test_endpoint(endpoint)
                    
                    if result and result['status_code'] == 200:
                        self.discovered_endpoints.append(pattern)
                        self.endpoint_details[pattern] = result
                        print(f"    ✅ Works with {event_ticker}")
                        break
                        
            if "{series_ticker}" in pattern:
                for series_ticker in sample_data.get('series_tickers', [])[:3]:
                    endpoint = pattern.replace("{series_ticker}", series_ticker)
                    result = self.test_endpoint(endpoint)
                    
                    if result and result['status_code'] == 200:
                        self.discovered_endpoints.append(pattern)
                        self.endpoint_details[pattern] = result
                        print(f"    ✅ Works with {series_ticker}")
                        break
            
            time.sleep(0.1)
    
    def get_sample_identifiers(self):
        sample_data = {
            'tickers': [],
            'event_tickers': [],
            'series_tickers': []
        }
        
        markets_result = self.test_endpoint("/markets", {"limit": 10})
        if markets_result and markets_result.get('data'):
            markets = markets_result['data'].get('markets', [])
            for market in markets:
                if market.get('ticker'):
                    sample_data['tickers'].append(market['ticker'])
                if market.get('event_ticker'):
                    sample_data['event_tickers'].append(market['event_ticker'])
                
                ticker = market.get('ticker', '')
                if '-' in ticker:
                    series = ticker.split('-')[0]
                    sample_data['series_tickers'].append(series)
        
        for key in sample_data:
            sample_data[key] = list(set(sample_data[key]))[:5]
        
        return sample_data
    
    def analyze_endpoint_capabilities(self):
        print("\n🔬 Analyzing endpoint capabilities...")
        
        capabilities = defaultdict(list)
        
        for endpoint in self.discovered_endpoints:
            details = self.endpoint_details.get(endpoint, {})
            
            if details.get('status_code') == 200 and details.get('data'):
                data = details['data']
                
                if isinstance(data, dict):
                    for key in data.keys():
                        capabilities[endpoint].append(f"Returns '{key}' field")
                        
                    if 'cursor' in data:
                        capabilities[endpoint].append("Supports pagination")
                    
                    if any(field in data for field in ['markets', 'events', 'series']):
                        capabilities[endpoint].append("Returns collection data")
                        
                elif isinstance(data, list):
                    capabilities[endpoint].append(f"Returns array of {len(data)} items")
            
            elif details.get('status_code') in [401, 403]:
                capabilities[endpoint].append("Requires authentication")
            
            content_type = details.get('content_type', '')
            if 'json' in content_type:
                capabilities[endpoint].append("JSON response")
        
        return dict(capabilities)
    
    def generate_report(self):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"kalshi_api_discovery_{timestamp}.json"
        
        capabilities = self.analyze_endpoint_capabilities()
        
        report = {
            'discovery_time': timestamp,
            'base_url': self.base_url,
            'summary': {
                'total_endpoints_tested': len(self.endpoint_details) + len(self.failed_endpoints),
                'successful_endpoints': len(self.discovered_endpoints),
                'failed_endpoints': len(self.failed_endpoints),
                'public_endpoints': len([e for e in self.discovered_endpoints 
                                       if self.endpoint_details.get(e, {}).get('status_code') == 200]),
                'auth_required_endpoints': len([e for e in self.discovered_endpoints 
                                              if self.endpoint_details.get(e, {}).get('status_code') in [401, 403]])
            },
            'public_endpoints': [],
            'authenticated_endpoints': [],
            'endpoint_details': self.endpoint_details,
            'capabilities': capabilities,
            'failed_endpoints': self.failed_endpoints
        }
        
        for endpoint in self.discovered_endpoints:
            details = self.endpoint_details.get(endpoint, {})
            endpoint_info = {
                'endpoint': endpoint,
                'status_code': details.get('status_code'),
                'method': 'GET',
                'description': self.generate_endpoint_description(endpoint, details),
                'capabilities': capabilities.get(endpoint, [])
            }
            
            if details.get('status_code') == 200:
                report['public_endpoints'].append(endpoint_info)
            elif details.get('status_code') in [401, 403]:
                report['authenticated_endpoints'].append(endpoint_info)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        return filename, report
    
    def generate_endpoint_description(self, endpoint, details):
        if endpoint == "/markets":
            return "Retrieve all available markets with pagination support"
        elif endpoint == "/events":
            return "Retrieve all events"
        elif endpoint == "/series":
            return "Retrieve all market series"
        elif endpoint == "/exchange_status":
            return "Get current exchange status and operational information"
        elif "{ticker}" in endpoint:
            if "orderbook" in endpoint:
                return "Get order book data for a specific market"
            elif "history" in endpoint:
                return "Get price history for a specific market"
            elif "orders" in endpoint:
                return "Get orders for a specific market (auth required)"
            else:
                return "Get detailed information for a specific market"
        elif endpoint.startswith("/user"):
            return "User account information (authentication required)"
        elif "portfolio" in endpoint or "balance" in endpoint or "positions" in endpoint:
            return "Account/portfolio information (authentication required)"
        else:
            return "API endpoint"
    
    def print_summary(self, report, filename):
        print("\n" + "="*60)
        print("KALSHI API DISCOVERY REPORT")
        print("="*60)
        
        print(f"Base URL: {self.base_url}")
        print(f"Discovery completed: {report['discovery_time']}")
        print("-"*60)
        
        print(f"📊 SUMMARY")
        print(f"  Total endpoints tested: {report['summary']['total_endpoints_tested']}")
        print(f"  Successful discoveries: {report['summary']['successful_endpoints']}")
        print(f"  Public endpoints: {report['summary']['public_endpoints']}")
        print(f"  Auth required: {report['summary']['auth_required_endpoints']}")
        print(f"  Failed/Not found: {report['summary']['failed_endpoints']}")
        
        if report['public_endpoints']:
            print(f"\n🌐 PUBLIC ENDPOINTS ({len(report['public_endpoints'])})")
            for ep in report['public_endpoints']:
                print(f"  ✅ {ep['endpoint']}")
                print(f"     {ep['description']}")
                if ep['capabilities']:
                    print(f"     Features: {', '.join(ep['capabilities'])}")
        
        if report['authenticated_endpoints']:
            print(f"\n🔒 AUTHENTICATED ENDPOINTS ({len(report['authenticated_endpoints'])})")
            for ep in report['authenticated_endpoints']:
                print(f"  🔐 {ep['endpoint']}")
                print(f"     {ep['description']}")
        
        print(f"\n💾 Detailed report saved to: {filename}")

def main():
    print("🚀 KALSHI API ENDPOINT DISCOVERY")
    print("="*60)
    
    discovery = KalshiAPIDiscovery()
    
    endpoints = discovery.discover_endpoints()
    
    if endpoints:
        filename, report = discovery.generate_report()
        discovery.print_summary(report, filename)
        
        print(f"\n🎯 QUICK START GUIDE")
        print("="*30)
        print("Public endpoints you can use immediately:")
        
        for ep in report['public_endpoints']:
            if not any(param in ep['endpoint'] for param in ['{', 'user', 'portfolio']):
                print(f"  curl '{discovery.base_url}{ep['endpoint']}'")
    
    else:
        print("❌ No endpoints discovered")

if __name__ == "__main__":
    main()