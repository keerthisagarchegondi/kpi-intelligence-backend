# Quick Reference - New API Endpoints

## 🚀 Quick Start

### Start the Server
```bash
python api/main.py
```
Server runs at: `http://localhost:8000`

---

## 📊 /kpis Endpoint

### Basic Usage
```bash
# Get all KPIs
curl "http://localhost:8000/api/v1/kpis"

# Get financial KPIs for last 30 days
curl "http://localhost:8000/api/v1/kpis?period=30d&category=financial"
```

### Parameters
- `period`: `current` | `7d` | `30d` | `90d` | `ytd` | `12m`
- `category`: `financial` | `operational` | `customer` | `product` | `sales`
- `format`: `minimal` | `summary` | `detailed`
- `compare_previous`: `true` | `false`
- `include_trends`: `true` | `false`

### Categories & Metrics

**Financial**: Revenue, Profit, Margins, ROI, Cost  
**Operational**: Transactions, Units Sold, Return Rate, Fulfillment  
**Customer**: Total Customers, Retention, Churn, LTV, CAC, NPS  
**Product**: Total Products, Performance, Market Share, Quality  
**Sales**: Sales Volume, Conversion, Win Rate, Quota Attainment

---

## 📈 /reports Endpoint

### Basic Usage
```bash
# Executive Summary
curl "http://localhost:8000/api/v1/reports?report_type=executive_summary&period=30d"

# Financial Report with Charts
curl "http://localhost:8000/api/v1/reports?report_type=financial&period=90d&include_charts=true"
```

### Parameters
- `report_type`: **REQUIRED** (see types below)
- `period`: `7d` | `30d` | `90d` | `qtd` | `ytd` | `12m`
- `format`: `json` | `summary` | `detailed`
- `include_charts`: `true` | `false`
- `include_recommendations`: `true` | `false`

### Report Types

| Type | Description |
|------|-------------|
| `executive_summary` | High-level overview for executives |
| `financial` | Revenue, costs, profitability analysis |
| `sales_analysis` | Sales performance and pipeline |
| `customer_analytics` | Customer behavior and segmentation |
| `product_performance` | Product-level performance analysis |
| `operational` | Operational efficiency metrics |
| `custom` | Customizable report |

---

## 💡 Common Use Cases

### Dashboard - Get Current KPIs
```bash
curl "http://localhost:8000/api/v1/kpis?format=minimal&period=current"
```

### Weekly Report - Financial Performance
```bash
curl "http://localhost:8000/api/v1/reports?report_type=financial&period=7d"
```

### Monthly Review - All Categories
```bash
curl "http://localhost:8000/api/v1/kpis?period=30d&format=detailed"
```

### Quarterly Analysis - Sales Performance
```bash
curl "http://localhost:8000/api/v1/reports?report_type=sales_analysis&period=qtd&include_charts=true"
```

### Customer Health Check
```bash
curl "http://localhost:8000/api/v1/kpis?category=customer&compare_previous=true&period=30d"
```

---

## 🔍 PowerShell Examples

```powershell
# Get all KPIs
$kpis = Invoke-RestMethod "http://localhost:8000/api/v1/kpis?period=30d"

# Display revenue
$kpis.data.kpis.financial.revenue.value

# Get executive summary
$report = Invoke-RestMethod "http://localhost:8000/api/v1/reports?report_type=executive_summary&period=30d"

# Display business health score
$report.report.sections[1].data.overall_score
```

---

## 🐍 Python Examples

```python
import requests

# Get KPIs
response = requests.get("http://localhost:8000/api/v1/kpis", 
                       params={"period": "30d", "category": "financial"})
kpis = response.json()

# Access revenue
revenue = kpis['data']['kpis']['financial']['revenue']['value']
print(f"Revenue: ${revenue:,.2f}")

# Get report
response = requests.get("http://localhost:8000/api/v1/reports",
                       params={"report_type": "financial", "period": "90d"})
report = response.json()
```

---

## 📱 Response Structure

### KPIs Response
```json
{
  "status": "success",
  "data": {
    "kpis": { /* KPI data by category */ },
    "summary": { /* Overall summary */ },
    "insights": [ /* Automated insights */ ],
    "period_info": { /* Period details */ }
  },
  "metadata": { /* Processing info */ }
}
```

### Reports Response
```json
{
  "status": "success",
  "report": {
    "title": "Report Title",
    "sections": [ /* Report sections */ ]
  },
  "charts": [ /* Chart data (optional) */ ],
  "recommendations": [ /* Insights (optional) */ ],
  "metadata": { /* Generation info */ }
}
```

---

## ✅ Health Check

```bash
curl "http://localhost:8000/api/v1/health"
```

Returns list of all available endpoints.

---

## 🎯 Performance Tips

1. Use `format=minimal` for dashboards
2. Filter by `category` to reduce response size
3. Cache responses on client side when possible
4. Use `compare_previous=true` for trend analysis
5. Enable `include_charts` only when needed

---

## 🔗 Full Documentation

See [API_ENDPOINTS_DOCUMENTATION.md](./API_ENDPOINTS_DOCUMENTATION.md) for complete details.

---

## 📊 Interactive API Docs

Visit: `http://localhost:8000/docs`

Swagger UI with interactive testing for all endpoints.

---

## ⚡ Status Indicators

KPI status values:
- `healthy` - Meeting or exceeding targets
- `warning` - Below optimal levels
- `excellent` - Significantly exceeding targets
- `neutral` - No status determination

Trend values:
- `up` - Increasing trend
- `down` - Decreasing trend
- `stable` - Minimal change

---

**Quick Reference v1.0** | Last Updated: May 8, 2026
