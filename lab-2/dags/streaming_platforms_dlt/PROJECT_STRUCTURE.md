# Streaming Platforms DLT Pipeline

## 📁 Project Structure

```
streaming_platforms_dlt/
├── dag_loader.py                    # 🚀 Main DAG entry point (loaded by Airflow)
├── streaming_platforms_dlt.yaml    # ⚙️  DAG configuration (tasks, dependencies)
├── functions.py                     # 🔧 All pipeline functions (core logic)
├── test_pipeline.py                # 🧪 Local testing script
├── README.md                       # 📖 Documentation
└── PROJECT_STRUCTURE.md            # 📋 This file
```

## 🔧 Core Files

### **`dag_loader.py`**
- **Purpose**: Airflow DAG entry point
- **Used by**: Airflow scheduler
- **Contains**: DAG factory loader code

### **`streaming_platforms_dlt.yaml`**
- **Purpose**: DAG configuration
- **Used by**: `dag_loader.py`
- **Contains**: Task definitions, dependencies, schedules

### **`functions.py`**
- **Purpose**: All pipeline logic
- **Used by**: Airflow tasks (via YAML config)
- **Contains**:
  - `load_csv_to_dlt()` - Load CSV files to DuckDB
  - `create_unified_table()` - Merge data by title
  - `validate_data()` - Data validation and stats
  - `analyze_cross_platform_rankings()` - Cross-platform analysis

## 🧪 Testing

### **`test_pipeline.py`**
- **Purpose**: Local testing outside Airflow
- **Usage**:
  ```bash
  # Full pipeline test
  python test_pipeline.py

  # Quick validation only
  python test_pipeline.py --quick

  # Clean test databases
  python test_pipeline.py --clean
  ```

## 🎯 Best Practices

### ✅ **What We Do Right**
1. **Single source of truth**: All functions in `functions.py`
2. **Configuration-driven**: DAG defined in YAML
3. **Testable**: Separate test script for local development
4. **Clean separation**: DAG loading vs. business logic

### 🚀 **Why This Structure Works**
1. **Maintainable**: One file per concern
2. **Testable**: Can test functions independently
3. **Scalable**: Easy to add new functions to `functions.py`
4. **Airflow-friendly**: Uses DAG Factory pattern

## 🔄 Development Workflow

1. **Add new function** → Edit `functions.py`
2. **Add new task** → Edit `streaming_platforms_dlt.yaml`
3. **Test locally** → Run `python test_pipeline.py`
4. **Deploy** → Airflow picks up changes automatically

## 📊 Data Flow

```
CSV Files → load_csv_to_dlt() → Individual DuckDB files
                                       ↓
Unified Table ← create_unified_table() ← Platform DBs
     ↓
[validate_data() + analyze_cross_platform_rankings()]
     ↓
Final Reports & Statistics
```
