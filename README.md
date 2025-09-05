# Amazon Business Data Processor

A clean, modular Python script that processes Amazon business data and generates comprehensive reports. This is a rewrite of the `Final_Data_Process_V1.ipynb` notebook logic, made more maintainable and reusable.

## 🎯 What it does

This script processes Amazon business data from Excel files and generates the following reports:

### Main Reports
- **`full_detailed_aggregated_report.xlsx`** - Complete ASIN-level analysis with all costs and metrics
- **`full_detailed_aggregated_report_brand.xlsx`** - Brand-level aggregated data
- **`full_detailed_aggregated_report_gm.xlsx`** - GM-level aggregated data  
- **`underperforming_ats_report.xlsx`** - ASINs with sales below expected ATS

### Supporting Reports
- **`monthly_fba_storage_allocation.xlsx`** - Storage cost allocation by ASIN
- **`long_term_storage_summary.xlsx`** - Long-term storage costs
- **`refund_summary_all_asins.xlsx`** - Return data summary

## 📋 Requirements

### Python Dependencies
```bash
pip install pandas numpy openpyxl xlsxwriter
```

### Required Files
1. **Main input file**: `202502 Brands Reports v5.xlsx` (or specify custom filename)
2. **Optional additional files**:
   - `BusinessReport-4-24-25.csv` - Business report data
   - `Brands and ASINs list.xlsx` - Brand and GM mappings
   - `202502 UF.xlsx` - Additional unit financial data

## 🚀 Quick Start

### Basic Usage
```bash
python data_processor.py
```

### With Custom Input File
```bash
python data_processor.py "your_custom_file.xlsx"
```

### From Python
```python
from data_processor import AmazonDataProcessor

# Create processor with default file
processor = AmazonDataProcessor()
processor.generate_all_reports()

# Or with custom file
processor = AmazonDataProcessor("your_file.xlsx")
processor.generate_all_reports()
```

## 📊 Input File Structure

The script expects an Excel file with the following sheets:

| Sheet Name | Description | Required |
|------------|-------------|----------|
| ASIN-Brand | ASIN to brand mappings | Yes |
| Business Report | Business metrics and data | Yes |
| Transaction Report | Order and refund transactions | Yes |
| Return Report | Return data | Yes |
| Selling Econ | Shipping and label fees | Yes |
| Storage | Storage fee data | Yes |
| LT Storage | Long-term storage data | Yes |
| Unit Financial | Unit costs and pricing | Yes |
| Shipping + Label | Additional shipping data | Yes |

## ⚙️ Configuration

Customize the script behavior by editing `config.py`:

```python
# Change default input file
DEFAULT_INPUT_FILE = "your_file.xlsx"

# Modify sheet names if different
SHEET_NAMES = {
    'business': "Your Business Sheet Name",
    # ... other sheets
}

# Customize output file names
OUTPUT_FILES = {
    'main_report': 'your_main_report.xlsx',
    # ... other files
}
```

## 🔧 Key Features

### 1. **Comprehensive Cost Analysis**
- Product costs from unit financial data
- FBA fees and referral fees
- Storage costs (short-term and long-term)
- Shipping and labeling fees
- Return costs and fees
- Allocated subscription and premium service fees

### 2. **Multi-Level Aggregation**
- ASIN-level detailed analysis
- Brand-level summaries
- GM-level summaries
- Automatic total calculations

### 3. **Data Validation & Cleaning**
- Handles missing data gracefully
- Resolves SKU to ASIN mapping conflicts
- Cleans monetary values (removes $ and commas)
- Validates numeric data

### 4. **Performance Monitoring**
- Identifies underperforming ASINs
- Compares actual vs expected ATS
- Tracks gross margin performance

### 5. **Flexible Output**
- Excel format with proper formatting
- Currency formatting ($1,234.56)
- Percentage formatting (12.34%)
- Number formatting with commas (1,234)

## 📈 Report Details

### Main Aggregated Report
Contains for each ASIN:
- Sales data (quantity, product sales)
- Cost breakdown (product cost, fees, storage, shipping)
- Business metrics (Buy Box %, sessions, conversion)
- Financial metrics (gross profit, gross margin)

### Brand/GM Reports
Aggregated summaries showing:
- Total sales and costs by brand/GM
- Average performance metrics
- Formatted for executive review

### Underperforming Report
Identifies ASINs where:
- Actual sales < Expected ATS
- Transaction-level details for investigation

## 🛠️ Customization

### Adding New Reports
```python
def create_custom_report(self, final_df: pd.DataFrame) -> pd.DataFrame:
    """Create a custom report."""
    # Your custom logic here
    return custom_report

# Add to generate_all_reports method
custom_report = self.create_custom_report(final_df)
custom_report.to_excel('custom_report.xlsx', index=False)
```

### Modifying Calculations
Edit the calculation methods in the `AmazonDataProcessor` class:
- `process_orders_and_aggregate()` - Order processing logic
- `process_storage_costs()` - Storage cost calculations
- `process_returns()` - Return processing
- `create_final_aggregated_report()` - Main report creation

### Adding New Data Sources
```python
def load_additional_files(self):
    """Load additional required files."""
    additional_files = {
        'your_new_file': "your_file.xlsx",
        # ... existing files
    }
    # ... loading logic
```

## 🔍 Troubleshooting

### Common Issues

1. **File not found errors**
   - Check file paths in `config.py`
   - Ensure all required files are in the same directory

2. **Column name errors**
   - Verify column names in your Excel files
   - Update `COLUMN_MAPPINGS` in `config.py`

3. **Data type errors**
   - Check for non-numeric data in monetary columns
   - Verify date formats in date columns

4. **Memory issues with large files**
   - Consider processing in chunks
   - Use `chunksize` parameter in pandas read functions

### Debug Mode
Add debug prints to track processing:
```python
print(f"Processing {len(df)} rows...")
print(f"Columns: {df.columns.tolist()}")
```

## 📝 Output Format

All reports are saved as Excel files with:
- Proper column formatting
- Currency formatting for monetary values
- Percentage formatting for ratios
- Number formatting with commas
- Clean, professional appearance

## 🤝 Contributing

To extend the script:
1. Add new methods to `AmazonDataProcessor` class
2. Update `config.py` for new settings
3. Add new output files to `OUTPUT_FILES`
4. Update this README with new features

## 📄 License

This script is provided as-is for processing Amazon business data. Modify as needed for your specific requirements.

## 🆘 Support

For issues or questions:
1. Check the troubleshooting section
2. Verify your input file structure
3. Review the configuration settings
4. Check Python dependencies are installed

---

**Note**: This script processes sensitive business data. Always backup your original files before running the processor.

