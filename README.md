# Simple Query Using Salesforce-Bulk API

### A simple query only python module to extract data from salesforce API

Update config.py with appropriate credientials


```python 
import salesfore_bulk_query as sbq
df = sbq.bulk_query("Select Name from Account").as_df()
```
    
