import os
from dagster import MonthlyPartitionsDefinition

monthly_partitions = MonthlyPartitionsDefinition(
    start_date=os.getenv("PARTITION_START", "2024-01-01"), 
    end_date=os.getenv("PARTITION_END"),    #  up to today automatically
)