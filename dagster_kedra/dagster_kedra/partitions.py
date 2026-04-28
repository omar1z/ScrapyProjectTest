import os
from dagster import MonthlyPartitionsDefinition, WeeklyPartitionsDefinition

monthly_partitions = MonthlyPartitionsDefinition(
    start_date=os.getenv("PARTITION_START", "2024-01-01"), 
    end_date=os.getenv("PARTITION_END"),    #  up to today automatically
)

# weekly_partitions = WeeklyPartitionsDefinition(
#     start_date=os.getenv("PARTITION_START", "2024-01-01"), 
#     end_date=os.getenv("PARTITION_END"),    #  up to today automatically
# )

# we have daily and hourly as well
# we have StaticPartitionsDefinition # Useful for: countries, environments, categories
# we have also DynamicPartitionsDefinition # when new file arrives
# MultiPartitionsDefinition # date with region for example

        