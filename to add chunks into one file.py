import duckdb

duckdb.sql("""
    COPY (
        SELECT * FROM 'C:/Users/saikr/OneDrive/Desktop/s/combined_output/combined_*.csv'
    )
    TO 'C:/Users/saikr/OneDrive/Desktop/s/combined_output/FINAL_COMBINED.csv'
    (HEADER, DELIMITER ',');
""")

print("Final single CSV created successfully!")
