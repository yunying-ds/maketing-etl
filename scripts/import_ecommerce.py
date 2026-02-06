import pandas as pd
import os
from sqlalchemy import create_engine

# =================configuration=================
folder_path = "/Users/yunying/projects/maketing-etl/data"

# 2. connect to database marketing_etl
db_url = "postgresql://yunying@localhost:5432/marketing_etl"

# 3. file list
files_to_import = [
    "campaigns.csv",
    "customers.csv",
    "events.csv",
    "products.csv",
    "transactions.csv",
]
# ===========================================


def run_ecommerce_etl():
    # create query engine
    engine = create_engine(db_url)

    print(f"🚀 Start batch importing {len(files_to_import)} files...")

    for file_name in files_to_import:
        # full path
        file_path = os.path.join(folder_path, file_name)
        # extract table names (remove .csv)
        table_name = file_name.replace(".csv", "").lower()

        print(f"\n📄 Processing {table_name} ...")

        # 1. read CSV
        df = pd.read_csv(file_path)

        # 2. clean column names (lower case, replace space)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # 3. auto transform date column
        for col in df.columns:
            if "date" in col or "time" in col:
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    pass  # if not transferable, keep origin

        print(f"   ✅ Done! Inserted {len(df)} rows.")

        # 4. Automatically create the table and write the data
        # (if_exists='replace' will automatically create a new table,
        # or overwrite the existing table if it already exists).
        df.to_sql(table_name, engine, if_exists="replace", index=False, schema="raw")

        print(f"   🎉 Succeed! Table 'raw.{table_name}' has been created.")

    print("\n🏁 All missions completed!")


if __name__ == "__main__":
    run_ecommerce_etl()
