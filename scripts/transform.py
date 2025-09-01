# transform.py
import pandas as pd
from sqlalchemy import text
from etl_utils import get_engine

def main():
    """
    Cloud-only: currently repeats the build of dim_locations.
    Replace with actual master_table transformation when ready.
    """
    print("Starting transform step (current: dim_locations build placeholder)...")

    try:
        engine = get_engine()
        with engine.begin() as conn:
            print("Connected to RDS.")

            df_customers = pd.read_sql(
                text("SELECT customer_zip_code_prefix, customer_city, customer_state FROM customers;"),
                conn
            )
            df_sellers = pd.read_sql(
                text("SELECT seller_zip_code_prefix, seller_city, seller_state FROM sellers;"),
                conn
            )
            df_geo = pd.read_sql(
                text("SELECT geolocation_zip_code_prefix, geolocation_city, geolocation_state FROM geolocation;"),
                conn
            )

            df_customers.rename(columns={
                'customer_zip_code_prefix': 'zip_code_prefix',
                'customer_city': 'city',
                'customer_state': 'state_code'
            }, inplace=True)
            df_sellers.rename(columns={
                'seller_zip_code_prefix': 'zip_code_prefix',
                'seller_city': 'city',
                'seller_state': 'state_code'
            }, inplace=True)
            df_geo.rename(columns={
                'geolocation_zip_code_prefix': 'zip_code_prefix',
                'geolocation_city': 'city',
                'geolocation_state': 'state_code'
            }, inplace=True)

            df_locations = pd.concat([df_customers, df_sellers, df_geo], ignore_index=True)
            df_locations.drop_duplicates(inplace=True)
            df_locations.dropna(inplace=True)

            state_mapping = {
                'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas', 'BA': 'Bahia',
                'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo', 'GO': 'Goiás',
                'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul', 'MG': 'Minas Gerais',
                'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná', 'PE': 'Pernambuco', 'PI': 'Piauí',
                'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte', 'RS': 'Rio Grande do Sul',
                'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina', 'SP': 'São Paulo',
                'SE': 'Sergipe', 'TO': 'Tocantins'
            }
            df_locations['state_name'] = df_locations['state_code'].map(state_mapping)

            print(f"Loading {len(df_locations)} rows → dim_locations ...")
            df_locations.to_sql('dim_locations', conn, if_exists='replace', index=True, index_label='location_id')
            conn.execute(text('ALTER TABLE dim_locations ADD PRIMARY KEY (location_id);'))

            print("Transform step finished (dim_locations).")

    except Exception as e:
        print(f"An error occurred in transform.py: {e}")
        raise

if __name__ == "__main__":
    main()
