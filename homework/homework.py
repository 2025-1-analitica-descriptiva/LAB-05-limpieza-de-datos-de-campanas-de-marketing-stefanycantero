"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortgage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaign_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import pandas as pd
    import zipfile
    import os

    input_path = "files/input/"
    output_path = "files/output/"
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        
    data = []
    for filename in os.listdir(input_path):
        if filename.endswith(".csv.zip"):
            with zipfile.ZipFile(os.path.join(input_path, filename), 'r') as z:
                for file in z.namelist():
                    with z.open(file) as f:
                        data.append(pd.read_csv(f))

    df = pd.concat(data, ignore_index=True)

    # client.csv:
    client_df = df[['client_id', 'age', 'job', 'marital', 'education',
                    'credit_default', 'mortgage']].copy()
    client_df['job'] = client_df['job'].str.replace('.', '', regex=False)
    client_df['job'] = client_df['job'].str.replace('-', '_', regex=False)
    client_df['education'] = client_df['education'].str.replace('.', '_', regex=False)
    client_df['education'] = client_df['education'].replace('unknown', pd.NA)
    client_df['credit_default'] = client_df['credit_default'].astype(str).str.strip().str.lower().apply(lambda x: 1 if x == 'yes' else 0)
    client_df['mortgage'] = client_df['mortgage'].astype(str).str.strip().str.lower().apply(lambda x: 1 if x == 'yes' else 0)

    client_df.to_csv(os.path.join(output_path, 'client.csv'), index=False)

    # campaign.csv:
    campaign_df = df[['client_id', 'number_contacts', 'contact_duration',
                       'previous_campaign_contacts', 'previous_outcome',
                       'campaign_outcome', 'day', 'month']].copy()
    campaign_df['previous_outcome'] = campaign_df['previous_outcome'].replace('success', 1)
    campaign_df['previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: 1 if x == 1 else 0)
    campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].astype(str).str.strip().str.lower().apply(lambda x: 1 if x == 'yes' else 0)
    campaign_df['last_contact_date'] = campaign_df.apply(
        lambda row: f"2022-{row['month']}-{int(row['day']):02d}", axis=1
    )
    campaign_df['last_contact_date'] = pd.to_datetime(campaign_df['last_contact_date'], format='%Y-%b-%d')
    campaign_df['last_contact_date'] = campaign_df['last_contact_date'].dt.strftime('%Y-%m-%d')
    campaign_df = campaign_df.drop(columns=['day', 'month'])
    campaign_df.to_csv(os.path.join(output_path, 'campaign.csv'), index=False)

    # economics.csv:
    economics_df = df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    economics_df.to_csv(os.path.join(output_path, 'economics.csv'), index=False)    

if __name__ == "__main__":
    clean_campaign_data()
