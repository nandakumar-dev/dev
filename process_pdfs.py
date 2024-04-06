import pandas as pd
import requests
from data_common_library.azure import client
from io import StringIO
from urllib.parse import urlparse
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from pdf import pdf_parser
from raw_to_bronze_operators import RawToBronzeOperatorManual


# task 1
def check_and_upload_raw_pdf_files(azure_storage_account, source_container, source_blob_path, target_container,
                                   **kwargs):
    # Create a blob client
    # Get the CSV blob and read its content
    csv_blob_client = client.create_blob_client(blob_account=azure_storage_account, container=source_container,
                                                blob_name=source_blob_path)
    csv_content = csv_blob_client.download_blob().content_as_text()
    csv_data = pd.read_csv(StringIO(csv_content))

    # Iterate through each URL in the CSV
    for url in csv_data['url']:
        parsed_url = urlparse(url)
        # Construct the path for the file to be stored in the target container
        blob_name = f"{parsed_url.netloc}{parsed_url.path}"

        # Check if the blob already exists in the target container
        target_blob_client = client.create_blob_client(blob_account=azure_storage_account, container=target_container,
                                                       blob_name=blob_name)
        try:
            target_blob_client.get_blob_properties()
            print(f"Blob already exists: {blob_name}")
        except ResourceNotFoundError:
            # If the blob does not exist, download the file and upload it
            try:
                file_response = requests.get(url)
                file_response.raise_for_status()
                print(f"Uploading file: {blob_name}")
                target_blob_client.upload_blob(file_response.content)
            except requests.exceptions.RequestException as e:
                # Catches any exception that may occur during the HTTP request
                print(f"Failed to download file from URL: {url}. Error: {e}")
    # Convert DataFrame to JSON and push to XCom
    json_csv_data = csv_data.to_json(date_format='iso', orient='split')
    kwargs['ti'].xcom_push(key='csv_data', value=json_csv_data)


# task 2
def check_and_upload_demarcated_pdf_file(azure_storage_account, target_container, **kwargs):
    # Step 1: Pull the DataFrame from XCom
    ti = kwargs['ti']
    json_csv_data = ti.xcom_pull(task_ids='check_and_upload_raw_pdf_files', key='csv_data')

    # Convert the JSON string to a file-like object using StringIO
    json_csv_data_io = StringIO(json_csv_data)

    # Use pd.read_json() with the file-like object
    csv_data = pd.read_json(json_csv_data_io, orient='split')

    # Step 2: Create a dictionary for demarcation of pdfs
    demarcation_settings = {
        'vsrr031.pdf': {
            "2": {
                "1": {
                    'region': {'x0': 20.4, 'x1': 760, 'top': 86.4, 'bottom': 372},
                    'adjustments': {
                        'rows_remove_indices': [2],
                        'columns_remove_indices': [],
                        'column_row_indices_limit_columns': [(2, 2), (3, 2), (5, 2), (6, 2), (8, 2), (9, 2), (11, 2),
                                                             (12, 2), (13, 1), (14, 2)],
                        'column_position_move_coulmns': [(6, -3), (8, -3), (9, -3)]
                    }
                }
            }
        },
        'nvsr71-01.pdf': {
            "2": {
                "1": {
                    'region': {'x0': 14, 'x1': 590, 'top': 456, 'bottom': 723},
                    'adjustments': {
                        'rows_remove_indices': [1],
                        'columns_remove_indices': [],
                        'column_row_indices_limit_columns': [(2, 1), (3, 1), (5, 1), (6, 1), (8, 1), (9, 1), (11, 1),
                                                             (12, 1), (14, 1), (15, 1), (17, 1), (18, 1)],
                        'column_position_move_coulmns': []
                    }
                }

            },
            "3": {
                "1": {
                    'region': {'x0': 5, 'x1': 730, 'top': 57.6, 'bottom': 322},
                    'adjustments': {
                        'rows_remove_indices': [1],
                        'columns_remove_indices': [],
                        'column_row_indices_limit_columns': [(2, 1), (3, 1), (5, 1), (6, 1), (8, 1), (9, 1), (11, 1),
                                                             (12, 1), (14, 1), (15, 1), (17, 1), (18, 1)],
                        'column_position_move_coulmns': [(8, 15), (9, 15)]
                    }
                },
                "rotation": pdf_parser.adjust_for_orientation_ccw
            }
        },
        'nvsr71-02.pdf': {
            "3": {
                "1": {
                    'region': {'x0': 25.7, 'x1': 580.3, 'top': 74.9, 'bottom': 677.3},
                    'adjustments': {
                        'rows_remove_indices': [2, 3],
                        'columns_remove_indices': [],
                        'column_row_indices_limit_columns': [],
                        'column_position_move_coulmns': []
                    }
                }
            },
            "6": {
                "1": {
                    'region': {'x0': 25.7, 'x1': 580.3, 'top': 74.9, 'bottom': 677.3},
                    'adjustments': {
                        'rows_remove_indices': [2, 3],
                        'columns_remove_indices': [],
                        'column_row_indices_limit_columns': [],
                        'column_position_move_coulmns': []
                    }
                }
            },
            "7": {
                "1_1": {
                    'region': {'x0': 29.5, 'x1': 301, 'top': 448.1, 'bottom': 760},
                    'adjustments': {
                        'rows_remove_indices': [1, 2],
                        'columns_remove_indices': [],
                        'column_row_indices_limit_columns': [],
                        'column_position_move_coulmns': []
                    }
                },
                "1_2": {
                    'region': {'x0': 310, 'x1': 568.3, 'top': 448.1, 'bottom': 735},
                    'adjustments': {
                        'rows_remove_indices': [1, 2],
                        'columns_remove_indices': [],
                        'column_row_indices_limit_columns': [],
                        'column_position_move_coulmns': []
                    }
                }
            }
        },
        "LExpMort.pdf": {
            "1": {
                "1_1": {
                    'region': {'x0': 21.6, 'x1': 579, 'top': 80, 'bottom': 418},
                    'adjustments': {
                        'rows_remove_indices': [],
                        'columns_remove_indices': [1],
                        'column_row_indices_limit_columns': [(3, 1), (4, 1), (6, 1), (8, 1), (9, 1)],
                        'column_position_move_coulmns': [(8, 5)]
                    }
                },
                "1_2": {
                    'region': {'x0': 21.6, 'x1': 579, 'top': 430, 'bottom': 704},
                    'adjustments': {
                        'rows_remove_indices': [],
                        'columns_remove_indices': [],
                        'column_row_indices_limit_columns': [],
                        'column_position_move_coulmns': []
                    }
                }
            },
            "2": {
                "1": {
                    'region': {'x0': 31, 'x1': 568, 'top': 93.6, 'bottom': 365},
                    'adjustments': {
                        'rows_remove_indices': [],
                        'columns_remove_indices': [1],
                        'column_row_indices_limit_columns': [(2, 1), (8, 1), (9, 1)],
                        'column_position_move_coulmns': [(8, 5)]
                    }
                },
                "2": {
                    'region': {'x0': 31, 'x1': 568, 'top': 373, 'bottom': 587},
                    'adjustments': {
                        'rows_remove_indices': [],
                        'columns_remove_indices': [1],
                        'column_row_indices_limit_columns': [(2, 1), (3, 1), (5, 1), (6, 1), (8, 1), (9, 1)],
                        'column_position_move_coulmns': []
                    }
                },
                "3": {
                    'region': {'x0': 31, 'x1': 568, 'top': 595, 'bottom': 667},
                    'adjustments': {
                        'rows_remove_indices': [],
                        'columns_remove_indices': [1],
                        'column_row_indices_limit_columns': [(2, 1), (3, 1), (5, 1), (6, 1)],
                        'column_position_move_coulmns': []
                    },
                    'vertical_threshold': 70
                }
            },
            "3": {
                "1": {
                    'region': {'x0': 29.8, 'x1': 572, 'top': 93.6, 'bottom': 307},
                    'adjustments': {
                        'rows_remove_indices': [],
                        'columns_remove_indices': [1],
                        'column_row_indices_limit_columns': [(2, 1), (3, 1), (5, 1), (6, 1), (8, 1)],
                        'column_position_move_coulmns': []
                    }
                },
                "2": {
                    'region': {'x0': 27.8, 'x1': 438, 'top': 313.9, 'bottom': 385},
                    'adjustments': {
                        'rows_remove_indices': [1],
                        'columns_remove_indices': [1, 2],
                        'column_row_indices_limit_columns': [(2, 1), (3, 1), (5, 1), (6, 1)],
                        'column_position_move_coulmns': [(2, 15)]
                    },
                    'vertical_threshold': 70
                },
                "3": {
                    'region': {'x0': 28, 'x1': 568, 'top': 392, 'bottom': 606},
                    'adjustments': {
                        'rows_remove_indices': [],
                        'columns_remove_indices': [1, 2],
                        'column_row_indices_limit_columns': [(2, 1), (3, 1), (5, 1), (6, 1)],
                        'column_position_move_coulmns': []
                    },
                    'vertical_threshold': 70
                },
                "4": {
                    'region': {'x0': 30, 'x1': 446, 'top': 613, 'bottom': 689},
                    'adjustments': {
                        'rows_remove_indices': [1],
                        'columns_remove_indices': [1, 2],
                        'column_row_indices_limit_columns': [(2, 1), (3, 1), (5, 1), (6, 1)],
                        'column_position_move_coulmns': [(2, 15)]
                    },
                    'vertical_threshold': 70
                }
            }
        },
        "lifeexpectancy-H.pdf": {
            "3": {
                "1": {
                    'region': {'x0': 62, 'x1': 508, 'top': 70, 'bottom': 186},
                    'adjustments': {
                        'rows_remove_indices': [],
                        'columns_remove_indices': [],
                        'column_row_indices_limit_columns': [],
                        'column_position_move_coulmns': []
                    }
                }
            },
            "4": {
                "1": {
                    'region': {'x0': 65, 'x1': 540, 'top': 77, 'bottom': 307},
                    'adjustments': {
                        'rows_remove_indices': [14],
                        'columns_remove_indices': [1, 4, 7],
                        'column_row_indices_limit_columns': [],
                        'column_position_move_coulmns': []
                    },
                    'vertical_threshold': 130
                }
            },
            "5": {
                "1": {
                    'region': {'x0': 65, 'x1': 542, 'top': 77, 'bottom': 343},
                    'adjustments': {
                        'rows_remove_indices': [6, 8, 16, 18, 20],
                        'columns_remove_indices': [1, 4, 7],
                        'column_row_indices_limit_columns': [],
                        'column_position_move_coulmns': []
                    },
                    'vertical_threshold': 130
                }
            }
        }
    }

    # Step 3 & 4: Iterate through each URL in the DataFrame and check for a demarcated PDF blob
    for url in csv_data['url']:
        parsed_url = urlparse(url)
        # Modify the URL path to include '_demarcated' before the '.pdf' extension
        path_parts = parsed_url.path.rsplit('.', 1)
        demarcated_blob_name = f"{parsed_url.netloc}{path_parts[0]}_demarcated.{path_parts[1]}"
        print(f"Checking for demarcated blob: {demarcated_blob_name}")

        # Check if the blob already exists in the target container
        target_blob_client = client.create_blob_client(blob_account=azure_storage_account, container=target_container,
                                                       blob_name=demarcated_blob_name)

        if target_blob_client.exists():
            print(f"Demarcated blob already exists: {demarcated_blob_name}")
        else:
            raw_pdf_blob_name = f"{parsed_url.netloc}{parsed_url.path}"
            raw_pdf_blob_client = client.create_blob_client(blob_account=azure_storage_account,
                                                            container=target_container,
                                                            blob_name=raw_pdf_blob_name)
            pdf_file_content = raw_pdf_blob_client.download_blob().readall()

            filename = path_parts[0].split('/')[-1]
            filename = f"{filename}.{path_parts[1]}"

            if filename in demarcation_settings:
                modified_pdf_content = pdf_parser.draw_lines_on_pdf(pdf_file_content, demarcation_settings[filename])
                # Upload the demarcated PDF file to Azure Blob Storage
                target_blob_client.upload_blob(modified_pdf_content, overwrite=True)
                print(f"Uploaded demarcated PDF to {demarcated_blob_name}")
            else:
                print(f"No demarcation settings found for {filename}, skipping.")


# task 3, processing single pdf file and storing in to_bronze container
def process_vssr031_pdf():
    import pandas as pd
    vssr_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-wild',
                                                 blob_name='www.cdc.gov/nchs/data/vsrr/vsrr031_demarcated.pdf')

    pdf_content = vssr_blob_client.download_blob().readall()

    # getting a table as list of lists from pdf
    raw_table_vsrr031_2_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_blob=pdf_content, page_number=1, region=None))
    # creating column hierarchy
    table_vsrr031_2_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_vsrr031_2_1[1],
            "hierarchy2": raw_table_vsrr031_2_1[2][1:]
        }
    )
    table_vsrr031_2_1_columns.insert(0, 'Age')

    table_vsrr031_2_1_columns[4] = 'Hispanic_Total'
    table_vsrr031_2_1_columns[5] = 'Hispanic_Male'
    table_vsrr031_2_1_columns[6] = 'Hispanic_Female'
    # creating a dataframe
    table_vsrr031_2_1 = pdf_parser.to_dataframe(table_vsrr031_2_1_columns, raw_table_vsrr031_2_1[3:])

    # table specific cleaning of dataframe
    table_vsrr031_2_1['Age'] = table_vsrr031_2_1['Age'].str.replace(r'\.+\s*', '', regex=True).str.strip()
    table_vsrr031_2_1.replace(r'…', '', regex=True, inplace=True)

    csv_datatable_vsrr031_2_1 = table_vsrr031_2_1.to_csv(index=False)

    # uploading to to_bronze container
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/vsrr/vsrr031_2_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/vsrr/vsrr031_2_1.csv'")
    else:
        # Convert DataFrame to a CSV string and then upload
        target_blob_client.upload_blob(csv_datatable_vsrr031_2_1, overwrite=True)


def process_nvsr71_02_pdf():
    nvsr_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-wild',
                                                 blob_name='www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71-02_demarcated.pdf')

    pdf_content = nvsr_blob_client.download_blob().readall()

    # page 3
    # getting a table as list of lists from pdf
    raw_table_nvsr71_02_3_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_blob=pdf_content, page_number=2, region=None))

    # creating column hierarchy
    table_nvsr71_02_3_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_nvsr71_02_3_1[0],
            "hierarchy2": raw_table_nvsr71_02_3_1[1][1:]
        }
    )
    table_nvsr71_02_3_1_columns.insert(0, 'Area')
    table_nvsr71_02_3_1 = pdf_parser.to_dataframe(table_nvsr71_02_3_1_columns, raw_table_nvsr71_02_3_1[2:])

    table_nvsr71_02_3_1['Area'] = table_nvsr71_02_3_1['Area'].str.replace(r'\.+\s*', ' ', regex=True).str.strip()
    table_nvsr71_02_3_1.replace(r'\…', '', regex=True, inplace=True)

    csv_data_nvsr71_02_3_1 = table_nvsr71_02_3_1.to_csv(index=False)
    ####################################################################################################################

    # page 6
    raw_table_nvsr71_02_6_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_blob=pdf_content, page_number=5, region=None))

    # creating column hierarchies
    table_nvsr71_02_6_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_nvsr71_02_6_1[0],
            "hierarchy2": raw_table_nvsr71_02_6_1[1][1:]
        }
    )
    table_nvsr71_02_6_1_columns.insert(0, 'Area')
    table_nvsr71_02_6_1 = pdf_parser.to_dataframe(table_nvsr71_02_6_1_columns, raw_table_nvsr71_02_6_1[2:])

    # applying transformations
    table_nvsr71_02_6_1['Area'] = table_nvsr71_02_6_1['Area'].str.replace(r'\.+\s*', ' ', regex=True).str.strip()
    table_nvsr71_02_6_1.replace(r'\…', '', regex=True, inplace=True)

    csv_data_nvsr71_02_6_1 = table_nvsr71_02_6_1.to_csv(index=False)
    ####################################################################################################################

    # page 7
    raw_table_nvsr71_02_7_1_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_blob=pdf_content, page_number=6,
                                             region={'x0': 29.5, 'x1': 301, 'top': 448.1, 'bottom': 760}))
    raw_table_nvsr71_02_7_1_2 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_blob=pdf_content, page_number=6,
                                             region={'x0': 310, 'x1': 568.3, 'top': 448.1, 'bottom': 735}))

    table_nvsr71_02_7_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_nvsr71_02_7_1_1[0]
        }
    )
    table_nvsr71_02_7_1 = pdf_parser.to_dataframe(table_nvsr71_02_7_1_columns,
                                                  raw_table_nvsr71_02_7_1_1[1:] + raw_table_nvsr71_02_7_1_2[1:])
    # applying transformations
    table_nvsr71_02_7_1['Area'] = table_nvsr71_02_7_1['Area'].str.replace(r'\.+\s*', ' ', regex=True).str.strip()
    table_nvsr71_02_7_1.replace(r'…', '', regex=True, inplace=True)

    csv_data_nvsr71_02_7_1 = table_nvsr71_02_7_1.to_csv(index=False)
    ####################################################################################################################
    # uploading blob
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_02/nvsr71_02_3_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_02/nvsr71_02_3_1.csv'")
    else:
        target_blob_client.upload_blob(csv_data_nvsr71_02_3_1, overwrite=True)
    ##

    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_02/nvsr71_02_6_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_02/nvsr71_02_6_1.csv'")
    else:
        target_blob_client.upload_blob(csv_data_nvsr71_02_6_1, overwrite=True)
    ##

    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_02/nvsr71_02_7_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_02/nvsr71_02_7_1.csv'")
    else:
        target_blob_client.upload_blob(csv_data_nvsr71_02_7_1, overwrite=True)


def process_nvsr71_01_pdf():
    nvsr_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-wild',
                                                 blob_name='www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71-01_demarcated.pdf')

    pdf_content = nvsr_blob_client.download_blob().readall()

    # page 2
    raw_table_nvsr71_01_2_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_blob=pdf_content, page_number=1,
                                             region={'x0': 14, 'x1': 590, 'top': 456, 'bottom': 723}))

    raw_table_nvsr71_01_2_1[0][4] = 'Hispanic'
    raw_table_nvsr71_01_2_1[0][7] = 'Non-Hispanic American\nIndian or Alaska Native'
    raw_table_nvsr71_01_2_1[0][10] = 'Non-Hispanic Asian'
    raw_table_nvsr71_01_2_1[0][13] = 'Non-Hispanic Black'
    raw_table_nvsr71_01_2_1[0][16] = 'Non-Hispanic White'

    # # creating column hierarchies
    table_nvsr71_01_2_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_nvsr71_01_2_1[0],
            "hierarchy2": raw_table_nvsr71_01_2_1[1][1:]
        }
    )

    table_nvsr71_01_2_1_columns.insert(0, raw_table_nvsr71_01_2_1[1][0])
    table_nvsr71_01_2_1 = pdf_parser.to_dataframe(table_nvsr71_01_2_1_columns, raw_table_nvsr71_01_2_1[2:])

    # applying transformations
    table_nvsr71_01_2_1['Age (years)'] = table_nvsr71_01_2_1['Age (years)'].str.replace(r'\.+\s*', ' ',
                                                                                        regex=True).str.strip()
    table_nvsr71_01_2_1.replace(r'^\s*\.{2,}+\s*$', '', regex=True, inplace=True)

    csv_data_nvsr71_01_2_1 = table_nvsr71_01_2_1.to_csv(index=False)
    ####################################################################################################################
    # page 3
    raw_table_nvsr71_01_3_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_blob=pdf_content, page_number=2,
                                             region=None))
    raw_table_nvsr71_01_3_1[0][4] = 'Hispanic'
    raw_table_nvsr71_01_3_1[0][7] = 'Non-Hispanic American\nIndian or Alaska Native'
    raw_table_nvsr71_01_3_1[0][10] = 'Non-Hispanic Asian'
    raw_table_nvsr71_01_3_1[0][13] = 'Non-Hispanic Black'
    raw_table_nvsr71_01_3_1[0][16] = 'Non-Hispanic White'
    # creating column hierarchies
    table_nvsr71_01_3_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_nvsr71_01_3_1[0],
            "hierarchy2": raw_table_nvsr71_01_3_1[1][1:]
        }
    )
    table_nvsr71_01_3_1_columns.insert(0, raw_table_nvsr71_01_2_1[1][0])
    table_nvsr71_01_3_1 = pdf_parser.to_dataframe(table_nvsr71_01_3_1_columns, raw_table_nvsr71_01_3_1[2:])
    # applying transformations
    table_nvsr71_01_3_1['Age (years)'] = table_nvsr71_01_3_1['Age (years)'].str.replace(r'\.+\s*', ' ',
                                                                                        regex=True).str.strip()
    table_nvsr71_01_3_1.replace(r'^\s*\.{2,}+\s*$', '', regex=True, inplace=True)
    csv_data_nvsr71_01_3_1 = table_nvsr71_01_3_1.to_csv("table_nvsr71_01_3_1.csv ", index=False)
    ####################################################################################################################

    # uploading blob
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_01/nvsr71_01_2_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_01_2_1.csv'")
    else:
        target_blob_client.upload_blob(csv_data_nvsr71_01_2_1, overwrite=True)
    ##

    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_01/nvsr71_01_3_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_01/nvsr71_01_3_1.csv'")
    else:
        target_blob_client.upload_blob(csv_data_nvsr71_01_3_1, overwrite=True)
    ##


def process_lexport_pdf():
    lexport_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-wild',
                                                    blob_name='www.cdc.gov/nchs/data/hus/2020-2021/LExpMort_demarcated.pdf')

    pdf_content = lexport_blob_client.download_blob().readall()

    # page 1 table 1
    raw_table_LExpMort_1_1_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 0,
                                             {'x0': 21.6, 'x1': 579, 'top': 80, 'bottom': 418}))

    raw_table_LExpMort_1_1_2 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 0,
                                             {'x0': 21.6, 'x1': 579, 'top': 430, 'bottom': 704}))

    # custom modifications
    raw_table_LExpMort_1_1_1[0][5] = 'White'
    raw_table_LExpMort_1_1_1[0][7] = 'Black or African American'

    table_LExpMort_1_1_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_LExpMort_1_1_1[0],
            "hierarchy2": raw_table_LExpMort_1_1_1[1][1:]
        }
    )

    table_LExpMort_1_1_1_columns.insert(0, 'Specified age and year')

    table_LExpMort_1_1_1 = pdf_parser.to_dataframe(table_LExpMort_1_1_1_columns, raw_table_LExpMort_1_1_1[3:])

    table_LExpMort_1_1_1['Specified age and year'] = table_LExpMort_1_1_1['Specified age and year'].str.extract(
        r'(\d{4})')
    table_LExpMort_1_1_1.replace(r'- - -', '', regex=True, inplace=True)
    table_LExpMort_1_1_1['Specified age and year'] = "At_birth_" + table_LExpMort_1_1_1[
        'Specified age and year'].str.replace('At birth ', '')

    table_LExpMort_1_1_2 = pdf_parser.to_dataframe(table_LExpMort_1_1_1_columns, raw_table_LExpMort_1_1_2)

    table_LExpMort_1_1_2.iloc[:, 0] = table_LExpMort_1_1_2.iloc[:, 0].str.extract(r'(\d{4})')
    table_LExpMort_1_1_2.replace(r'- - -', '', regex=True, inplace=True)
    table_LExpMort_1_1_2.iloc[:, 0] = "At_65_years_" + table_LExpMort_1_1_2.iloc[:, 0].str.replace('At 65 years ', '')

    table_LExpMort_1_1 = pd.concat([table_LExpMort_1_1_1, table_LExpMort_1_1_2], axis=0, ignore_index=True)

    csv_data_table_LExpMort_1_1 = table_LExpMort_1_1.to_csv(index=False)
    ####################################################################################################################
    # page 2 table 1
    raw_table_LExpMort_2_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 1,
                                             {'x0': 31, 'x1': 568, 'top': 93.6, 'bottom': 365}))

    # custom modifications
    raw_table_LExpMort_2_1[0][5] = 'white'
    raw_table_LExpMort_2_1[0][7] = 'Black or African American'

    raw_table_LExpMort_2_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_LExpMort_1_1_1[0],
            "hierarchy2": raw_table_LExpMort_1_1_1[1][1:]
        }
    )

    raw_table_LExpMort_2_1_columns.insert(0, 'Specified age and year')
    table_LExpMort_2_1 = pdf_parser.to_dataframe(raw_table_LExpMort_2_1_columns, raw_table_LExpMort_2_1[3:])
    table_LExpMort_2_1['Specified age and year'] = table_LExpMort_2_1['Specified age and year'].str.extract(r'(\d{4})')
    table_LExpMort_2_1.replace(r'- - -', '', regex=True, inplace=True)
    table_LExpMort_2_1['Specified age and year'] = "At_65_years_" + table_LExpMort_2_1[
        'Specified age and year'].str.replace('At 65 years ', '')

    csv_data_table_LExpMort_2_1 = table_LExpMort_2_1.to_csv(index=False)
    ########################################################################################################################
    # page 2 table 2
    raw_table_LExpMort_2_2 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 1,
                                             {'x0': 31, 'x1': 568, 'top': 373, 'bottom': 587}))
    raw_table_LExpMort_2_2[0][1] = 'White_not_Hispanic'
    raw_table_LExpMort_2_2[0][4] = 'Black_not_Hispanic'
    raw_table_LExpMort_2_2[0][7] = 'Hispanic'

    raw_table_LExpMort_2_2_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_LExpMort_2_2[0],
            "hierarchy2": raw_table_LExpMort_2_2[1][1:]
        }
    )
    raw_table_LExpMort_2_2_columns.insert(0, 'Specified age and year')
    table_LExpMort_2_2 = pdf_parser.to_dataframe(raw_table_LExpMort_2_2_columns, raw_table_LExpMort_2_2[3:])

    table_LExpMort_2_2['Specified age and year'] = table_LExpMort_2_2['Specified age and year'].str.extract(r'(\d{4})')
    table_LExpMort_2_2.replace(r'…', '', regex=True, inplace=True)
    table_LExpMort_2_2['Specified age and year'] = "At_birth_" + table_LExpMort_2_2[
        'Specified age and year'].str.replace('At birth ', '')

    csv_data_table_LExpMort_2_2 = table_LExpMort_2_2.to_csv(index=False)
    ####################################################################################################################
    # page 2 table 3
    raw_table_LExpMort_2_3 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 1,
                                             {'x0': 31, 'x1': 568, 'top': 594, 'bottom': 667}))

    raw_table_LExpMort_2_3[0][1] = 'American_Indian_or_Alaska_Native_not_Hispanic'
    raw_table_LExpMort_2_3[0][4] = 'Asian_not_Hispanic'
    raw_table_LExpMort_2_3_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_LExpMort_2_3[0],
            "hierarchy2": raw_table_LExpMort_2_3[1][1:]
        }
    )
    raw_table_LExpMort_2_3_columns.insert(0, 'Specified age and year')
    table_LExpMort_2_3 = pdf_parser.to_dataframe(raw_table_LExpMort_2_3_columns, raw_table_LExpMort_2_3[3:])
    table_LExpMort_2_3.loc[0, 'Specified age and year'] = '2019_single_race'

    csv_data_table_LExpMort_2_3 = table_LExpMort_2_3.to_csv(index=False)
    ####################################################################################################################
    # page 3 table 1
    raw_table_LExpMort_3_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 2, {'x0': 29.8, 'x1': 572, 'top': 93.6, 'bottom': 307}))
    raw_table_LExpMort_3_1[0][1] = 'White_not_Hispanic'
    raw_table_LExpMort_3_1[0][4] = 'Black_not_Hispanic'
    raw_table_LExpMort_3_1[0][7] = 'Hispanic'
    raw_table_LExpMort_3_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_LExpMort_3_1[0],
            "hierarchy2": raw_table_LExpMort_3_1[1][1:]
        }
    )
    raw_table_LExpMort_3_1_columns.insert(0, 'Specified age and year')
    table_LExpMort_3_1 = pdf_parser.to_dataframe(raw_table_LExpMort_3_1_columns, raw_table_LExpMort_3_1[3:])

    # table_LExpMort_1_1
    table_LExpMort_3_1['Specified age and year'] = table_LExpMort_3_1['Specified age and year'].str.extract(r'(\d{4})')
    table_LExpMort_3_1.replace(r'…', '', regex=True, inplace=True)
    table_LExpMort_3_1['Specified age and year'] = "At_65_years_" + table_LExpMort_3_1[
        'Specified age and year'].str.replace('At 65 years ', '')
    table_LExpMort_3_1.loc[13, 'Specified age and year'] = table_LExpMort_3_1.loc[
                                                               13, 'Specified age and year'] + '_single_race'
    table_LExpMort_3_1.loc[15, 'Specified age and year'] = table_LExpMort_3_1.loc[
                                                               15, 'Specified age and year'] + '_single_race'
    csv_data_table_LExpMort_3_1 = table_LExpMort_3_1.to_csv("table_LExpMort_3_1.csv ", index=False)
    ####################################################################################################################
    # page 3 table 2
    raw_table_LExpMort_3_2 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 2,
                                             {'x0': 27.8, 'x1': 438, 'top': 313.9, 'bottom': 385}))
    raw_table_LExpMort_3_2[0][1] = 'American_Indian_or_Alaska_Native_not_Hispanic'
    raw_table_LExpMort_3_2[0][4] = 'Asian_not_Hispanic'

    raw_table_LExpMort_3_2_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_LExpMort_3_2[0],
            "hierarchy2": raw_table_LExpMort_3_2[1][1:]
        }
    )
    raw_table_LExpMort_3_2_columns.insert(0, 'Specified age and year')
    raw_table_LExpMort_3_2 = pdf_parser.to_dataframe(raw_table_LExpMort_3_2_columns, raw_table_LExpMort_3_2[3:])
    raw_table_LExpMort_3_2.loc[0, 'Specified age and year'] = '2019_single_race'
    csv_data_table_LExpMort_3_2 = raw_table_LExpMort_3_2.to_csv(index=False)
    #######################################################################################################################
    # page 3 table 3
    raw_table_LExpMort_3_3 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 2,
                                             {'x0': 28, 'x1': 568, 'top': 392, 'bottom': 606}))
    raw_table_LExpMort_3_3[0][1] = 'White_not_Hispanic'
    raw_table_LExpMort_3_3[0][4] = 'Black_not_Hispanic'
    raw_table_LExpMort_3_3[0][8] = 'Hispanic'
    raw_table_LExpMort_3_3_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_LExpMort_3_3[0],
            "hierarchy2": raw_table_LExpMort_3_3[1][1:]
        }
    )
    raw_table_LExpMort_3_3_columns.insert(0, 'Specified age and year')
    table_LExpMort_3_3 = pdf_parser.to_dataframe(raw_table_LExpMort_3_3_columns, raw_table_LExpMort_3_3[3:])
    table_LExpMort_3_3['Specified age and year'] = table_LExpMort_3_3['Specified age and year'].str.extract(r'(\d{4})')
    table_LExpMort_3_3.replace(r'…', '', regex=True, inplace=True)
    table_LExpMort_3_3['Specified age and year'] = "At_75_years_" + table_LExpMort_3_3[
        'Specified age and year'].str.replace('At 75 years ', '')

    table_LExpMort_3_3.loc[13, 'Specified age and year'] = table_LExpMort_3_3.loc[
                                                               13, 'Specified age and year'] + '_single_race'
    table_LExpMort_3_3.loc[15, 'Specified age and year'] = table_LExpMort_3_3.loc[
                                                               15, 'Specified age and year'] + '_single_race'
    csv_data_table_LExpMort_3_3 = table_LExpMort_3_3.to_csv(index=False)
    ####################################################################################################################
    # page 3 table 4
    raw_table_LExpMort_3_4 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 2,
                                             {'x0': 30, 'x1': 446, 'top': 613, 'bottom': 689}))
    raw_table_LExpMort_3_4[0][1] = 'American_Indian_or_Alaska_Native_not_Hispanic'
    raw_table_LExpMort_3_4[0][4] = 'Asian_not_Hispanic'
    raw_table_LExpMort_3_4_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_LExpMort_3_4[0],
            "hierarchy2": raw_table_LExpMort_3_4[1][1:]
        }
    )
    raw_table_LExpMort_3_4_columns.insert(0, 'Specified age and year')
    table_LExpMort_3_4 = pdf_parser.to_dataframe(raw_table_LExpMort_3_4_columns, raw_table_LExpMort_3_4[3:])
    table_LExpMort_3_4.loc[0, 'Specified age and year'] = 'At_75_years_2019_single_race'
    csv_data_table_LExpMort_3_4 = table_LExpMort_3_4.to_csv(index=False)
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    # upload blobs
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_1_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_1_1.csv'")
    else:
        target_blob_client.upload_blob(csv_data_table_LExpMort_1_1, overwrite=True)
    ##

    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_2_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_2_1.csv'")
    else:
        target_blob_client.upload_blob(csv_data_table_LExpMort_2_1, overwrite=True)

    ##

    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hus/2020-2021/LExpMort'
                                                             '/LExpMort_2_2.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_2_2.csv'")
    else:
        target_blob_client.upload_blob(csv_data_table_LExpMort_2_2, overwrite=True)
    ##

    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_2_3.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_2_3.csv'")
    else:
        target_blob_client.upload_blob(csv_data_table_LExpMort_2_3, overwrite=True)

    ##
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_1.csv'")
    else:
        target_blob_client.upload_blob(csv_data_table_LExpMort_3_1, overwrite=True)
    ##
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_2.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_2.csv'")
    else:
        target_blob_client.upload_blob(csv_data_table_LExpMort_3_2, overwrite=True)
    ##
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_3.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_3.csv'")
    else:
        target_blob_client.upload_blob(csv_data_table_LExpMort_3_3, overwrite=True)
    ##
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_4.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, ''")
    else:
        target_blob_client.upload_blob(csv_data_table_LExpMort_3_4, overwrite=True)


def process_lifeexpectancy_h_pdf():
    lexport_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-wild',
                                                    blob_name='www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy-H_demarcated.pdf')

    pdf_content = lexport_blob_client.download_blob().readall()

    ###################################################################################################################

    # page 3 table 1
    raw_table_lifeexpectancy_h_3_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 2,
                                             {'x0': 62, 'x1': 508, 'top': 70, 'bottom': 186}))
    table_lifeexpectancy_h_3_1 = pdf_parser.to_dataframe(raw_table_lifeexpectancy_h_3_1[0],
                                                         raw_table_lifeexpectancy_h_3_1[1:])

    csv_data_table_lifeexpectancy_h_3_1 = table_lifeexpectancy_h_3_1.to_csv(index=False)
    ####################################################################################################################
    # page 4 table 1
    raw_table_lifeexpectancy_h_4_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 3,
                                             {'x0': 65, 'x1': 540, 'top': 77, 'bottom': 307}))

    raw_table_lifeexpectancy_h_4_1_columns = pdf_parser.create_column_hierarchy_ordered(
        hierarchies={
            "hierarchy1": raw_table_lifeexpectancy_h_4_1[0],
            "hierarchy2": raw_table_lifeexpectancy_h_4_1[1]
        }
    )
    table_lifeexpectancy_h_4_1 = pdf_parser.to_dataframe(raw_table_lifeexpectancy_h_4_1_columns,
                                                         raw_table_lifeexpectancy_h_4_1[
                                                         3:10] + raw_table_lifeexpectancy_h_4_1[
                                                                 11:])
    table_lifeexpectancy_h_4_1.replace(r'\n', ' ', regex=True, inplace=True)

    csv_data_table_lifeexpectancy_h_4_1 = table_lifeexpectancy_h_4_1.to_csv(index=False)
    ####################################################################################################################
    # page 5 table 1
    raw_table_lifeexpectancy_h_5_1 = pdf_parser.filter_empty_or_none_rows(
        pdf_parser.extract_table_from_region(pdf_content, 4,
                                             {'x0': 65, 'x1': 542, 'top': 77, 'bottom': 343}))

    table_lifeexpectancy_h_5_1 = pdf_parser.to_dataframe(raw_table_lifeexpectancy_h_4_1_columns,
                                                         raw_table_lifeexpectancy_h_5_1[
                                                         3:10] + raw_table_lifeexpectancy_h_5_1[
                                                                 11:])

    table_lifeexpectancy_h_5_1.replace(r'\n', ' ', regex=True, inplace=True)

    csv_data_table_lifeexpectancy_h_5_1 = table_lifeexpectancy_h_5_1.to_csv(index=False)
    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy_h_3_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy_h_3_1'")
    else:
        target_blob_client.upload_blob(csv_data_table_lifeexpectancy_h_3_1, overwrite=True)
    ##
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy_h_4_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy_h_4_1'")
    else:
        target_blob_client.upload_blob(csv_data_table_lifeexpectancy_h_4_1, overwrite=True)
    ##
    target_blob_client = client.create_blob_client(blob_account='usafactsdpmanual', container='to-bronze',
                                                   blob_name='www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy_h_5_1.csv')
    if target_blob_client.exists():
        print(f"csv blob already exists, 'www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy_h_5_1'")
    else:
        target_blob_client.upload_blob(csv_data_table_lifeexpectancy_h_5_1, overwrite=True)

def create_and_sequence_dynamic_tasks(**kwargs):
    blob_names = [
        'www.cdc.gov/nchs/data/vsrr/vsrr031_2_1.csv',
        'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_02/nvsr71_02_3_1.csv',
        'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_02/nvsr71_02_6_1.csv',
        'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_02/nvsr71_02_7_1.csv',
        'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_01/nvsr71_01_2_1.csv',
        'www.cdc.gov/nchs/data/nvsr/nvsr71/nvsr71_01/nvsr71_01_3_1.csv',
        'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_1_1.csv',
        'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_2_1.csv',
        'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_2_2.csv',
        'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_2_3.csv',
        'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_1.csv',
        'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_2.csv',
        'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_3.csv',
        'www.cdc.gov/nchs/data/hus/2020-2021/LExpMort/LExpMort_3_4.csv',
        'www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy_h_3_1.csv',
        'www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy_h_4_1.csv',
        'www.cdc.gov/nchs/data/hestat/life-expectancy/lifeexpectancy_h_5_1.csv'
    ]

    def create_task_for_file(blob_name):
        task_id = blob_name.split('/')[-1].replace('.csv', '') + '_to_bronze_TO_bronze'
        blob_processed = blob_name.split('/')[-1].replace('.csv', '')
        return RawToBronzeOperatorManual(
            task_id=task_id,
            pipeline_id='918dc078-bed7-41f9-a730-6385b1ebdc3d',
            created_by='Sharang Kulkarni',
            description=f"to_bronze TO bronze for file {blob_processed}",
            filename=blob_name,
            dag=dag,
        )

    # Dynamically create tasks based on the blob_names list
    dynamic_tasks = [create_task_for_file(blob_name) for blob_name in blob_names]

    return dynamic_tasks


default_args = {
    'owner': 'Sharang Kulkarni',
    'start_date': datetime(2021, 1, 1),
    'retries': 0,
}

dag = DAG(
    'pdf_parsing_flow',
    default_args=default_args,
    description='Fetch files from URLs in a CSV and upload to Azure Blob Storage',
    schedule_interval='@once',
)

check_upload_pdfs_raw = PythonOperator(
    task_id='check_and_upload_raw_pdf_files',
    python_callable=check_and_upload_raw_pdf_files,
    op_kwargs={'azure_storage_account': 'usafactsdpmanual',
               'source_container': 'ingestion-metainfo',
               'source_blob_path': 'scraped_urls/life_expectancy/life_expectancy.csv',
               'target_container': 'to-wild'},
    dag=dag,
)

check_upload_pdfs_demarcated = PythonOperator(
    task_id='check_and_upload_demarcated_pdf_files',
    python_callable=check_and_upload_demarcated_pdf_file,
    op_kwargs={'azure_storage_account': 'usafactsdpmanual',
               'target_container': 'to-wild'},
    dag=dag,
)

process_vssr031_pdf_task = PythonOperator(
    task_id='process_vssr031_pdf',
    python_callable=process_vssr031_pdf,
    dag=dag,
)

process_nvsr71_02_pdf_task = PythonOperator(
    task_id='process_nvsr71_02_pdf',
    python_callable=process_nvsr71_02_pdf,
    dag=dag,
)
process_nvsr71_01_pdf_task = PythonOperator(
    task_id='process_nvsr71_01_pdf',
    python_callable=process_nvsr71_01_pdf,
    dag=dag,
)

process_lexport_pdf_task = PythonOperator(
    task_id='process_lexport_pdf',
    python_callable=process_lexport_pdf,
    dag=dag,
)

process_lifeexpectancy_h_task = PythonOperator(
    task_id='process_lifeexpectancy_h',
    python_callable=process_lifeexpectancy_h_pdf,
    dag=dag,
)


check_upload_pdfs_raw >> check_upload_pdfs_demarcated >> process_vssr031_pdf_task >> process_nvsr71_02_pdf_task >> process_nvsr71_01_pdf_task >> process_lifeexpectancy_h_task >> process_lexport_pdf_task

dynamic_tasks = create_and_sequence_dynamic_tasks()

for task in dynamic_tasks:
    process_lexport_pdf_task.set_downstream(task)

