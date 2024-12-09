# from azure.storage.blob import BlobServiceClient

# def main(connection_string, container_name, folder_name):
#     blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#     container_client = blob_service_client.get_container_client(container_name)

#     if container_client.exists():
#         blobs = container_client.list_blobs(name_starts_with=folder_name + '/')
#         for blob in blobs:
#             print(blob.name)
#     else:
#         print(f"Container '{container_name}' does not exist.")



# # Input your connection details here
# connection_string = "DefaultEndpointsProtocol=https;AccountName=stginvoicepdf;AccountKey=L+C2a+5LgrnQ3qkbuVCMDmV32GZIP3W26kDdmZar8MVsB/6d0mfH3HRL1dHIe7q2SQeMBP/ymvCE+ASt0HY4Fg==;EndpointSuffix=core.windows.net"
# container_name = "remittancepdfs"
# folder_name = "Remittance PDFs"  # Replace with your folder name

# main(connection_string, container_name, folder_name)
#--------------Working Download-----------
import os
import json
from azure.storage.blob import BlobServiceClient, ContainerClient
from PyPDF2 import PdfReader
from io import BytesIO
import google.generativeai as genai

# Configure Google Gemini API (Hardcoded API Key)
GOOGLE_API_KEY = "AIzaSyDCvBpAvpLeVHf4boPFBfZBfINzfH_ySCo"
genai.configure(api_key=GOOGLE_API_KEY)

# Azure Blob Storage Configuration
connection_string = "DefaultEndpointsProtocol=https;AccountName=stginvoicepdf;AccountKey=L+C2a+5LgrnQ3qkbuVCMDmV32GZIP3W26kDdmZar8MVsB/6d0mfH3HRL1dHIe7q2SQeMBP/ymvCE+ASt0HY4Fg==;EndpointSuffix=core.windows.net"
container_name = "remittancepdfs"
folder_name = "Remittance PDFs"
json_folder_name = "json_response"  # New folder for storing JSON responses

# Function to check container and list files in Azure Blob Storage
def list_files_in_blob(connection_string, container_name, folder_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    if container_client.exists():
        blobs = container_client.list_blobs(name_starts_with=folder_name + '/')
        return [blob.name for blob in blobs]
    else:
        raise Exception(f"Container '{container_name}' does not exist.")

# Function to download PDF from Azure Blob Storage as a byte stream (no local file required)
def download_pdf_from_blob(connection_string, container_name, pdf_blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    blob_client = container_client.get_blob_client(pdf_blob_name)
    return BytesIO(blob_client.download_blob().readall())

# Function to extract text from PDF (using BytesIO stream)
def extract_text_from_pdf(pdf_stream):
    reader = PdfReader(pdf_stream)
    return "".join(page.extract_text() for page in reader.pages)

# Function to get Gemini API response
def get_gemini_response(user_input, pdf_text, prompt):
    model = genai.GenerativeModel('gemini-1.5-flash')
    combined_input = f"I need the response in JSON {user_input}\n\n{prompt.strip()}\n\nText: {pdf_text}"
    response = model.generate_content([combined_input])
    
    if hasattr(response, 'text'):
        return response.text
    else:
        raise Exception("No response received from Gemini API.")

# Function to upload JSON response to Azure Blob Storage
def upload_json_to_blob(connection_string, container_name, json_folder_name, json_data, json_file_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    blob_client = container_client.get_blob_client(f"{json_folder_name}/{json_file_name}")
    blob_client.upload_blob(json_data, overwrite=True)
    print(f"Successfully uploaded JSON response to {json_folder_name}/{json_file_name}")

# Main function to process PDFs
def process_pdfs(connection_string, container_name, folder_name, json_folder_name):
    pdf_files = list_files_in_blob(connection_string, container_name, folder_name)

    if not pdf_files:
        print("No PDFs found in the source folder.")
        return

    input_prompt = """
        You are an expert in extracting data from remittance advice documents.
        For each remittance advice with multiple invoice numbers, create a separate row for each invoice.
        The output should be formatted as JSON and include the following fields:
        Invoice Number, Remittance Date, Remittance Number, Etn No, Payer Name, Payer Address, 
        Payee Name, Payee Address, Contact No, Phone No, Country, Payment Method, Amount Paid, Payment Breakdown, 
        Deductions, Adjustments, Bank Details, Reference Numbers, Payment Terms, Bill Ref, Net Amount, Instrument Date, Instrument No, Mode of payment,
        Currency, Tax Amount, Discount Information, Payment Status, Payment Reason, 
        Vendor ID, Remittance Contact Information, Purchase Order Number, Transaction Type, 
        Adjustment Codes, Memo, Comments, Total, Amount Due, Due, Amount Paid, Due Date, Bank Account, Code, Company Name, Gross Amount, Amount, Sum Total
    """

    for pdf_file in pdf_files:
        if not pdf_file.lower().endswith('.pdf'):
            print(f"Skipping non-PDF file: {pdf_file}")
            continue

        try:
            pdf_stream = download_pdf_from_blob(connection_string, container_name, pdf_file)
            pdf_text = extract_text_from_pdf(pdf_stream)

            user_input = "Extract the remittance advice data"
            response = get_gemini_response(user_input, pdf_text, input_prompt)

            json_file_name = f"{pdf_file.replace('/', '_').replace('.pdf', '')}_response.json"
            upload_json_to_blob(connection_string, container_name, json_folder_name, response, json_file_name)
        except Exception as e:
            print(f"Error processing file {pdf_file}: {str(e)}")

# Example Usage
if __name__ == "__main__":
    process_pdfs(connection_string, container_name, folder_name, json_folder_name)
