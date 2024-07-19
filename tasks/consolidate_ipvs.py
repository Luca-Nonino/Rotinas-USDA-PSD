import os
import csv
from datetime import datetime
def consolidate_ipv_files():
    # Define the structure of the output CSV
    headers = ["<cod>", "<data>", "<ARH>", "<BES>", "<ENS>", "<FSC>", "<FAR>", "<MYI>", "<PRO>", "<TIU>", "<TYI>", "<TTC>", "<TTD>", "<TTS>", "<YLD>", "<MYE>", "<TYE>", "<CIU>", "<CRU>", "<MIU>", "<FWD>", "<TDC>", "<CYI>", "<EXR>", "<MIE>", "<SME>", "<CEU>", "<CYE>", "<MEE>", "<FDC>", "<IDC>", "<CFR>", "<ARP>", "<SLR>", "<TRE>"]

    # Get current date to construct input and output paths
    now = datetime.now()
    current_year_month = now.strftime("%Y_%m")
    input_dir = f"data/ipvs/{current_year_month}"
    output_dir = f"data/ipvs/{current_year_month}/final"
    output_file_path = os.path.join(output_dir, f"USDA_PSD_{current_year_month}.ipv")

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()

        # Process each file in the input directory
        for filename in os.listdir(input_dir):
            if filename.endswith(".ipv"):
                file_path = os.path.join(input_dir, filename)
                with open(file_path, 'r', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        # Fill missing columns with empty values
                        for header in headers:
                            if header not in row:
                                row[header] = ''
                        writer.writerow(row)

        # Remove files that begin with USDA from the input directory
        for filename in os.listdir(input_dir):
            if filename.startswith("USDA"):
                os.remove(os.path.join(input_dir, filename))

if __name__ == "__main__":
    consolidate_ipv_files()