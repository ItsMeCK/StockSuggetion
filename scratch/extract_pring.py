from pypdf import PdfReader
import os

pdf_path = "/Users/poonamsalke/Downloads/Pring on Price Patterns  The Definitive Guide to Price Pattern Analysis and Intrepretation by Martin Pring, Martin Pring (z-lib.org).pdf"
output_path = "scratch/pring_extracted_rules.txt"

reader = PdfReader(pdf_path)
text = ""
# Extract first 50 pages for dense rule finding
for i in range(min(50, len(reader.pages))):
    text += f"\n--- PAGE {i+1} ---\n"
    text += reader.pages[i].extract_text()

with open(output_path, "w") as f:
    f.write(text)

print(f"Extracted {len(text)} characters to {output_path}")
