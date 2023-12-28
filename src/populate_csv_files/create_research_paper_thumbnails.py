import os

import fitz

from src.utils import root_directory


def generate_pdf_thumbnails(pdf_directory, output_directory, thumb_scale=2, page_fraction=0.6):
    """
    Generate high-resolution thumbnails for a portion of the first page of all PDFs.
    Args:
    pdf_directory (str): Directory containing PDF files.
    output_directory (str): Directory to save the thumbnails.
    thumb_scale (float): Scale factor for the thumbnail resolution.
    page_fraction (float): Fraction of the page to be shown in the thumbnail (0 to 1).
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    for filename in os.listdir(pdf_directory):
        if filename.lower().endswith('.pdf'):
            pdf_path = os.path.join(pdf_directory, filename)
            thumbnail_path = os.path.join(output_directory, os.path.splitext(filename)[0] + '.png')
            # Open the PDF
            with fitz.open(pdf_path) as doc:
                page = doc.load_page(0)  # First page
                # Define the rectangle for the specified portion of the page
                portion_rect = fitz.Rect(0, 0, page.rect.width, page.rect.height * page_fraction)
                # Create a pixmap for the specified rectangle with increased resolution
                matrix = fitz.Matrix(thumb_scale, thumb_scale).prescale(portion_rect.width / page.rect.width, portion_rect.width / page.rect.width)
                pix = page.get_pixmap(matrix=matrix, clip=portion_rect)
                pix.save(thumbnail_path)
                print(f"High-resolution thumbnail generated for {filename} at {thumbnail_path}")


def run():
    pdf_dir = f"{root_directory()}/data/papers_pdf_downloads"
    generate_pdf_thumbnails(pdf_directory=pdf_dir, output_directory=f'{root_directory()}/data/research_papers_pdf_thumbnails')


if __name__ == '__main__':
    run()