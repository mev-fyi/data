import os
import fitz
from PIL import Image
from io import BytesIO
from src.utils import root_directory


def generate_pdf_thumbnails(pdf_directory, output_directory, page_fraction=0.4, min_px=600, max_px=900):
    """
    Generate thumbnails for the top portion of the first page of all PDFs in the pdf_directory.
    Thumbnails will have a size between min_px and max_px on the longest side.
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for filename in os.listdir(pdf_directory):
        if filename.lower().endswith('.pdf'):
            pdf_path = os.path.join(pdf_directory, filename)
            thumbnail_path = os.path.join(output_directory, os.path.splitext(filename)[0] + '.png')

            with fitz.open(pdf_path) as doc:
                page = doc[0]  # First page

                # Calculate zoom factors based on the desired pixel size and actual page size
                scale = min_px / min(page.rect.width, page.rect.height)
                if scale * max(page.rect.width, page.rect.height) > max_px:
                    scale = max_px / max(page.rect.width, page.rect.height)

                mat = fitz.Matrix(scale, scale)  # Create the matrix with the zoom scale
                pix = page.get_pixmap(matrix=mat)  # Render page to an image
                img = Image.open(BytesIO(pix.tobytes()))  # Convert to a PIL image

                # Crop the image to the desired page_fraction
                img_cropped = img.crop((0, 0, img.width, int(img.height * page_fraction)))

                # Save the thumbnail
                img_cropped.save(thumbnail_path)
                print(f"Thumbnail generated for {filename} at {thumbnail_path}")


def run():
    pdf_dir = f"{root_directory()}/data/papers_pdf_downloads"
    generate_pdf_thumbnails(pdf_directory=pdf_dir, output_directory=f'{root_directory()}/data/research_papers_pdf_thumbnails')


if __name__ == '__main__':
    run()