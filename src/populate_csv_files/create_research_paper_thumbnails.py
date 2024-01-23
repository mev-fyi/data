import os
import fitz
from PIL import Image
from io import BytesIO
from src.utils import root_directory


def generate_pdf_thumbnails(pdf_directory, output_directory, page_fraction=0.5, min_px=400, max_px=1500, zoom_factor=1.5):
    """
    Generate thumbnails for the top portion of the first page of all PDFs in the pdf_directory, then apply zooming.
    Thumbnails will have a size between min_px and max_px on the longest side after zooming.
    Args:
        pdf_directory (str): Directory containing PDF files.
        output_directory (str): Directory to save the thumbnails.
        page_fraction (float): Fraction of the page to be shown in the thumbnail (0 to 1).
        min_px (int): Minimum pixel size for the thumbnail's longest side.
        max_px (int): Maximum pixel size for the thumbnail's longest side.
        zoom_factor (float): Factor by which to zoom into the cropped thumbnail.
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for filename in os.listdir(pdf_directory):
        if filename.lower().endswith('.pdf'):
            pdf_path = os.path.join(pdf_directory, filename)
            thumbnail_path = os.path.join(output_directory, os.path.splitext(filename)[0] + '.png')

            with fitz.open(pdf_path) as doc:
                page = doc[0]  # First page

                # Calculate initial scale to fit the min_px requirement
                initial_scale = min_px / min(page.rect.width, page.rect.height * page_fraction)
                mat = fitz.Matrix(initial_scale, initial_scale)  # Create the matrix with the initial scale
                pix = page.get_pixmap(matrix=mat)  # Render page to an image
                img = Image.open(BytesIO(pix.tobytes()))  # Convert to a PIL image

                # Crop the image to the desired page_fraction
                img_cropped = img.crop((0, 0, img.width, int(img.height * page_fraction)))

                # Calculate the final size with zoom factor
                final_width = int(img_cropped.width * zoom_factor)
                final_height = int(img_cropped.height * zoom_factor)

                # Ensure final dimensions are within the min_px and max_px bounds
                if final_width > max_px or final_height > max_px:
                    scale_factor = min(max_px / final_width, max_px / final_height)
                    final_width = int(final_width * scale_factor)
                    final_height = int(final_height * scale_factor)

                # Resize image to final dimensions
                img_final = img_cropped.resize((final_width, final_height), Image.Resampling.LANCZOS)

                # Save the final thumbnail
                img_final.save(thumbnail_path)
                print(f"Thumbnail generated for {filename} at {thumbnail_path}")


def run():
    pdf_dir = f"{root_directory()}/data/papers_pdf_downloads"
    generate_pdf_thumbnails(pdf_directory=pdf_dir, output_directory=f'{root_directory()}/data/research_papers_pdf_thumbnails')


if __name__ == '__main__':
    run()