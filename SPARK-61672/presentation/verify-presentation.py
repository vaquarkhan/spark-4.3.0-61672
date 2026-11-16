"""Validate both generated PPTX/PDF decks. Contact sheets go to the OS temp directory."""
from pathlib import Path
import tempfile
import zipfile
import fitz
from PIL import Image, ImageDraw
from pptx import Presentation

HERE = Path(__file__).resolve().parent
DECKS = [
    ("SPARK-61672-Feature-Overview", 15,
     ["The user experience is one SQL expression", "Calls finish out of order; rows do not",
      "5.79x", "SPARK-61454", "What we are asking the community to agree on"]),
    ("SPARK-61672-POC-Review", 27,
     ["AsyncScalarFunction", "AsyncEvalExec", "FunctionResolution", "OpenAIFunctionCatalog",
      "BedrockFunctionCatalog", "LiveSparkSqlProof", "SPARK-61454", "5.79x"]),
]

for stem, expected_pages, required_text in DECKS:
    pptx = HERE / (stem + ".pptx")
    pdf = HERE / (stem + ".pdf")
    presentation = Presentation(pptx)
    assert len(presentation.slides) == expected_pages, (stem, len(presentation.slides))
    with zipfile.ZipFile(pptx) as package:
        assert package.testzip() is None, stem

    document = fitz.open(pdf)
    assert len(document) == expected_pages, (stem, len(document))
    all_text = "\n".join(page.get_text() for page in document)
    for required in required_text:
        assert required in all_text, (stem, required)

    thumbs = []
    for page in document:
        pix = page.get_pixmap(matrix=fitz.Matrix(0.5, 0.5), alpha=False)
        image = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        image.thumbnail((320, 180))
        thumbs.append(image)
    cols = 3 if expected_pages > 14 else 2
    rows = (len(thumbs) + cols - 1) // cols
    sheet = Image.new("RGB", (cols * 340, rows * 210), "#e9eef5")
    draw = ImageDraw.Draw(sheet)
    for i, image in enumerate(thumbs):
        x = (i % cols) * 340 + 10
        y = (i // cols) * 210 + 22
        sheet.paste(image, (x, y))
        draw.text((x, 4 + (i // cols) * 210), "Slide %d" % (i + 1), fill="#0e1d34")
    sheet_path = Path(tempfile.gettempdir()) / (stem + "-contact-sheet.png")
    sheet.save(sheet_path)
    print("%s: PPTX_OK slides=%d; PDF_OK pages=%d; TEXT_OK; contact=%s" %
          (stem, expected_pages, expected_pages, sheet_path))
