"""Zip the SPARK-61672 review package. Run from anywhere:
    python SPARK-61672/make-zip.py
Produces SPARK-61672-review-package.zip in the Spark repo root (next to the SPARK-61672 folder).
Skips compiled output, generated provider jars, machine-specific classpaths, temporary argument files,
Office lock files, local credentials/probes, and the optional visual-QA contact sheet.
"""
import os
import sys
import zipfile

here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(here)
out = os.path.join(root, "SPARK-61672-review-package.zip")
skip_dirs = {"target", "node_modules", "out", "__pycache__", ".pytest_cache"}
skip_ext = {".class", ".jar", ".args", ".pyc", ".zip"}
skip_names = {
    "spark-runtime-classpath.txt",
    "bedrock-bearer.txt",
    "probe.json",
    "SPARK-61672-POC-Review-contact-sheet.png",
}

count = 0
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    for dirpath, dirnames, filenames in os.walk(here):
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]
        for name in filenames:
            if (name.startswith("~$") or name in skip_names or
                    os.path.splitext(name)[1] in skip_ext):
                continue
            full = os.path.join(dirpath, name)
            arc = os.path.join("SPARK-61672", os.path.relpath(full, here))
            try:
                z.write(full, arc)
                count += 1
            except OSError as e:
                print("skipped (locked): %s (%s)" % (arc, e), file=sys.stderr)

print("wrote %s" % out)
print("files: %d  size: %.1f KB" % (count, os.path.getsize(out) / 1024.0))
