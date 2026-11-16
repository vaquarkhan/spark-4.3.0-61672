"""Create a sanitized full Apache Spark source archive with the SPARK-61672 changes and POC.

The archive is written next to the spark-4.3.0 checkout:
  SPARK-61672-full-spark-source.zip

It excludes Git metadata, build output, generated binaries, machine-specific files, credentials,
and upstream synthetic private-key fixtures. See SOURCE-ZIP-MANIFEST.md.
"""
from pathlib import Path
import os
import re
import zipfile

POC = Path(__file__).resolve().parent
SPARK = POC.parent
OUT = SPARK.parent / "SPARK-61672-full-spark-source.zip"
ARCHIVE_ROOT = "spark-4.3.0-SPARK-61672"
REPORT = POC / "FULL-SOURCE-ZIP-REPORT.txt"

SKIP_DIRS = {
    ".git", ".idea", ".vscode", "target", "out", "node_modules", "__pycache__",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", ".sparkStaging", "spark-warehouse",
    "metastore_db", "dist",
}
SKIP_EXTS = {
    ".jar", ".class", ".zip", ".pyc", ".pyo", ".exe", ".dll", ".so", ".dylib",
    ".tmp", ".temp", ".lock",
}
SKIP_NAMES = {
    "spark-runtime-classpath.txt", "bedrock-bearer.txt", "probe.json",
    "dependency-reduced-pom.xml", "SPARK-61672-POC-Review-contact-sheet.png",
}
TEXT_EXTS = {
    ".scala", ".java", ".py", ".md", ".txt", ".xml", ".json", ".yaml", ".yml",
    ".properties", ".conf", ".sh", ".cmd", ".bat", ".ps1", ".sql", ".csv",
    ".html", ".js", ".ts", ".css", ".toml", ".ini", ".gradle", "",
}
# Credential VALUE patterns only. Environment-variable names such as AWS_PROFILE are allowed.
SECRET_PATTERNS = [
    ("AWS access key", re.compile(rb"(?:AKIA|ASIA)[0-9A-Z]{16}")),
    ("Bedrock bearer key", re.compile(rb"bedrock-api-key-[A-Za-z0-9+/=_-]{40,}")),
    ("private key", re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("AWS account ARN", re.compile(rb"arn:aws(?:-[a-z]+)?:[^:\s]+:[^:\s]*:\d{12}:")),
]


def reason_to_skip(path: Path, rel: Path):
    if path.resolve() == REPORT.resolve():
        return "regenerated ZIP report"
    if path.name.startswith("~$"):
        return "Office lock file"
    if path.name in SKIP_NAMES or path.suffix.lower() in SKIP_EXTS:
        return "generated or machine-specific artifact"
    if path.suffix.lower() == ".log" and not str(rel).replace("\\", "/").startswith("SPARK-61672/"):
        return "non-POC log"
    if path.is_symlink():
        return "symlink"
    # Scan text and small fixture files. Skip anything with credential or private-key material.
    try:
        if path.suffix.lower() in TEXT_EXTS or path.stat().st_size <= 2 * 1024 * 1024:
            data = path.read_bytes()
            for label, pattern in SECRET_PATTERNS:
                if pattern.search(data):
                    return label
    except OSError as exc:
        return "unreadable: %s" % exc
    return None


included = []
skipped = []
for dirpath, dirnames, filenames in os.walk(SPARK, followlinks=False):
    current = Path(dirpath)
    dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not (current / d).is_symlink()]
    for filename in filenames:
        path = current / filename
        rel = path.relative_to(SPARK)
        reason = reason_to_skip(path, rel)
        if reason:
            skipped.append((str(rel).replace("\\", "/"), reason))
        else:
            included.append((path, rel))

# Write the report before the zip and ensure the report itself is included.
lines = [
    "SPARK-61672 full source ZIP report",
    "source: %s" % SPARK,
    "archive: %s" % OUT,
    "included files: %d" % (len(included) + 1),
    "skipped files: %d" % len(skipped),
    "",
    "Skipped files (path | reason):",
]
lines.extend("%s | %s" % item for item in skipped)
REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
included.append((REPORT, REPORT.relative_to(SPARK)))

with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
    for path, rel in included:
        arcname = str(Path(ARCHIVE_ROOT) / rel).replace("\\", "/")
        archive.write(path, arcname)

with zipfile.ZipFile(OUT) as archive:
    bad = archive.testzip()
    names = archive.namelist()
    if bad is not None:
        raise RuntimeError("ZIP integrity failure: %s" % bad)
    required = [
        "sql/core/src/main/scala/org/apache/spark/sql/execution/async/AsyncEvalExec.scala",
        "sql/core/src/main/scala/org/apache/spark/sql/execution/async/ExtractAsyncFunctions.scala",
        "sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/functions/AsyncScalarFunction.java",
        "SPARK-61672/README.md",
        "SPARK-61672/START-HERE.md",
        "SPARK-61672/preflight.ps1",
        "SPARK-61672/run-poc.ps1",
        "SPARK-61672/presentation/SPARK-61672-Feature-Overview.pptx",
        "SPARK-61672/presentation/SPARK-61672-Feature-Overview.pdf",
        "SPARK-61672/presentation/SPARK-61672-POC-Review.pptx",
        "SPARK-61672/presentation/SPARK-61672-POC-Review.pdf",
        "SPARK-61672/SOURCE-ZIP-MANIFEST.md",
    ]
    missing = [r for r in required if (ARCHIVE_ROOT + "/" + r) not in names]
    if missing:
        raise RuntimeError("missing required entries: %s" % missing)
    forbidden_suffixes = tuple(SKIP_EXTS)
    forbidden = [n for n in names if n.lower().endswith(forbidden_suffixes)]
    if forbidden:
        raise RuntimeError("forbidden generated binaries in ZIP: %s" % forbidden[:10])

print("wrote %s" % OUT)
print("files=%d skipped=%d bytes=%d" % (len(included), len(skipped), OUT.stat().st_size))
print("zip_ok=True required_ok=True generated_binaries=0")
