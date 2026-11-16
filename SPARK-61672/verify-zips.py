"""Verify the clean POC ZIP and the full Spark source ZIP."""
from pathlib import Path
import os
import re
import zipfile

POC = Path(__file__).resolve().parent
REPO = POC.parent
REVIEW_ZIP = REPO / "SPARK-61672-review-package.zip"
FULL_ZIP = REPO.parent / "SPARK-61672-full-spark-source.zip"
FORBIDDEN_ACCOUNT = os.environ.get("SPARK_61672_FORBIDDEN_ACCOUNT_ID", "").encode()

SECRET_PATTERNS = [
    ("AWS access key", re.compile(rb"(?:AKIA|ASIA)[0-9A-Z]{16}")),
    ("Bedrock bearer", re.compile(rb"bedrock-api-key-[A-Za-z0-9+/=_-]{40,}")),
    ("private key", re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
]
FORBIDDEN_EXT = (".jar", ".class", ".args", ".pyc", ".pyo", ".zip")
FORBIDDEN_PARTS = ("/target/", "/node_modules/", "/.git/")


def verify(path, prefix, required):
    with zipfile.ZipFile(path) as z:
        assert z.testzip() is None, "corrupt member in %s" % path
        names = z.namelist()
        missing = [prefix + x for x in required if prefix + x not in names]
        assert not missing, "missing: %s" % missing
        bad_names = [n for n in names if n.lower().endswith(FORBIDDEN_EXT)
                     or any(part in n for part in FORBIDDEN_PARTS)]
        assert not bad_names, "generated artifacts: %s" % bad_names[:10]
        for name in names:
            if name.endswith("/"):
                continue
            data = z.read(name)
            if FORBIDDEN_ACCOUNT:
                assert FORBIDDEN_ACCOUNT not in data, "local AWS account in %s" % name
            for label, pattern in SECRET_PATTERNS:
                assert not pattern.search(data), "%s material in %s" % (label, name)
        print("%s: ZIP_OK files=%d bytes=%d secrets=0 generated=0" %
              (path.name, len(names), path.stat().st_size))


poc_required = [
    "README.md", "START-HERE.md", "ARCHITECTURE.md", "BUILD-AND-RUN.md",
    "GUIDE-FOR-HOLDEN.md", "preflight.ps1", "run-poc.ps1",
    "proposal/01-SPIP-incremental-for-Holden.md",
    "presentation/SPARK-61672-Feature-Overview.pptx",
    "presentation/SPARK-61672-Feature-Overview.pdf",
    "presentation/SPARK-61672-POC-Review.pptx",
    "presentation/SPARK-61672-POC-Review.pdf",
    "related-work/SPARK-61454-break-stage-barrier.md",
]
full_required = [
    "sql/core/src/main/scala/org/apache/spark/sql/execution/async/AsyncEvalExec.scala",
    "sql/core/src/main/scala/org/apache/spark/sql/execution/async/ExtractAsyncFunctions.scala",
    "sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/functions/AsyncScalarFunction.java",
    "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ApplyAsyncFunctionExpression.scala",
    "SPARK-61672/README.md",
    "SPARK-61672/START-HERE.md",
    "SPARK-61672/preflight.ps1",
    "SPARK-61672/run-poc.ps1",
    "SPARK-61672/presentation/SPARK-61672-Feature-Overview.pptx",
    "SPARK-61672/presentation/SPARK-61672-Feature-Overview.pdf",
    "SPARK-61672/presentation/SPARK-61672-POC-Review.pptx",
    "SPARK-61672/presentation/SPARK-61672-POC-Review.pdf",
]
verify(REVIEW_ZIP, "SPARK-61672/", poc_required)
verify(FULL_ZIP, "spark-4.3.0-SPARK-61672/", full_required)
