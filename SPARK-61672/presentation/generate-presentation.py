"""Generate the SPARK-61672 POC review deck.

Output:
  SPARK-61672-POC-Review.pptx

The deck is built only from verified local source, logs, and diagrams. It contains no secret,
credential, or generated provider jar.
"""
from pathlib import Path
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE, MSO_CONNECTOR
from pptx.dml.color import RGBColor
from pptx.enum.dml import MSO_THEME_COLOR

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "presentation" / "SPARK-61672-POC-Review.pptx"
D = ROOT / "diagrams"
PD = ROOT / "proposal" / "diagrams"

# Palette
NAVY = RGBColor(14, 29, 52)
NAVY2 = RGBColor(22, 44, 74)
BLUE = RGBColor(43, 105, 172)
CYAN = RGBColor(37, 174, 190)
PURPLE = RGBColor(111, 78, 172)
GREEN = RGBColor(43, 143, 95)
ORANGE = RGBColor(235, 137, 48)
RED = RGBColor(197, 68, 68)
WHITE = RGBColor(249, 251, 254)
PALE = RGBColor(239, 244, 250)
MID = RGBColor(113, 132, 154)
DARK = RGBColor(30, 42, 56)
BLACK = RGBColor(16, 22, 29)

prs = Presentation()
prs.slide_width = Inches(13.333)
prs.slide_height = Inches(7.5)
blank = prs.slide_layouts[6]


def rect(slide, x, y, w, h, fill, line=None, radius=False):
    shape = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE if radius else MSO_SHAPE.RECTANGLE,
        Inches(x), Inches(y), Inches(w), Inches(h))
    shape.fill.solid(); shape.fill.fore_color.rgb = fill
    shape.line.color.rgb = line or fill
    if radius:
        shape.adjustments[0] = 0.08
    return shape


def text(slide, value, x, y, w, h, size=18, color=DARK, bold=False,
         font="Aptos", align=PP_ALIGN.LEFT, valign=MSO_ANCHOR.TOP, margin=0.07):
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = box.text_frame; tf.clear(); tf.word_wrap = True
    tf.margin_left = tf.margin_right = Inches(margin)
    tf.margin_top = tf.margin_bottom = Inches(margin)
    tf.vertical_anchor = valign
    p = tf.paragraphs[0]; p.alignment = align
    r = p.add_run(); r.text = value
    r.font.name = font; r.font.size = Pt(size); r.font.bold = bold; r.font.color.rgb = color
    return box


def rich_text(slide, runs, x, y, w, h, size=18, color=DARK, font="Aptos",
              align=PP_ALIGN.LEFT, valign=MSO_ANCHOR.TOP):
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = box.text_frame; tf.clear(); tf.word_wrap = True
    tf.margin_left = tf.margin_right = Inches(0.08)
    tf.margin_top = tf.margin_bottom = Inches(0.05)
    tf.vertical_anchor = valign
    p = tf.paragraphs[0]; p.alignment = align
    for value, bold, clr in runs:
        r = p.add_run(); r.text = value
        r.font.name = font; r.font.size = Pt(size); r.font.bold = bold
        r.font.color.rgb = clr or color
    return box


def bullets(slide, items, x, y, w, h, size=18, color=DARK, bullet_color=None,
            gap=7, font="Aptos"):
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = box.text_frame; tf.clear(); tf.word_wrap = True
    tf.margin_left = Inches(0.10); tf.margin_right = Inches(0.05)
    tf.margin_top = tf.margin_bottom = Inches(0.03)
    for i, item in enumerate(items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = "- " + item; p.level = 0; p.space_after = Pt(gap); p.font.name = font
        p.font.size = Pt(size); p.font.color.rgb = color
    # accent rail
    rect(slide, x, y + 0.06, 0.06, min(h - 0.12, len(items) * 0.56), bullet_color or CYAN)
    return box


def add_base(title, section="SPARK-61672 POC", dark=False):
    slide = prs.slides.add_slide(blank)
    bg = slide.background.fill; bg.solid(); bg.fore_color.rgb = NAVY if dark else WHITE
    rect(slide, 0, 0, 13.333, 0.10, ORANGE)
    text(slide, section.upper(), 0.50, 0.20, 3.0, 0.28, 9, CYAN if dark else BLUE, True)
    text(slide, title, 0.50, 0.52, 12.1, 0.62, 27, WHITE if dark else NAVY, True)
    # footer
    rect(slide, 0.50, 7.14, 12.30, 0.012, MID)
    text(slide, "Native async functions | Spark 4.3.0 working POC | Local, uncommitted",
         0.52, 7.20, 10.8, 0.18, 8, RGBColor(170, 184, 201) if dark else MID)
    text(slide, str(len(prs.slides)), 12.35, 7.17, 0.4, 0.22, 8,
         RGBColor(170, 184, 201) if dark else MID, True, align=PP_ALIGN.RIGHT)
    return slide


def card(slide, x, y, w, h, heading, body, accent=BLUE, big=None, dark=False):
    fill = NAVY2 if dark else PALE
    c = rect(slide, x, y, w, h, fill, RGBColor(62, 83, 109) if dark else RGBColor(215, 225, 237), True)
    rect(slide, x, y, 0.08, h, accent)
    if big:
        text(slide, big, x + 0.25, y + 0.20, w - 0.4, 0.48, 25, accent, True)
        text(slide, heading, x + 0.25, y + 0.72, w - 0.4, 0.30, 12,
             WHITE if dark else NAVY, True)
        text(slide, body, x + 0.25, y + 1.05, w - 0.4, h - 1.16, 10.5,
             RGBColor(204, 217, 231) if dark else DARK)
    else:
        text(slide, heading, x + 0.25, y + 0.18, w - 0.4, 0.38, 15,
             WHITE if dark else NAVY, True)
        text(slide, body, x + 0.25, y + 0.64, w - 0.4, h - 0.76, 11.5,
             RGBColor(204, 217, 231) if dark else DARK)
    return c


def table(slide, rows, cols, x, y, w, h, widths=None, font_size=10.5,
          header_fill=NAVY2, header_color=WHITE, fills=(WHITE, PALE)):
    tbl = slide.shapes.add_table(rows, cols, Inches(x), Inches(y), Inches(w), Inches(h)).table
    if widths:
        for i, v in enumerate(widths): tbl.columns[i].width = Inches(v)
    for r in range(rows):
        for c in range(cols):
            cell = tbl.cell(r, c)
            cell.margin_left = cell.margin_right = Inches(0.06)
            cell.margin_top = cell.margin_bottom = Inches(0.03)
            cell.fill.solid(); cell.fill.fore_color.rgb = header_fill if r == 0 else fills[(r-1) % len(fills)]
            cell.border_left = None if hasattr(cell, 'border_left') else None
            tf = cell.text_frame; tf.word_wrap = True; tf.vertical_anchor = MSO_ANCHOR.MIDDLE
            for p in tf.paragraphs:
                p.font.name = "Aptos"; p.font.size = Pt(font_size if r else font_size + 0.5)
                p.font.bold = (r == 0); p.font.color.rgb = header_color if r == 0 else DARK
    return tbl


def set_cell(tbl, r, c, value, bold=False, color=None, size=None):
    cell = tbl.cell(r, c); cell.text = value
    for p in cell.text_frame.paragraphs:
        p.font.name = "Aptos"; p.font.bold = bold
        if size: p.font.size = Pt(size)
        if color: p.font.color.rgb = color


def pill(slide, label, x, y, w, fill, color=WHITE):
    rect(slide, x, y, w, 0.33, fill, fill, True)
    text(slide, label, x, y + 0.02, w, 0.22, 9, color, True, align=PP_ALIGN.CENTER)


def arrow(slide, x1, y1, x2, y2, color=BLUE, width=2.5):
    line = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1), Inches(x2), Inches(y2))
    line.line.color.rgb = color; line.line.width = Pt(width)
    line.line.end_arrowhead = True
    return line

# 1 --------------------------------------------------------------------------------------------
slide = prs.slides.add_slide(blank)
slide.background.fill.solid(); slide.background.fill.fore_color.rgb = NAVY
rect(slide, 0, 0, 0.18, 7.5, ORANGE)
text(slide, "SPARK-61672", 0.75, 0.62, 3.0, 0.35, 13, CYAN, True)
text(slide, "Provider-neutral\nasynchronous scalar functions", 0.75, 1.15, 8.6, 1.55, 36, WHITE, True)
text(slide, "A working Apache Spark SQL POC, from public SPI to real model calls",
     0.77, 2.92, 9.5, 0.52, 19, RGBColor(190, 208, 226))
pill(slide, "REAL SPARK SQL", 0.78, 3.70, 1.75, BLUE)
pill(slide, "2 PROVIDERS", 2.68, 3.70, 1.65, PURPLE)
pill(slide, "REAL BEDROCK", 4.48, 3.70, 1.72, GREEN)
pill(slide, "5.79x", 6.35, 3.70, 1.08, ORANGE)
text(slide, "Holden Karau  |  Prashant Singh  |  Vaquar Khan", 0.78, 5.35, 7.8, 0.36, 15, WHITE, True)
text(slide, "Incremental implementation of Holden and Prashant's initial SPARK-61672 direction",
     0.78, 5.83, 8.8, 0.40, 12, RGBColor(174, 194, 216))
text(slide, "Related: SPARK-61454, Break the Stage Barrier (complementary, independent)",
     0.78, 6.38, 9.6, 0.30, 11, CYAN)
text(slide, "POC review package | 15 November 2026", 10.0, 6.92, 2.55, 0.25, 9, MID, align=PP_ALIGN.RIGHT)

# 2 --------------------------------------------------------------------------------------------
slide = add_base("What is real today", "Executive summary", dark=True)
metrics = [
    ("25 / 25", "engine tests", "6 end-to-end, 7 parity, 12 safety", CYAN),
    ("314 / 314", "existing Catalyst regressions", "0 failed, 1 ignored", BLUE),
    ("71 / 71", "provider conformance", "7 cases call real services", PURPLE),
    ("25 / 25", "one-process live SQL proof", "both packaged modules", GREEN),
    ("5.79x", "real Bedrock speedup", "24 / 24 correct, ordered", ORANGE),
    ("35", "review findings fixed", "17 + 12 + 6 across 3 rounds", RED),
]
for i, (big, head, body, acc) in enumerate(metrics):
    col, row = i % 3, i // 3
    card(slide, 0.55 + col*4.16, 1.45 + row*2.25, 3.85, 1.82, head, body, acc, big, True)
text(slide, "Baseline: Apache Spark v4.3.0 working tree. Nothing committed or pushed.",
     0.58, 6.30, 11.8, 0.35, 13, WHITE, True, align=PP_ALIGN.CENTER)

# 3 --------------------------------------------------------------------------------------------
slide = add_base("The problem and the deliberate design choice", "Why")
card(slide, 0.55, 1.42, 5.9, 4.85, "Status quo: a UDF waits", "", RED)
bullets(slide, [
    "One task thread waits on each remote call, wasting the latency window.",
    "One terminal call failure can retry the whole partition and repeat paid calls.",
    "Python-only implementations do not give SQL, Scala, Java, and Connect one backend.",
    "Every team reinvents credentials, destinations, retry, limits, errors, and cancellation.",
], 0.85, 2.18, 5.25, 3.55, 16, DARK, RED)
card(slide, 6.85, 1.42, 5.9, 4.85, "Choice: one native async contract", "", GREEN)
bullets(slide, [
    "Spark owns planning, bounds, row order, spill, and task cancellation.",
    "Provider modules own credentials, wire formats, retry, and response parsing.",
    "Administrators own allowlists and cost ceilings; sessions can only tighten them.",
    "No cloud SDK enters Spark core; the same SQL can switch providers.",
], 7.15, 2.18, 5.25, 3.55, 16, DARK, GREEN)

# 4 --------------------------------------------------------------------------------------------
slide = add_base("Architecture: clean separation between Spark and providers", "Architecture")
img = D / "arch-01-layering.png"
slide.shapes.add_picture(str(img), Inches(0.65), Inches(1.28), width=Inches(12.0), height=Inches(5.55))

# 5 --------------------------------------------------------------------------------------------
slide = add_base("One SQL expression becomes a bounded physical operator", "Query lifecycle")
img = D / "arch-02-lifecycle.png"
slide.shapes.add_picture(str(img), Inches(0.55), Inches(1.34), width=Inches(12.22), height=Inches(5.42))

# 6 --------------------------------------------------------------------------------------------
slide = add_base("Two independent bounds preserve order without unbounded memory", "Execution")
card(slide, 0.62, 1.42, 3.65, 2.00, "Active-call bound", "How many provider futures may be running. Per-provider limit, clamped by a static cluster ceiling.", BLUE, "C", False)
card(slide, 4.52, 1.42, 3.65, 2.00, "Reorder-window bound", "How many submitted rows may wait behind the next row to emit. Prevents a stalled head from growing memory.", PURPLE, "W", False)
card(slide, 8.42, 1.42, 3.65, 2.00, "Spillable input", "HybridRowQueue stores projected input and spills. Completed futures do not keep unbounded rows on heap.", GREEN, "DISK", False)
# visual timeline
text(slide, "Calls complete out of order", 0.70, 4.02, 3.1, 0.28, 13, NAVY, True)
for i, clr in enumerate([ORANGE, CYAN, GREEN, PURPLE, BLUE]):
    rect(slide, 3.55+i*1.30, 3.92, 0.86, 0.55, clr, clr, True)
    text(slide, str([3,1,5,2,4][i]), 3.55+i*1.30, 4.05, 0.86, 0.22, 13, WHITE, True, align=PP_ALIGN.CENTER)
text(slide, "Results emit in input order", 0.70, 5.20, 3.1, 0.28, 13, NAVY, True)
for i, clr in enumerate([CYAN, PURPLE, ORANGE, BLUE, GREEN]):
    rect(slide, 3.55+i*1.30, 5.10, 0.86, 0.55, clr, clr, True)
    text(slide, str(i+1), 3.55+i*1.30, 5.23, 0.86, 0.22, 13, WHITE, True, align=PP_ALIGN.CENTER)
text(slide, "N calls, concurrency C: about ceil(N / C) x provider latency, not max(latency).",
     0.72, 6.28, 11.8, 0.33, 14, DARK, True, align=PP_ALIGN.CENTER)

# 7 --------------------------------------------------------------------------------------------
slide = add_base("Modified Spark files: exact folder, owner class, and change", "Complete change map 1 / 5")
rows = [
 ("sql/catalyst/.../analysis/FunctionResolution.scala", "class FunctionResolution", "Resolve AsyncScalarFunction from FunctionCatalog"),
 ("sql/catalyst/.../optimizer/Optimizer.scala", "object LimitPushDown", "Push LocalLimit below EvalAsyncFunctions"),
 ("sql/catalyst/.../optimizer/Optimizer.scala", "object PushPredicateThroughNonJoin", "Push child-only filters below async evaluation"),
 ("sql/catalyst/.../trees/TreePatterns.scala", "object TreePattern", "ASYNC_FUNCTION and EVAL_ASYNC_FUNCTIONS"),
 ("sql/catalyst/.../internal/SQLConf.scala", "object SQLConf", "Static active ceiling; reorder window; poll interval"),
 ("sql/core/.../execution/SparkOptimizer.scala", "class SparkOptimizer", "Register extraction and aggregate-aware rule"),
 ("sql/core/.../execution/SparkStrategies.scala", "object AsyncFunctionEvals", "Plan EvalAsyncFunctions to AsyncEvalExec"),
 ("sql/core/.../execution/SparkPlanner.scala", "class SparkPlanner", "Register AsyncFunctionEvals strategy"),
 ("hadoop-cloud/pom.xml", "Maven module", "Test-only Catalyst test-jar dependency"),
]
tbl = table(slide, len(rows)+1, 3, 0.45, 1.28, 12.45, 5.72, [4.25, 3.15, 5.05], 10.5)
for c,h in enumerate(["Modified file", "Class / object", "Change"]): set_cell(tbl,0,c,h)
for r,row in enumerate(rows,1):
    for c,v in enumerate(row): set_cell(tbl,r,c,v, color=BLUE if c==1 else None)

# 8 --------------------------------------------------------------------------------------------
slide = add_base("New Catalyst API and plan classes", "Complete change map 2 / 5")
rows = [
 ("sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/functions/", "AsyncScalarFunction.java", "interface AsyncScalarFunction<R>", "Public @Evolving SPI: future, concurrency, determinism, canonical identity, close"),
 ("sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/", "ApplyAsyncFunctionExpression.scala", "ApplyAsyncFunctionExpression; AsyncFunctionIdentity", "Unevaluable call; deterministic/non-deterministic canonicalization"),
 ("sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/", "asyncFunctionLogicalOperators.scala", "EvalAsyncFunctions", "One-to-one logical operator; appends result attributes"),
]
tbl = table(slide, 4, 4, 0.45, 1.38, 12.45, 4.92, [3.55,2.25,2.70,3.95], 10.5)
for c,h in enumerate(["Folder", "File", "Class / interface", "Responsibility"]): set_cell(tbl,0,c,h)
for r,row in enumerate(rows,1):
    for c,v in enumerate(row): set_cell(tbl,r,c,v, color=PURPLE if c==2 else None)
card(slide, 0.75, 6.00, 11.85, 0.90, "Public contract", "Row in -> CompletableFuture out. Generative providers report non-deterministic; Spark never calls the provider from expression eval or codegen.", CYAN)

# 9 --------------------------------------------------------------------------------------------
slide = add_base("New SQL execution classes", "Complete change map 3 / 5")
rows = [
 ("sql/core/src/main/scala/org/apache/spark/sql/execution/async/", "AsyncEvalExec.scala", "case class AsyncEvalExec; object AsyncEvalExec", "Physical operator, iterator, bounds, queue, cancellation, cleanup, metrics"),
 ("sql/core/src/main/scala/org/apache/spark/sql/execution/async/", "ExtractAsyncFunctions.scala", "object ExtractAsyncFunctions", "Innermost-first extraction for projections and filters"),
 ("same file", "ExtractAsyncFunctions.scala", "object ExtractAsyncFunctionsFromAggregate", "Place async call above aggregates it consumes"),
]
tbl = table(slide, 4, 4, 0.45, 1.34, 12.45, 4.75, [3.45,2.25,3.20,3.55], 11)
for c,h in enumerate(["Folder", "File", "Class / object", "Responsibility"]): set_cell(tbl,0,c,h)
for r,row in enumerate(rows,1):
    for c,v in enumerate(row): set_cell(tbl,r,c,v, color=BLUE if c==2 else None)
text(slide, "Operator invariants", 0.72, 6.22, 2.0, 0.3, 14, NAVY, True)
pill(slide, "1 row in = 1 row out", 2.55, 6.20, 2.05, BLUE)
pill(slide, "input order", 4.78, 6.20, 1.42, PURPLE)
pill(slide, "bounded", 6.38, 6.20, 1.15, GREEN)
pill(slide, "spillable", 7.71, 6.20, 1.15, CYAN)
pill(slide, "cancellable", 9.04, 6.20, 1.35, ORANGE)
pill(slide, "at-least-once", 10.57, 6.20, 1.55, RED)

# 10 -------------------------------------------------------------------------------------------
slide = add_base("New Spark test and live-proof classes", "Complete change map 4 / 5")
rows = [
 ("sql/core/src/test/.../execution/async/", "AsyncFunctionEndToEndSuite.scala", "AsyncFunctionEndToEndSuite; GenerateUnbound; HttpGenerateFunction", "6 real Spark SQL end-to-end cases"),
 ("sql/core/src/test/.../execution/async/", "AiQueryParitySuite.scala", "AiQueryParitySuite; AiQueryUnbound; AiQueryFunction", "7 Databricks/Bedrock envelope and error-policy cases"),
 ("sql/core/src/test/.../execution/async/", "AsyncFunctionSafetySuite.scala", "AsyncFunctionSafetySuite + 9 helper classes/objects", "12 optimizer, limit, identity, config, failure, cleanup, cancellation cases"),
 ("hadoop-cloud/src/test/.../execution/async/", "BedrockLiveAsyncFunctionSuite.scala", "BedrockLiveAsyncFunctionSuite; BedrockConverseUnbound; BedrockConverseFunction", "Real SQL planner -> AsyncEval -> AWS SDK v2 Converse"),
]
tbl = table(slide, 5, 4, 0.38, 1.30, 12.57, 5.88, [2.90,2.55,3.65,3.47], 9.6)
for c,h in enumerate(["Folder", "File", "Classes / objects", "Purpose"]): set_cell(tbl,0,c,h)
for r,row in enumerate(rows,1):
    for c,v in enumerate(row): set_cell(tbl,r,c,v, color=GREEN if c==2 else None)

# 11 -------------------------------------------------------------------------------------------
slide = add_base("Safety test helper classes, all named", "Complete change map 4a / 5")
helpers = [
 ("SafetyState", "Atomic call, active, peak, close, cancel counters"),
 ("LongToStringUnbound / LongToStringFunction", "Aggregate-result argument coverage"),
 ("SafetyUnbound", "Wrap a bound test function in a catalog function"),
 ("CountingFunction", "Call count and deterministic/non-deterministic behavior"),
 ("SynchronousFailureFunction", "Synchronous throw and null-future paths"),
 ("SlowCountingFunction", "Global and per-provider in-flight ceilings"),
 ("CloseCountingFunction", "One close per shared provider identity"),
 ("CancellableFunction", "Task cancellation reaches source future"),
]
tbl=table(slide,len(helpers)+1,2,0.75,1.35,11.85,5.65,[4.75,7.10],12)
set_cell(tbl,0,0,"Class / object"); set_cell(tbl,0,1,"What it proves")
for r,(a,b) in enumerate(helpers,1): set_cell(tbl,r,0,a,color=PURPLE); set_cell(tbl,r,1,b)

# 12 -------------------------------------------------------------------------------------------
slide = add_base("OpenAI-compatible provider: folders and classes", "Complete change map 5 / 5")
rows=[
 (".../sql/ai/openai/OpenAIFunctions.scala", "ErrorMode + Fail / Null / Status", "Three SQL forms"),
 ("same", "OpenAISettings; OpenAIConf", "Immutable settings; cluster allowlists and ceilings"),
 ("same", "OpenAIFunctionCatalog", "CatalogPlugin / FunctionCatalog entry point"),
 ("same", "GenerateUnbound; GenerateBound", "Type binding; AsyncScalarFunction implementation"),
 ("same", "OpenAIFunctions; ChatResult; OpenAIException", "Status rows; safe result metadata; sanitized errors"),
 (".../sql/ai/openai/OpenAIClient.scala", "OpenAIClient", "JDK HTTP, Jackson, gzip, retry, deadline, cancel"),
 ("same", "Pacer; CircuitBreaker", "Two-layer rate pacing; generation-stamped half-open breaker"),
 ("same", "Classification + Ok / Retryable / Terminal", "Stable error taxonomy"),
]
tbl=table(slide,len(rows)+1,3,0.45,1.28,12.45,5.90,[4.15,3.75,4.55],10.2)
for c,h in enumerate(["File", "Class / object", "Responsibility"]): set_cell(tbl,0,c,h)
for r,row in enumerate(rows,1):
    for c,v in enumerate(row): set_cell(tbl,r,c,v,color=BLUE if c==1 else None)

# 13 -------------------------------------------------------------------------------------------
slide = add_base("Bedrock provider and proof programs: folders and classes", "Complete change map 5a / 5")
rows=[
 (".../sql/ai/bedrock/BedrockFunctions.scala", "ErrorMode; BedrockSettings; BedrockConf", "Forms and cluster policy"),
 ("same", "BedrockFunctionCatalog", "FunctionCatalog entry point"),
 ("same", "GenerateUnbound; GenerateBound", "Binding and async SPI implementation"),
 ("same", "BedrockFunctions; ConverseResult; BedrockException", "Status rows, request id, safe errors"),
 (".../sql/ai/bedrock/BedrockClient.scala", "BedrockClient; CircuitBreaker", "AWS SDK v2 async Converse; per-request timeouts; breaker"),
 ("spark-implementation/conformance/", "ProviderConformanceHarness", "71 checks; 7 live against real services"),
 ("same", "LiveSparkSqlProof", "Both jars through real Spark SQL in one JVM; 25 checks"),
 ("same", "LiveThroughputBenchmark", "Real Bedrock, in-flight 1 vs 8"),
]
tbl=table(slide,len(rows)+1,3,0.45,1.28,12.45,5.90,[4.35,3.35,4.75],10.2)
for c,h in enumerate(["File / folder", "Class / object", "Responsibility"]): set_cell(tbl,0,c,h)
for r,row in enumerate(rows,1):
    for c,v in enumerate(row): set_cell(tbl,r,c,v,color=GREEN if c==1 else None)

# 14 -------------------------------------------------------------------------------------------
slide = add_base("Trust boundary: sessions may tighten, never widen", "Security")
img = D / "arch-03-config-boundary.png"
slide.shapes.add_picture(str(img), Inches(0.55), Inches(1.35), width=Inches(12.20), height=Inches(3.45))
card(slide, 0.70, 5.10, 3.75, 1.30, "Cluster-only", "Allowlists, credential source, breaker policy, every numeric ceiling.", RED)
card(slide, 4.78, 5.10, 3.75, 1.30, "Session/catalog", "Choose only an allowlisted target; lower limits only.", BLUE)
card(slide, 8.86, 5.10, 3.75, 1.30, "Executor", "Resolve credentials locally; no secret in SQL, plan, error, or package.", GREEN)

# 15 -------------------------------------------------------------------------------------------
slide = add_base("One capability, three explicit error semantics", "User contract")
forms=[
 ("ai.generate(prompt)", "STRICT", "Terminal error fails the task. Spark retry behavior is visible and documented.", RED),
 ("ai.try_generate(prompt)", "TOLERANT", "Documented row-level error returns NULL. Auth, config, TLS, bad model remain fatal.", BLUE),
 ("ai.generate_status(prompt)", "OBSERVABLE", "Struct: status, result, safe code/message, attempts where known, request id.", GREEN),
]
for i,(fn,label,body,acc) in enumerate(forms):
    card(slide,0.65+i*4.18,1.50,3.80,3.45,fn,body,acc,label)
text(slide,"At-least-once is not hidden",0.70,5.45,3.2,0.35,17,NAVY,True)
bullets(slide,[
 "Task retry, stage retry, speculation, executor loss, or a repeated action may repeat paid calls.",
 "NULL input passes through without a call. Non-deterministic model calls are never deduplicated.",
],3.65,5.30,8.75,1.15,13.5,DARK,ORANGE)

# 16 -------------------------------------------------------------------------------------------
slide = add_base("Both packaged modules ran through real Spark SQL", "Real end-to-end proof")
card(slide,0.55,1.38,5.95,3.75,"Bedrock catalog","",GREEN)
bullets(slide,[
 "BedrockFunctionCatalog loaded from spark.sql.catalog.bedrock",
 "AWS SDK v2 Converse, Nova Micro, us-east-1",
 "Optimized: EvalAsyncFunctions; executed: AsyncEval",
 "Rows: A, B, NULL",
 "Service request id: 191c242a-11e5-41c2-964f-289f4ef6a5f5",
],0.85,2.03,5.25,2.75,12.5,DARK,GREEN,4)
card(slide,6.80,1.38,5.95,3.75,"OpenAI-compatible catalog","",PURPLE)
bullets(slide,[
 "OpenAIFunctionCatalog loaded from spark.sql.catalog.oai",
 "Bedrock OpenAI-compatible API, Qwen3 32B, bearer token",
 "Optimized: EvalAsyncFunctions; executed: AsyncEval",
 "Rows: A, B, NULL",
 "Service request id: ff08cf3c-fc68-4712-b9b5-af075bec46a7",
],7.10,2.03,5.25,2.75,12.5,DARK,PURPLE,4)
text(slide,"25 / 25 checks passed in one JVM. No test catalog. No test function. Both jars loaded by configuration.",
     0.65,5.55,12.0,0.55,16,NAVY,True,align=PP_ALIGN.CENTER)
text(slide,"Evidence: spark-implementation/live-spark-sql-proof.log",0.65,6.24,12.0,0.30,11,MID,align=PP_ALIGN.CENTER)

# 17 -------------------------------------------------------------------------------------------
slide = add_base("Real example: Spark summarized the review rows", "Real output")
rows=[
 ("101","5","Battery lasted all day and setup was simple.","Long battery, easy setup."),
 ("103","5","Clear instructions and fast performance.","Fast, clear guidance."),
 ("104","5","NULL","NULL, no call"),
 ("105","3","The screen is bright but the speaker is weak.","Loud screen, quiet speaker."),
 ("106","5","Exceeded my expectations for the price.","Great value, exceeded expectations."),
 ("108","4","Comfortable to hold and easy to clean.","Easy-grip, easy-clean design"),
 ("109","4","NULL","NULL, no call"),
 ("110","5","Works well with my existing accessories.","Compatible with my current gear."),
]
tbl=table(slide,len(rows)+1,4,0.40,1.28,12.55,5.55,[0.65,0.65,6.00,5.25],10.2)
for c,h in enumerate(["ID","Stars","Input review","Real Bedrock summary"]): set_cell(tbl,0,c,h)
for r,row in enumerate(rows,1):
    for c,v in enumerate(row): set_cell(tbl,r,c,v,color=GREEN if c==3 else None)
text(slide,"Rows 102 and 107 were filtered below AsyncEval and never billed.",0.55,6.92,12.0,0.24,11,NAVY,True,align=PP_ALIGN.CENTER)

# 18 -------------------------------------------------------------------------------------------
slide = add_base("Real throughput result: 5.79x on Amazon Bedrock", "Performance")
# bars proportional to times; lower is better
text(slide,"Same Spark SQL | 24 prompts | one task | same model | only in-flight limit changed",0.60,1.25,12.0,0.35,14,NAVY,True,align=PP_ALIGN.CENTER)
# baseline bar
text(slide,"In-flight 1 (serial)",0.70,2.13,2.05,0.35,15,DARK,True)
rect(slide,2.75,2.08,8.15,0.70,RED,RED,True)
text(slide,"8,200 ms  |  2.93 rows/s",3.05,2.27,6.8,0.26,14,WHITE,True)
text(slide,"In-flight 8 (async)",0.70,3.38,2.05,0.35,15,DARK,True)
rect(slide,2.75,3.33,1.41,0.70,GREEN,GREEN,True)
text(slide,"1,416 ms",4.38,3.50,1.75,0.28,14,GREEN,True)
# speedup card
card(slide,7.45,4.35,4.65,1.45,"Serial / async wall time","24 / 24 correct in both runs. Input order preserved.",ORANGE,"5.79x")
card(slide,0.75,4.35,5.95,1.45,"What this proves","Overlapping real model calls cuts task wall time without sacrificing row/result association.",BLUE)
text(slide,"Single developer-machine sample; absolute numbers vary with network and service load. Full Pandas/Arrow comparison remains open.",0.72,6.32,11.85,0.45,11,MID,align=PP_ALIGN.CENTER)

# 19 -------------------------------------------------------------------------------------------
slide = add_base("Validation is layered, not one happy-path demo", "Evidence")
rows=[
 ("Engine feature suites","25 / 25","Planner, operator, order, bounds, pushdown, failure, cancel"),
 ("Existing Catalyst regression suites","314 / 314","31 suites around optimizer and tree rules; 1 ignored"),
 ("Provider conformance","71 / 71","Real module code; 7 cases live against services"),
 ("One-process real SQL proof","25 / 25","Both jars, real planner, real services, request ids, sample summaries"),
 ("Live planner integration","1 / 1","Spark SQL -> AsyncEval -> AWS SDK Converse"),
 ("Real throughput benchmark","PASS","5.79x; 24 / 24 correct and ordered"),
 ("Maven module builds","2 / 2","ai-openai and ai-bedrock poms executed successfully"),
]
tbl=table(slide,len(rows)+1,3,0.65,1.34,12.0,5.65,[3.65,1.65,6.70],11.2)
for c,h in enumerate(["Evidence layer","Result","Covers"]): set_cell(tbl,0,c,h)
for r,row in enumerate(rows,1):
    for c,v in enumerate(row): set_cell(tbl,r,c,v,color=GREEN if c==1 else None,bold=(c==1))

# 20 -------------------------------------------------------------------------------------------
slide = add_base("Where provider configuration goes", "Run configuration")
# Locations
pill(slide, "conf/spark-defaults.conf", 0.65, 1.25, 2.60, BLUE)
pill(slide, "spark-submit / spark-sql --conf", 3.47, 1.25, 3.15, PURPLE)
pill(slide, "SparkSession.builder before getOrCreate", 6.84, 1.25, 3.75, GREEN)
text(slide, "Set cluster controls before Spark starts. A session may only tighten supported limits.",
     0.70, 1.75, 11.85, 0.35, 13, NAVY, True, align=PP_ALIGN.CENTER)
# Bedrock config
rect(slide, 0.55, 2.25, 6.00, 3.85, PALE, GREEN, True)
text(slide, "Amazon Bedrock", 0.82, 2.47, 5.40, 0.35, 18, GREEN, True)
bedrock = """spark.sql.catalog.ai=org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog
spark.sql.catalog.ai.region=us-east-1
spark.sql.catalog.ai.modelId=amazon.nova-micro-v1:0
spark.ai.bedrock.allowedModels=amazon.nova-micro-v1:0
spark.ai.bedrock.allowedRegions=us-east-1
spark.sql.asyncFunction.maxInFlightCeiling=16"""
text(slide, bedrock, 0.80, 2.95, 5.45, 2.15, 10.5, DARK, font="Aptos Mono")
text(slide, "Credentials: AWS default chain on every executor (role, workload identity, or caller-selected profile). Never put keys in Spark config.",
     0.80, 5.22, 5.48, 0.65, 10.5, NAVY, True)
# OpenAI config
rect(slide, 6.78, 2.25, 6.00, 3.85, PALE, PURPLE, True)
text(slide, "OpenAI-compatible", 7.05, 2.47, 5.40, 0.35, 18, PURPLE, True)
openai = """spark.sql.catalog.ai=org.apache.spark.sql.ai.openai.OpenAIFunctionCatalog
spark.sql.catalog.ai.baseUrl=https://approved.example/v1
spark.sql.catalog.ai.chatModel=approved-model
spark.ai.openai.allowedBaseUrls=https://approved.example/v1
spark.ai.openai.allowedModels=approved-model
spark.ai.openai.credentialEnv=AI_PROVIDER_TOKEN"""
text(slide, openai, 7.03, 2.95, 5.47, 2.15, 10.1, DARK, font="Aptos Mono")
text(slide, "Credentials: set AI_PROVIDER_TOKEN in every executor environment. The variable name is cluster-only; a query cannot choose another secret.",
     7.03, 5.22, 5.48, 0.65, 10.5, NAVY, True)
text(slide, "Same SQL for either catalog:  SELECT ai.generate(prompt)",
     0.70, 6.42, 11.9, 0.35, 14, BLUE, True, align=PP_ALIGN.CENTER, font="Aptos Mono")

# 21 -------------------------------------------------------------------------------------------
slide = add_base("SPARK-61454 is complementary, not a dependency", "Related work")
card(slide,0.60,1.42,5.85,3.65,"SPARK-61672: async functions","Overlap remote service calls inside one task. Provider futures, reorder window, spillable input, credentials, limits, per-row errors.",BLUE,"IN-TASK")
card(slide,6.88,1.42,5.85,3.65,"SPARK-61454: Break the Stage Barrier","Pipeline records across eligible shuffle stage boundaries. Connected-stage admission, reader reservations, producer waves, route credits, lifecycle.",PURPLE,"CROSS-STAGE")
arrow(slide,5.95,3.25,6.85,3.25,ORANGE,3)
text(slide,"Future composition",5.15,2.72,3.05,0.35,12,ORANGE,True,align=PP_ALIGN.CENTER)
bullets(slide,[
 "Neither SPIP blocks or replaces the other.",
 "Combined path is not claimed here. It needs joint bounds, progress, cancellation, generation fencing, retry semantics, and cleanup qualification.",
],0.85,5.32,11.65,1.05,13.5,DARK,ORANGE)
text(slide,"JIRA: https://issues.apache.org/jira/browse/SPARK-61454",0.75,6.68,11.8,0.24,10.5,BLUE,True,align=PP_ALIGN.CENTER)

# 22 -------------------------------------------------------------------------------------------
slide = add_base("What is in the share package", "Package map")
folders=[
 ("proposal/","SPIP, comparison, tutorial, examples, real output report, diagram",BLUE),
 ("spark-implementation/","Provider source, poms, conformance/live proof/benchmark source and logs",GREEN),
 ("poc/","Engine test and live planner logs",PURPLE),
 ("semantic-review/","Three adversarial review records",ORANGE),
 ("related-work/","SPARK-61454 summary and integration boundary",CYAN),
 ("presentation/","This PPTX, PDF, and generator",RED),
 ("ARCHITECTURE.md","Layering, lifecycle, trust boundary, provider internals",BLUE),
 ("BUILD-AND-RUN.md","Independent build and run path",GREEN),
 ("GUIDE-FOR-HOLDEN.md","Folder-by-folder handoff",PURPLE),
]
for i,(name,body,acc) in enumerate(folders):
    col,row=i%3,i//3
    card(slide,0.50+col*4.25,1.30+row*1.72,3.95,1.45,name,body,acc)
text(slide,"Source-only share: generated jars, target/class output, machine classpaths, args, tokens, and probe files are removed before zip.",0.60,6.58,12.0,0.40,11.5,NAVY,True,align=PP_ALIGN.CENTER)

# 23 -------------------------------------------------------------------------------------------
slide = add_base("Independent run path", "Build and run")
steps=[
 ("1","Build Spark engine","build/mvn -pl sql/catalyst,sql/core -am install -DskipTests",BLUE),
 ("2","Build provider module","mvn -f SPARK-61672/spark-implementation/connector/ai-bedrock/pom.xml package",PURPLE),
 ("3","Put provider + AWS SDK on classpath","Use the generated provider jar plus bedrockruntime and netty-nio-client 2.35.4",GREEN),
 ("4","Configure catalog and ceilings","spark.sql.catalog.ai=...BedrockFunctionCatalog; spark.ai.bedrock.allowedModels=...",ORANGE),
 ("5","Run SQL","SELECT ai.generate('Reply with exactly READY')",CYAN),
]
for i,(num,head,body,acc) in enumerate(steps):
    y=1.30+i*1.05
    rect(slide,0.65,y,0.62,0.62,acc,acc,True); text(slide,num,0.65,y+0.12,0.62,0.25,15,WHITE,True,align=PP_ALIGN.CENTER)
    text(slide,head,1.48,y,2.65,0.33,14,NAVY,True)
    text(slide,body,4.05,y,8.45,0.57,11.2,DARK,font="Aptos Mono")
text(slide,"Full commands and Windows notes: BUILD-AND-RUN.md",0.70,6.68,11.9,0.28,12,BLUE,True,align=PP_ALIGN.CENTER)

# 24 -------------------------------------------------------------------------------------------
slide = add_base("POC boundaries: stated, not hidden", "Scope")
card(slide,0.60,1.40,3.80,3.95,"Ready now","Real batch SQL POC; two provider modules; local[2] end-to-end; Maven builds; source and reproduction docs.",GREEN,"YES")
card(slide,4.76,1.40,3.80,3.95,"Not claimed","Exactly-once calls; streaming; multi-node qualification; live Databricks; full Pandas/Arrow comparison.",ORANGE,"NO")
card(slide,8.92,1.40,3.80,3.95,"Important baseline","Built in the Spark v4.3.0 source tree. Standard Spark 4.3.x binaries do not contain this SPI/operator; build this checkout.",BLUE,"SOURCE")
text(slide,"POC conclusion",0.70,5.72,2.25,0.35,15,NAVY,True)
text(slide,"The code, module builds, real model calls, plans, request IDs, outputs, and performance result are real. The remaining work is qualification and community alignment, not filling in a scaffold.",2.72,5.57,9.65,0.78,15,DARK,True)

# 25 -------------------------------------------------------------------------------------------
slide = add_base("Decision requested from Holden", "Next step", dark=True)
text(slide,"Do we agree on the mechanism?",0.72,1.35,7.2,0.55,29,WHITE,True)
bullets(slide,[
 "Provider-neutral AsyncScalarFunction in Catalyst",
 "Dedicated AsyncEvalExec operator: bounded, ordered, spillable, cancellable",
 "Cloud SDKs and credentials stay in provider modules",
 "Administrator allowlists and ceilings form the security and cost boundary",
 "SPARK-61454 remains independent; future composition requires separate qualification",
],0.95,2.15,8.05,3.55,17,RGBColor(218,228,240),CYAN,10)
card(slide,9.25,1.65,3.25,1.60,"If yes","Split into reviewable changes: SPI/plan, operator/tests, provider modules, docs/benchmark.",GREEN,dark=True)
card(slide,9.25,3.62,3.25,1.60,"If not","Keep the evidence, revise names and boundaries, and avoid committing to the wrong public API.",ORANGE,dark=True)
text(slide,"SPARK-61672 | Working tree and review package available locally",0.85,6.48,11.7,0.35,13,CYAN,True,align=PP_ALIGN.CENTER)

# 26 appendix ----------------------------------------------------------------------------------
slide = add_base("Appendix: exact test classes", "Class inventory")
rows=[
 ("AsyncFunctionEndToEndSuite.scala","AsyncFunctionEndToEndSuite; GenerateUnbound; HttpGenerateFunction"),
 ("AiQueryParitySuite.scala","AiQueryParitySuite; AiQueryUnbound; AiQueryFunction"),
 ("AsyncFunctionSafetySuite.scala","AsyncFunctionSafetySuite; SafetyState; LongToStringUnbound; LongToStringFunction; SafetyUnbound; CountingFunction; SynchronousFailureFunction; SlowCountingFunction; CloseCountingFunction; CancellableFunction"),
 ("BedrockLiveAsyncFunctionSuite.scala","BedrockLiveAsyncFunctionSuite; BedrockConverseUnbound; BedrockConverseFunction"),
]
tbl=table(slide,5,2,0.55,1.30,12.25,5.90,[3.35,8.90],10.6)
set_cell(tbl,0,0,"File"); set_cell(tbl,0,1,"All top-level classes / objects")
for r,(a,b) in enumerate(rows,1): set_cell(tbl,r,0,a,color=BLUE); set_cell(tbl,r,1,b)

# 27 appendix ----------------------------------------------------------------------------------
slide = add_base("Appendix: configuration keys added or enforced", "Configuration inventory")
rows=[
 ("spark.sql.asyncFunction.maxInFlightCeiling","Static Spark SQLConf; cluster cannot be widened by session"),
 ("spark.sql.asyncFunction.reorderWindow","Bound submitted rows waiting for ordered output"),
 ("spark.sql.asyncFunction.pollIntervalMs","Bound polling interval while waiting"),
 ("spark.ai.openai.allowedBaseUrls / allowedModels","Cluster allowlists"),
 ("spark.ai.openai.credentialEnv","Cluster-only credential source name"),
 ("spark.ai.openai.maxResponseBytes / maxAttempts / maxElapsedMs / requestTimeoutMs / ratePermitsPerSec","Cluster ceilings; catalog may only tighten"),
 ("spark.ai.openai.breakerFailureThreshold / breakerResetMs","Cluster-only breaker policy"),
 ("spark.ai.bedrock.allowedModels / allowedRegions","Cluster allowlists"),
 ("spark.ai.bedrock.maxTokens / apiCallTimeoutMs / apiCallAttemptTimeoutMs","Cluster ceilings; catalog may only tighten"),
 ("spark.ai.bedrock.breakerFailureThreshold / breakerResetMs","Cluster-only breaker policy"),
]
tbl=table(slide,len(rows)+1,2,0.55,1.28,12.25,5.90,[5.70,6.55],10.5)
set_cell(tbl,0,0,"Configuration"); set_cell(tbl,0,1,"Meaning")
for r,(a,b) in enumerate(rows,1): set_cell(tbl,r,0,a,color=PURPLE); set_cell(tbl,r,1,b)

# Sanity: set core properties
prs.core_properties.title = "SPARK-61672 Provider-neutral asynchronous scalar functions"
prs.core_properties.subject = "Working Apache Spark SQL POC review"
prs.core_properties.author = "Holden Karau; Prashant Singh; Vaquar Khan"
prs.core_properties.keywords = "Apache Spark, SPIP, AsyncScalarFunction, AsyncEvalExec, Bedrock, OpenAI"
prs.core_properties.comments = "Generated from verified local source and logs. No credentials included."

# Optional diagnostic: save only the first N slides to isolate PowerPoint compatibility issues.
import os
max_slides = int(os.environ.get("SPARK_61672_PPT_MAX_SLIDES", "0"))
if max_slides > 0:
    while len(prs.slides) > max_slides:
        slide_id = prs.slides._sldIdLst[-1]
        rel_id = slide_id.rId
        prs.part.drop_rel(rel_id)
        del prs.slides._sldIdLst[-1]

OUT.parent.mkdir(parents=True, exist_ok=True)
prs.save(OUT)
print(f"wrote {OUT}")
print(f"slides={len(prs.slides)} bytes={OUT.stat().st_size}")
