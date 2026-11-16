"""Generate the concise, feature-focused SPARK-61672 deck."""
from pathlib import Path
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE, MSO_CONNECTOR
from pptx.dml.color import RGBColor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "presentation" / "SPARK-61672-Feature-Overview.pptx"
D = ROOT / "diagrams"
NAVY=RGBColor(14,29,52); NAVY2=RGBColor(22,44,74); WHITE=RGBColor(249,251,254)
PALE=RGBColor(239,244,250); DARK=RGBColor(30,42,56); MID=RGBColor(108,126,148)
BLUE=RGBColor(43,105,172); CYAN=RGBColor(37,174,190); PURPLE=RGBColor(111,78,172)
GREEN=RGBColor(43,143,95); ORANGE=RGBColor(235,137,48); RED=RGBColor(197,68,68)
prs=Presentation(); prs.slide_width=Inches(13.333); prs.slide_height=Inches(7.5); blank=prs.slide_layouts[6]

def rect(s,x,y,w,h,fill,line=None,round=False):
    q=s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE if round else MSO_SHAPE.RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    q.fill.solid(); q.fill.fore_color.rgb=fill; q.line.color.rgb=line or fill
    if round: q.adjustments[0]=0.08
    return q

def txt(s,v,x,y,w,h,size=20,color=DARK,bold=False,align=PP_ALIGN.LEFT,valign=MSO_ANCHOR.TOP,font="Aptos"):
    b=s.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h)); f=b.text_frame
    f.clear(); f.word_wrap=True; f.vertical_anchor=valign
    f.margin_left=f.margin_right=Inches(.06); f.margin_top=f.margin_bottom=Inches(.03)
    p=f.paragraphs[0]; p.alignment=align; r=p.add_run(); r.text=v
    r.font.name=font; r.font.size=Pt(size); r.font.bold=bold; r.font.color.rgb=color
    return b

def base(title,section="FEATURE",dark=False):
    s=prs.slides.add_slide(blank); s.background.fill.solid(); s.background.fill.fore_color.rgb=NAVY if dark else WHITE
    rect(s,0,0,13.333,.10,ORANGE); txt(s,section,0.55,.22,2.2,.25,9,CYAN if dark else BLUE,True)
    txt(s,title,0.55,.52,12.0,.62,28,WHITE if dark else NAVY,True)
    rect(s,.55,7.12,12.2,.012,MID); txt(s,"SPARK-61672 | Feature overview",.58,7.19,4.5,.18,8,MID)
    txt(s,str(len(prs.slides)),12.35,7.17,.35,.20,8,MID,True,PP_ALIGN.RIGHT)
    return s

def card(s,x,y,w,h,title,body,accent=BLUE,big=None,dark=False):
    fill=NAVY2 if dark else PALE; rect(s,x,y,w,h,fill,RGBColor(70,90,115) if dark else RGBColor(213,224,236),True); rect(s,x,y,.08,h,accent)
    if big:
        txt(s,big,x+.26,y+.20,w-.42,.54,27,accent,True); ty=y+.82
    else: ty=y+.20
    txt(s,title,x+.26,ty,w-.42,.40,16,WHITE if dark else NAVY,True)
    txt(s,body,x+.26,ty+.52,w-.42,h-(ty-y)-.63,12.5,RGBColor(205,218,233) if dark else DARK)

def bullets(s,items,x,y,w,h,size=18,color=DARK,accent=CYAN):
    rect(s,x,y,.06,h,accent)
    b=s.shapes.add_textbox(Inches(x+.18),Inches(y),Inches(w-.18),Inches(h)); f=b.text_frame; f.clear(); f.word_wrap=True
    f.margin_left=f.margin_right=Inches(.04); f.margin_top=f.margin_bottom=Inches(.02)
    for i,item in enumerate(items):
        p=f.paragraphs[0] if i==0 else f.add_paragraph(); p.text="- "+item; p.space_after=Pt(10)
        p.font.name="Aptos"; p.font.size=Pt(size); p.font.color.rgb=color

def arrow(s,x1,y1,x2,y2,color=BLUE):
    q=s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, Inches(x1),Inches(y1),Inches(x2),Inches(y2)); q.line.color.rgb=color; q.line.width=Pt(3); q.line.end_arrowhead=True

def pill(s,v,x,y,w,fill):
    rect(s,x,y,w,.38,fill,fill,True); txt(s,v,x,y+.07,w,.20,10,WHITE,True,PP_ALIGN.CENTER)

# 1 Title
s=prs.slides.add_slide(blank); s.background.fill.solid(); s.background.fill.fore_color.rgb=NAVY
rect(s,0,0,.20,7.5,ORANGE); txt(s,"SPARK-61672",.78,.62,3,.35,14,CYAN,True)
txt(s,"Call AI from Spark SQL\nwithout blocking one row at a time",.78,1.25,10.8,1.45,38,WHITE,True)
txt(s,"Provider-neutral async functions, bounded execution, real model proof",.80,2.95,10.4,.48,20,RGBColor(192,210,229))
pill(s,"SAME SQL",.82,3.75,1.55,BLUE); pill(s,"ANY PROVIDER",2.60,3.75,1.90,PURPLE); pill(s,"INPUT ORDER",4.73,3.75,1.68,GREEN); pill(s,"BOUNDED",6.65,3.75,1.42,ORANGE)
txt(s,"A feature overview for community discussion",.82,5.50,7.4,.40,17,WHITE,True)
txt(s,"Holden Karau | Prashant Singh | Vaquar Khan",.82,6.04,7.2,.32,13,CYAN)

# 2 User experience
s=base("The user experience is one SQL expression","USER")
txt(s,"SELECT",.85,1.48,1.6,.42,20,MID,True,font="Aptos Mono")
txt(s,"id, ai.generate(review)",.85,1.98,6.30,.45,25,NAVY,True,font="Aptos Mono")
txt(s,"AS summary",.85,2.44,3.20,.40,21,BLUE,True,font="Aptos Mono")
txt(s,"FROM reviews",.85,2.98,5.0,.36,18,MID,True,font="Aptos Mono")
txt(s,"WHERE stars = 5",.85,3.41,5.0,.36,18,MID,True,font="Aptos Mono")
txt(s,"LIMIT 100;",.85,3.84,4.0,.36,18,MID,True,font="Aptos Mono")
arrow(s,7.0,3.0,8.35,3.0,GREEN)
card(s,8.55,1.55,3.75,3.25,"What Spark guarantees","Filtered/limited rows are not sent. NULL makes no call. Calls overlap. Results return in input order. A killed task asks the provider call to cancel.",GREEN,"NATIVE")
txt(s,"Provider changes are configuration, not pipeline rewrites.",.82,5.60,11.8,.48,18,BLUE,True,PP_ALIGN.CENTER)

# 3 problem
s=base("Remote calls do not behave like normal Spark expressions","PROBLEM")
card(s,.65,1.45,3.75,4.45,"Latency varies","One response may take 200 ms; another takes seconds. Serial UDF calls leave task threads waiting.",RED,"WAIT")
card(s,4.78,1.45,3.75,4.45,"Failures spend money","A terminal call fails the task. Spark may retry the partition and repeat calls that already succeeded.",ORANGE,"RETRY")
card(s,8.90,1.45,3.75,4.45,"Every team reinvents","Credentials, endpoint policy, limits, retry, errors, and cancellation differ across providers and languages.",PURPLE,"DRIFT")
txt(s,"The feature creates one execution contract for this workload.",.75,6.35,11.8,.40,17,NAVY,True,PP_ALIGN.CENTER)

# 4 simple architecture
s=base("Spark owns execution; providers own the wire","DESIGN")
steps=[("SQL","ai.generate(review)",BLUE),("CATALYST","Resolve + extract",CYAN),("ASYNC OPERATOR","Bound + order + spill",GREEN),("PROVIDER","Auth + request + retry",PURPLE),("MODEL","Real endpoint",ORANGE)]
for i,(a,b,c) in enumerate(steps):
    x=.45+i*2.55; rect(s,x,2.10,2.20,1.42,PALE,c,True); txt(s,a,x,2.31,2.20,.25,12,c,True,PP_ALIGN.CENTER); txt(s,b,x+.10,2.74,2.0,.45,15,NAVY,True,PP_ALIGN.CENTER)
    if i<4: arrow(s,x+2.22,2.81,x+2.48,2.81,MID)
card(s,.70,4.45,5.70,1.35,"No cloud SDK in Spark core","Catalyst sees only a row-to-future interface. Provider artifacts remain replaceable.",BLUE)
card(s,6.92,4.45,5.70,1.35,"One operator for every provider","Planning, bounds, order, spill, and cancellation are implemented once.",GREEN)

# 5 bounds
s=base("Calls finish out of order; rows do not","EXECUTION")
card(s,.65,1.42,3.70,1.82,"Active-call limit","Bounds requests currently running and cost exposure per task.",BLUE,"C")
card(s,4.82,1.42,3.70,1.82,"Reorder window","Bounds submitted rows waiting behind the next row to emit.",PURPLE,"W")
card(s,8.98,1.42,3.70,1.82,"Spillable input","Uses Spark's HybridRowQueue instead of keeping every row on heap.",GREEN,"DISK")
txt(s,"Completion",.72,4.14,1.35,.30,13,NAVY,True)
for i,n in enumerate([3,1,5,2,4]): rect(s,2.2+i*1.25,4.02,.78,.52,[ORANGE,CYAN,GREEN,PURPLE,BLUE][i],None,True); txt(s,str(n),2.2+i*1.25,4.14,.78,.20,12,WHITE,True,PP_ALIGN.CENTER)
txt(s,"Output",.72,5.28,1.35,.30,13,NAVY,True)
for i,n in enumerate([1,2,3,4,5]): rect(s,2.2+i*1.25,5.16,.78,.52,[CYAN,PURPLE,ORANGE,BLUE,GREEN][i],None,True); txt(s,str(n),2.2+i*1.25,5.28,.78,.20,12,WHITE,True,PP_ALIGN.CENTER)
txt(s,"Order is a default, not a performance sacrifice.",8.83,4.58,3.35,.72,18,NAVY,True,PP_ALIGN.CENTER)

# 6 providers
s=base("Switch the provider; keep the query","PORTABILITY")
card(s,.62,1.45,3.78,4.55,"Amazon Bedrock","AWS SDK v2 Converse. Default credential chain on executors. Model and region allowlists.",GREEN,"BEDROCK")
card(s,4.78,1.45,3.78,4.55,"OpenAI-compatible","JDK HTTP + Jackson. Bearer token from executor environment. Endpoint and model allowlists.",PURPLE,"OPENAI API")
card(s,8.94,1.45,3.78,4.55,"Future provider","Implement AsyncScalarFunction, own the request/response model, pass the same conformance contract.",BLUE,"PLUG IN")
txt(s,"SQL remains:  SELECT ai.generate(prompt)",.70,6.30,11.9,.38,17,NAVY,True,PP_ALIGN.CENTER,font="Aptos Mono")

# 7 provider configuration
s=base("Configure the catalog before Spark starts","HOW TO RUN")
pill(s,"spark-defaults.conf",.65,1.23,2.20,BLUE); pill(s,"spark-sql --conf",3.08,1.23,2.05,PURPLE); pill(s,"SparkSession.builder",5.36,1.23,2.35,GREEN)
rect(s,.55,1.92,6.00,4.55,PALE,GREEN,True); txt(s,"Bedrock",.82,2.14,5.35,.34,18,GREEN,True)
bedrock="""spark.sql.catalog.ai=
  org.apache.spark.sql.ai.bedrock.BedrockFunctionCatalog
spark.sql.catalog.ai.region=us-east-1
spark.sql.catalog.ai.modelId=amazon.nova-micro-v1:0
spark.ai.bedrock.allowedModels=amazon.nova-micro-v1:0
spark.ai.bedrock.allowedRegions=us-east-1
spark.sql.asyncFunction.maxInFlightCeiling=16"""
txt(s,bedrock,.80,2.61,5.45,2.65,10.2,DARK,font="Aptos Mono")
txt(s,"Credential: AWS default chain on each executor. Do not put keys in SQL or Spark config.",.80,5.55,5.48,.58,11,NAVY,True)
rect(s,6.78,1.92,6.00,4.55,PALE,PURPLE,True); txt(s,"OpenAI-compatible",7.05,2.14,5.35,.34,18,PURPLE,True)
openai="""spark.sql.catalog.ai=
  org.apache.spark.sql.ai.openai.OpenAIFunctionCatalog
spark.sql.catalog.ai.baseUrl=https://approved.example/v1
spark.sql.catalog.ai.chatModel=approved-model
spark.ai.openai.allowedBaseUrls=https://approved.example/v1
spark.ai.openai.allowedModels=approved-model
spark.ai.openai.credentialEnv=AI_PROVIDER_TOKEN"""
txt(s,openai,7.03,2.61,5.47,2.65,9.8,DARK,font="Aptos Mono")
txt(s,"Credential: AI_PROVIDER_TOKEN in each executor environment. The variable name is cluster-only.",7.03,5.55,5.48,.58,11,NAVY,True)
txt(s,"Then run the same SQL:  SELECT ai.generate(prompt)",.70,6.66,11.9,.28,13,BLUE,True,PP_ALIGN.CENTER,font="Aptos Mono")

# 8 governance
s=base("The query author cannot widen cost or destination controls","GOVERNANCE")
card(s,.70,1.40,5.75,4.75,"Administrator sets at launch","- Endpoint/model/region allowlists\n- Credential source\n- Global in-flight ceiling\n- Response size, attempts, elapsed time\n- Rate ceiling and breaker policy",RED,"CLUSTER")
card(s,6.90,1.40,5.75,4.75,"Session may only tighten","- Pick an allowlisted target\n- Lower concurrency\n- Lower token/response cap\n- Shorten timeout/deadline\n- Slow its own rate\n\nCannot name an executor secret.",BLUE,"SESSION")
txt(s,"Credentials are resolved on the executor and never placed in SQL, plans, errors, or the package.",.72,6.45,11.9,.35,15,GREEN,True,PP_ALIGN.CENTER)

# 8 errors
s=base("Failure behavior is explicit in the function name","ERRORS")
card(s,.65,1.50,3.80,3.55,"ai.generate","A terminal row error fails the task. Use when every result is required.",RED,"STRICT")
card(s,4.77,1.50,3.80,3.55,"ai.try_generate","A documented row-level error returns NULL. Fatal configuration/auth errors still fail.",BLUE,"TOLERANT")
card(s,8.89,1.50,3.80,3.55,"ai.generate_status","Returns result plus status, safe code/message, attempts where known, and request ID.",GREEN,"OBSERVABLE")
txt(s,"No exactly-once promise",.70,5.55,3.2,.34,17,NAVY,True)
txt(s,"A Spark retry, speculative attempt, executor loss, or repeated action may repeat paid calls. The feature states this rather than hiding it.",3.72,5.43,8.75,.82,14,DARK)

# 9 proof
s=base("The complete path ran against real services","REAL PROOF")
card(s,.62,1.42,5.85,3.95,"Bedrock provider","SparkSession -> BedrockFunctionCatalog -> AsyncEval -> AWS SDK Converse -> Nova Micro. Returned A, B, NULL and a service request ID.",GREEN,"PASS")
card(s,6.87,1.42,5.85,3.95,"OpenAI-compatible provider","SparkSession -> OpenAIFunctionCatalog -> AsyncEval -> Bedrock's OpenAI-compatible API -> Qwen3. Returned A, B, NULL and a service request ID.",PURPLE,"PASS")
txt(s,"25 / 25 one-process checks",.70,5.70,4.1,.42,20,NAVY,True)
txt(s,"Both catalogs loaded only through configuration. No test catalog. No test function.",4.70,5.65,7.75,.55,15,DARK,True)

# 10 real value
s=base("Real review enrichment, with billing avoided before the call","REAL OUTPUT")
rows=[("Battery lasted all day and setup was simple.","Long battery, easy setup."),("Clear instructions and fast performance.","Fast, clear guidance."),("The screen is bright but the speaker is weak.","Loud screen, quiet speaker."),("Works well with my existing accessories.","Compatible with my current gear.")]
for i,(a,b) in enumerate(rows):
    y=1.30+i*1.20; txt(s,a,.72,y,5.05,.62,14,DARK); arrow(s,5.85,y+.30,6.65,y+.30,GREEN); txt(s,b,6.85,y,5.70,.62,15,GREEN,True)
card(s,.72,5.90,5.40,.92,"NULL rows","2 NULL reviews -> NULL, no request",BLUE)
card(s,6.75,5.90,5.40,.92,"Filtered rows","2 low-star reviews removed below AsyncEval, never billed",ORANGE)

# 11 performance
s=base("Real Bedrock benchmark: 5.79x faster within one task","MEASURED")
txt(s,"Same SQL, same 24 prompts, same model, same partition. Only in-flight limit changed.",.65,1.24,12.0,.35,14,NAVY,True,PP_ALIGN.CENTER)
txt(s,"Serial (1)",.70,2.20,1.65,.35,16,DARK,True); rect(s,2.35,2.12,8.15,.72,RED,None,True); txt(s,"8,200 ms | 2.93 rows/s",2.70,2.33,6.6,.23,15,WHITE,True)
txt(s,"Async (8)",.70,3.56,1.65,.35,16,DARK,True); rect(s,2.35,3.48,1.41,.72,GREEN,None,True); txt(s,"1,416 ms | 16.95 rows/s",4.05,3.67,3.2,.28,15,GREEN,True)
card(s,7.72,4.40,4.25,1.65,"Correctness","24 / 24 correct in both runs. Input order preserved.",BLUE,"5.79x")
txt(s,"One developer-machine sample; absolute time varies with network and service load.",.72,6.40,11.9,.35,12,MID,False,PP_ALIGN.CENTER)

# 12 evidence
s=base("Evidence goes beyond a happy path","VALIDATION")
metrics=[("25 / 25","engine cases","planning, order, bounds, pushdown, failures, cancellation",CYAN),("314 / 314","Catalyst regressions","31 existing optimizer/tree suites, 1 ignored",BLUE),("71 / 71","provider conformance","7 live service cases",PURPLE),("2 / 2","Maven provider builds","both standalone poms",GREEN),("25 / 25","live SQL proof","both providers, one JVM",ORANGE),("5.79x","real throughput","24 / 24 correct and ordered",RED)]
for i,(big,head,body,c) in enumerate(metrics): card(s,.55+(i%3)*4.18,1.35+(i//3)*2.35,3.85,1.92,head,body,c,big)

# 13 related
s=base("SPARK-61454 removes a different wait","RELATED SPIP")
card(s,.62,1.42,5.85,3.75,"SPARK-61672","Overlap remote calls inside one task. No scheduler or shuffle change.",BLUE,"IN TASK")
card(s,6.88,1.42,5.85,3.75,"SPARK-61454: Break the Stage Barrier","Stream intermediate records across eligible shuffle stage boundaries.",PURPLE,"BETWEEN STAGES")
arrow(s,5.95,3.28,6.82,3.28,ORANGE)
txt(s,"Independent and complementary",4.52,5.52,4.35,.40,19,NAVY,True,PP_ALIGN.CENTER)
txt(s,"A future composition needs joint backpressure, resource, cancellation, retry, generation-fencing, and cleanup qualification. It is not claimed here.",.85,6.05,11.65,.64,13,DARK,False,PP_ALIGN.CENTER)

# 14 scope/ask
s=base("What we are asking the community to agree on","DECISION",True)
card(s,.65,1.42,3.80,3.90,"The contract","Provider-neutral AsyncScalarFunction in Catalyst: row in, future out, explicit determinism.",CYAN,"API",True)
card(s,4.78,1.42,3.80,3.90,"The execution","Dedicated AsyncEvalExec: bounded, ordered, spillable, cancellation-aware.",GREEN,"ENGINE",True)
card(s,8.90,1.42,3.80,3.90,"The boundary","Providers own cloud code. Administrators own destinations, credentials, and ceilings.",ORANGE,"CONTROL",True)
txt(s,"Not in v1: exactly-once external calls, streaming, multi-node qualification, or the full Pandas/Arrow comparison.",.78,5.82,11.80,.48,14,RGBColor(199,215,231),True,PP_ALIGN.CENTER)
txt(s,"If the mechanism is right, split it into reviewable implementation changes.",.78,6.48,11.80,.35,16,CYAN,True,PP_ALIGN.CENTER)

prs.core_properties.title="SPARK-61672 Feature Overview"
prs.core_properties.subject="Provider-neutral asynchronous functions for Spark SQL"
prs.core_properties.author="Holden Karau; Prashant Singh; Vaquar Khan"
prs.core_properties.comments="Feature-focused deck built from verified local evidence. No credentials included."
import os
max_slides=int(os.environ.get("SPARK_61672_FEATURE_MAX_SLIDES","0"))
if max_slides>0:
    while len(prs.slides)>max_slides:
        sid=prs.slides._sldIdLst[-1]; prs.part.drop_rel(sid.rId); del prs.slides._sldIdLst[-1]
OUT.parent.mkdir(parents=True,exist_ok=True); prs.save(OUT)
print(f"wrote {OUT}"); print(f"slides={len(prs.slides)} bytes={OUT.stat().st_size}")
