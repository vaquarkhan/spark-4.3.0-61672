# Convert the SPARK-61672 deck to PDF with installed Microsoft PowerPoint.
# The exported PDF uses PowerPoint's native layout engine.
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$pptx = Join-Path $here 'SPARK-61672-POC-Review.pptx'
$pdf = Join-Path $here 'SPARK-61672-POC-Review.pdf'
# PowerPoint COM can reject deep Windows paths. Convert from a short local temp path, then copy
# the PDF back beside the deck.
$tempDir = Join-Path $env:TEMP 'spark61672-ppt'
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null
$tempPptx = Join-Path $tempDir 'review.pptx'
$tempPdf = Join-Path $tempDir 'review.pdf'
Copy-Item $pptx $tempPptx -Force
Remove-Item $tempPdf -ErrorAction SilentlyContinue
$app = $null
$deck = $null
try {
  $app = New-Object -ComObject PowerPoint.Application
  $app.Visible = -1
  $deck = $app.Presentations.Open($tempPptx, $true, $false, $true)
  # 32 = ppSaveAsPDF
  $deck.SaveAs($tempPdf, 32)
  Copy-Item $tempPdf $pdf -Force
  Write-Output "wrote $pdf"
} finally {
  if ($deck -ne $null) { $deck.Close() }
  if ($app -ne $null) { $app.Quit() }
  if ($deck -ne $null) { [Runtime.InteropServices.Marshal]::ReleaseComObject($deck) | Out-Null }
  if ($app -ne $null) { [Runtime.InteropServices.Marshal]::ReleaseComObject($app) | Out-Null }
  [GC]::Collect(); [GC]::WaitForPendingFinalizers()
}
