# Convert the feature-focused SPARK-61672 deck to PDF using installed Microsoft PowerPoint.
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$pptx = Join-Path $here 'SPARK-61672-Feature-Overview.pptx'
$pdf = Join-Path $here 'SPARK-61672-Feature-Overview.pdf'
$tempDir = Join-Path $env:TEMP 'spark61672-feature-ppt'
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null
$tempPptx = Join-Path $tempDir 'feature.pptx'
$tempPdf = Join-Path $tempDir 'feature.pdf'
Copy-Item $pptx $tempPptx -Force
Remove-Item $tempPdf -ErrorAction SilentlyContinue
$app = $null
$deck = $null
try {
  $app = New-Object -ComObject PowerPoint.Application
  $app.Visible = -1
  $deck = $app.Presentations.Open($tempPptx, $true, $false, $true)
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
