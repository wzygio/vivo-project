$ErrorActionPreference = 'Stop'

$ProjectDir = 'D:\wzy\Python\vivo-project\projects\customer_midyear_summary_template'
$SourcePptx = Join-Path $ProjectDir 'sources\template-PPT.pptx'
$ExportDir = Join-Path $ProjectDir 'exports'
New-Item -ItemType Directory -Force -Path $ExportDir | Out-Null

$Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$OutputPptx = Join-Path $ExportDir "customer_midyear_summary_template_$Timestamp.pptx"

function ColorRgb($hex) {
    $h = $hex.TrimStart('#')
    $r = [Convert]::ToInt32($h.Substring(0, 2), 16)
    $g = [Convert]::ToInt32($h.Substring(2, 2), 16)
    $b = [Convert]::ToInt32($h.Substring(4, 2), 16)
    return ($r -bor ($g -shl 8) -bor ($b -shl 16))
}

$Colors = @{
    Blue = ColorRgb '#5B9BD5'
    DeepBlue = ColorRgb '#4472C4'
    Navy = ColorRgb '#1F4E79'
    Orange = ColorRgb '#ED7D31'
    Green = ColorRgb '#70AD47'
    Yellow = ColorRgb '#FFC000'
    Grey = ColorRgb '#A5A5A5'
    LightGrey = ColorRgb '#F3F6FA'
    MidGrey = ColorRgb '#D9E2EF'
    Text = ColorRgb '#44546A'
    White = ColorRgb '#FFFFFF'
    Red = ColorRgb '#C00000'
}

function Set-TextStyle($shape, $size, $color, $bold = $false, $align = 1) {
    $tf = $shape.TextFrame
    $tf.MarginLeft = 6
    $tf.MarginRight = 6
    $tf.MarginTop = 3
    $tf.MarginBottom = 3
    $tr = $tf.TextRange
    $tr.Font.Name = 'Microsoft YaHei'
    $tr.Font.NameFarEast = '微软雅黑'
    $tr.Font.Size = [single]$size
    $tr.Font.Color.RGB = $color
    $tr.Font.Bold = $(if ($bold) { -1 } else { 0 })
    $tr.ParagraphFormat.Alignment = $align
}

function Add-Text($slide, $text, $x, $y, $w, $h, $size, $color, $bold = $false, $align = 1) {
    $shape = $slide.Shapes.AddTextbox(1, $x, $y, $w, $h)
    $shape.TextFrame.TextRange.Text = $text
    Set-TextStyle $shape $size $color $bold $align
    return $shape
}

function Add-Rect($slide, $x, $y, $w, $h, $fill, $line = $null, $radius = $false) {
    $type = $(if ($radius) { 5 } else { 1 })
    $shape = $slide.Shapes.AddShape($type, $x, $y, $w, $h)
    $shape.Fill.ForeColor.RGB = $fill
    $shape.Line.ForeColor.RGB = $(if ($null -eq $line) { $fill } else { $line })
    $shape.Line.Weight = 0.75
    return $shape
}

function Add-Line($slide, $x1, $y1, $x2, $y2, $color, $weight = 1.5) {
    $shape = $slide.Shapes.AddLine($x1, $y1, $x2, $y2)
    $shape.Line.ForeColor.RGB = $color
    $shape.Line.Weight = [single]$weight
    return $shape
}

function Add-Title($slide, $section, $title, $subtitle, $pageNo) {
    Add-Text $slide $section 42 7 120 20 9 $Colors.Blue $true 1 | Out-Null
    Add-Text $slide $title 42 27 610 31 21 $Colors.Text $true 1 | Out-Null
    Add-Text $slide $subtitle 42 57 620 17 8.5 $Colors.Grey $false 1 | Out-Null
    Add-Text $slide $pageNo 880 500 46 18 8 $Colors.Grey $false 3 | Out-Null
}

function Add-KpiCard($slide, $x, $y, $w, $h, $label, $value, $accent) {
    Add-Rect $slide $x $y $w $h $Colors.White $Colors.MidGrey $true | Out-Null
    Add-Rect $slide $x $y 4 $h $accent $accent $false | Out-Null
    Add-Text $slide $label ($x + 13) ($y + 9) ($w - 22) 16 8.5 $Colors.Grey $false 1 | Out-Null
    Add-Text $slide $value ($x + 13) ($y + 29) ($w - 22) 26 18 $Colors.Text $true 1 | Out-Null
    Add-Text $slide '待填充数据' ($x + 13) ($y + 58) ($w - 22) 14 7.5 $accent $false 1 | Out-Null
}

function Add-NoteBox($slide, $text) {
    Add-Rect $slide 42 468 875 26 $Colors.LightGrey $Colors.MidGrey $true | Out-Null
    Add-Text $slide $text 52 473 855 15 8 $Colors.Grey $false 1 | Out-Null
}

function Build-SystemSlide($slide) {
    Add-Title $slide '01 系统搭建' '系统搭建进展总览' '填充建设里程碑、模块状态、联调/试运行进展与下一步计划' '01 / 03'

    Add-Text $slide '建设里程碑时间轴' 42 96 250 24 13 $Colors.Text $true 1 | Out-Null
    Add-Line $slide 78 202 700 202 $Colors.MidGrey 2 | Out-Null

    $steps = @(
        @('需求梳理', 'YYYY-MM', '完成/进行中', $Colors.Blue),
        @('规则配置', 'YYYY-MM', '完成/进行中', $Colors.Green),
        @('数据接入', 'YYYY-MM', '进行中', $Colors.Orange),
        @('看板联调', 'YYYY-MM', '计划中', $Colors.Yellow),
        @('试运行迭代', 'YYYY-MM', '计划中', $Colors.DeepBlue)
    )
    $xs = @(78, 233, 388, 543, 698)
    for ($i = 0; $i -lt $steps.Count; $i++) {
        $x = $xs[$i]
        $step = $steps[$i]
        Add-Rect $slide ($x - 12) 190 24 24 $step[3] $Colors.White $true | Out-Null
        Add-Text $slide ($i + 1).ToString('00') ($x - 15) 195 30 11 7.5 $Colors.White $true 2 | Out-Null
        Add-Text $slide $step[0] ($x - 48) 224 96 18 9.5 $Colors.Text $true 2 | Out-Null
        Add-Text $slide $step[1] ($x - 45) 246 90 14 8 $Colors.Grey $false 2 | Out-Null
        Add-Text $slide $step[2] ($x - 45) 264 90 14 8 $step[3] $false 2 | Out-Null
    }

    Add-Text $slide '关键模块状态' 42 318 180 20 12 $Colors.Text $true 1 | Out-Null
    $modules = @('数据源接入', '预警规则配置', '处置流转闭环', '报表看板发布')
    for ($i = 0; $i -lt $modules.Count; $i++) {
        $x = 42 + ($i * 170)
        Add-Rect $slide $x 348 148 64 $Colors.White $Colors.MidGrey $true | Out-Null
        Add-Text $slide $modules[$i] ($x + 11) 359 126 16 9 $Colors.Text $true 1 | Out-Null
        Add-Text $slide '状态：待填充' ($x + 11) 382 126 14 8 $Colors.Grey $false 1 | Out-Null
    }

    Add-KpiCard $slide 742 110 174 72 '模块完成率' 'XX%' $Colors.Blue
    Add-KpiCard $slide 742 200 174 72 '接口联通数' 'XX' $Colors.Green
    Add-KpiCard $slide 742 290 174 72 '待闭环事项' 'XX' $Colors.Orange
    Add-NoteBox $slide '填充提示：将时间轴节点替换为实际里程碑；右侧 KPI 用于放系统搭建阶段的核心进展数据。'
}

function Build-RiskSlide($slide) {
    Add-Title $slide '02 风险品处置' '风险品处置闭环概览' '填充触发预警数量、风险产品数、产品处置数及处置转化关系' '02 / 03'

    Add-KpiCard $slide 42 96 260 74 '触发预警数量' 'XX' $Colors.Blue
    Add-KpiCard $slide 350 96 260 74 '风险产品数' 'XX' $Colors.Orange
    Add-KpiCard $slide 658 96 260 74 '产品处置数' 'XX' $Colors.Green

    Add-Text $slide '处置漏斗样式' 42 207 180 22 13 $Colors.Text $true 1 | Out-Null
    $funnel = @(
        @('预警触发', 'XX 条', 70, 250, 760, 48, $Colors.Blue),
        @('风险识别', 'XX 个产品', 120, 316, 660, 48, $Colors.Orange),
        @('处置闭环', 'XX 个产品', 170, 382, 560, 48, $Colors.Green)
    )
    foreach ($row in $funnel) {
        Add-Rect $slide $row[2] $row[3] $row[4] $row[5] $row[6] $row[6] $true | Out-Null
        Add-Text $slide $row[0] ($row[2] + 24) ($row[3] + 11) 160 20 12 $Colors.White $true 1 | Out-Null
        Add-Text $slide $row[1] ($row[2] + $row[4] - 150) ($row[3] + 10) 120 22 15 $Colors.White $true 3 | Out-Null
    }

    Add-Text $slide '转化率 XX%' 790 303 110 18 9 $Colors.Orange $true 2 | Out-Null
    Add-Line $slide 790 282 790 304 $Colors.Orange 1.2 | Out-Null
    Add-Line $slide 790 348 790 370 $Colors.Green 1.2 | Out-Null
    Add-Text $slide '闭环率 XX%' 790 369 110 18 9 $Colors.Green $true 2 | Out-Null

    Add-Rect $slide 42 440 876 28 $Colors.LightGrey $Colors.MidGrey $true | Out-Null
    Add-Text $slide '可补充处置分类：已关闭 XX | 持续跟进 XX | 升级专项 XX | 责任部门 / 截止日期 / 闭环说明' 54 446 850 14 8.2 $Colors.Grey $false 1 | Out-Null
    Add-NoteBox $slide '填充提示：漏斗三段对应预警、识别、处置；右侧百分比用于展示从预警到处置的转化与闭环效率。'
}

function Build-ProcessSlide($slide) {
    Add-Title $slide '03 制程提升' '制程提升与专项进展' '填充 CPK 优化数、流程优化项、专项改善进展及优化前后对比' '03 / 03'

    Add-Text $slide 'CPK 优化前后对比' 42 96 220 22 13 $Colors.Text $true 1 | Out-Null
    Add-Rect $slide 42 130 500 270 $Colors.White $Colors.MidGrey $true | Out-Null
    Add-Line $slide 92 342 500 342 $Colors.MidGrey 1 | Out-Null
    Add-Line $slide 92 174 92 342 $Colors.MidGrey 1 | Out-Null
    $bars = @(
        @('优化前', 150, 255, 74, 87, $Colors.Grey, 'XX'),
        @('优化后', 275, 207, 74, 135, $Colors.Green, 'XX'),
        @('目标值', 400, 184, 74, 158, $Colors.Blue, 'XX')
    )
    foreach ($bar in $bars) {
        Add-Rect $slide $bar[1] $bar[2] $bar[3] $bar[4] $bar[5] $bar[5] $false | Out-Null
        Add-Text $slide $bar[6] ($bar[1] - 3) ($bar[2] - 24) 80 16 9 $Colors.Text $true 2 | Out-Null
        Add-Text $slide $bar[0] ($bar[1] - 3) 350 80 16 8.5 $Colors.Grey $false 2 | Out-Null
    }
    Add-Text $slide 'Y 轴：CPK / 良率 / 缺陷率等，可按实际口径替换' 70 377 440 13 7.5 $Colors.Grey $false 1 | Out-Null

    Add-KpiCard $slide 580 126 154 70 'CPK 优化数' 'XX' $Colors.Green
    Add-KpiCard $slide 760 126 154 70 '流程优化项' 'XX' $Colors.Blue
    Add-KpiCard $slide 580 214 154 70 '专项推进中' 'XX' $Colors.Orange
    Add-KpiCard $slide 760 214 154 70 '已固化 SOP' 'XX' $Colors.DeepBlue

    Add-Text $slide '专项进展矩阵' 580 317 180 18 12 $Colors.Text $true 1 | Out-Null
    $items = @(
        @('专项 A', '进度 XX%', $Colors.Green),
        @('专项 B', '进度 XX%', $Colors.Blue),
        @('专项 C', '进度 XX%', $Colors.Orange)
    )
    for ($i = 0; $i -lt $items.Count; $i++) {
        $y = 346 + ($i * 34)
        Add-Text $slide $items[$i][0] 586 $y 76 15 8.5 $Colors.Text $true 1 | Out-Null
        Add-Rect $slide 660 ($y + 3) 210 8 $Colors.MidGrey $Colors.MidGrey $true | Out-Null
        Add-Rect $slide 660 ($y + 3) (80 + $i * 35) 8 $items[$i][2] $items[$i][2] $true | Out-Null
        Add-Text $slide $items[$i][1] 878 ($y - 1) 42 13 7.2 $Colors.Grey $false 3 | Out-Null
    }

    Add-NoteBox $slide '填充提示：左侧柱状图展示优化前后或目标达成；右侧矩阵用于放流程优化、专项推进和固化成果。'
}

$ppt = $null
$pres = $null
try {
    $ppt = New-Object -ComObject PowerPoint.Application
    $ppt.DisplayAlerts = 1
    $pres = $ppt.Presentations.Open($SourcePptx, $false, $false, $false)

    while ($pres.Slides.Count -lt 3) {
        $pres.Slides.Item(1).Duplicate() | Out-Null
    }
    while ($pres.Slides.Count -gt 3) {
        $pres.Slides.Item($pres.Slides.Count).Delete()
    }

    Build-SystemSlide $pres.Slides.Item(1)
    Build-RiskSlide $pres.Slides.Item(2)
    Build-ProcessSlide $pres.Slides.Item(3)

    $pres.SaveAs($OutputPptx, 24)
    $pres.Close()
    $pres = $null
    Write-Output $OutputPptx
}
finally {
    if ($pres -ne $null) {
        try { $pres.Close() } catch {}
    }
    if ($ppt -ne $null) {
        try { $ppt.Quit() } catch {}
    }
    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()
}
