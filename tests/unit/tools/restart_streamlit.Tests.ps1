. (Join-Path $PSScriptRoot '..\..\..\tools\restart_streamlit.ps1')

Describe 'Streamlit process ownership' {
    $root = 'D:\reports with spaces\vivo-project'
    function New-Record([string]$Command, [string]$Executable = 'C:\Python\python.exe') {
        [pscustomobject]@{ Name = 'python.exe'; CommandLine = $Command; ExecutablePath = $Executable }
    }

    It 'recognizes the legacy uv-launched base interpreter' {
        $record = New-Record 'python "D:\reports with spaces\vivo-project\.venv\Scripts\streamlit.exe" run app/Home.py --server.port 8503'
        (Test-ProjectStreamlit $record $root 8503) | Should Be $true
    }
    It 'recognizes the new absolute module entrypoint' {
        $record = New-Record 'python -m streamlit run "D:\reports with spaces\vivo-project\app\Home.py" --server.port=8503'
        (Test-ProjectStreamlit $record $root 8503) | Should Be $true
    }
    It 'rejects a neighboring checkout with the same app name and port' {
        $record = New-Record 'python "D:\reports with spaces\vivo-project-other\.venv\Scripts\streamlit.exe" run app/Home.py --server.port 8503'
        (Test-ProjectStreamlit $record $root 8503) | Should Be $false
    }
    It 'rejects relative entrypoints with no verified project path' {
        $record = New-Record 'python -m streamlit run app/Home.py --server.port 8503'
        (Test-ProjectStreamlit $record $root 8503) | Should Be $false
    }
    It 'rejects another port even when its prefix matches' {
        $record = New-Record 'python -m streamlit run "D:\reports with spaces\vivo-project\app\Home.py" --server.port=85030'
        (Test-ProjectStreamlit $record $root 8503) | Should Be $false
    }
    It 'does not interpret application arguments as server options' {
        $record = New-Record 'python -m streamlit run "D:\reports with spaces\vivo-project\app\Home.py" -- --server.port=8503'
        (Test-ProjectStreamlit $record $root 8503) | Should Be $false
    }
    It 'rejects a different module' {
        $record = New-Record 'python -m worker run "D:\reports with spaces\vivo-project\app\Home.py" --server.port=8503'
        (Test-ProjectStreamlit $record $root 8503) | Should Be $false
    }
    It 'refuses an unrelated port owner' {
        { Assert-StreamlitPortOwner @([pscustomobject]@{ OwningProcess = 456 }) @([pscustomobject]@{ ProcessId = 123 }) } | Should Throw
    }
    It 'accepts both IPv4 and IPv6 listeners belonging to the verified process' {
        { Assert-StreamlitPortOwner @([pscustomobject]@{ OwningProcess = 123 }, [pscustomobject]@{ OwningProcess = 123 }) @([pscustomobject]@{ ProcessId = 123 }) } | Should Not Throw
    }
}

Describe 'Startup health must belong to the new process' {
    Mock Start-Sleep {}
    Mock Get-ProjectStreamlit { @([pscustomobject]@{ ProcessId = 200; ParentProcessId = 100 }) }
    Mock Get-StreamlitListeners { @([pscustomobject]@{ OwningProcess = 200 }) }
    Mock Test-StreamlitHealth { $true }

    It 'accepts a healthy child of the new launcher' {
        $started = [pscustomobject]@{ Id = 100; HasExited = $false }
        @(Wait-StreamlitReady $started 'D:\project' 8503 1)[0] | Should Be 200
    }
    It 'fails if the new launcher exited' {
        $started = [pscustomobject]@{ Id = 100; HasExited = $true }
        { Wait-StreamlitReady $started 'D:\project' 8503 1 } | Should Throw
    }
    It 'rejects a healthy server outside the new process family' {
        $started = [pscustomobject]@{ Id = 999; HasExited = $false }
        { Wait-StreamlitReady $started 'D:\project' 8503 1 } | Should Throw
    }
    It 'fails on a health timeout' {
        Mock Test-StreamlitHealth { $false }
        $started = [pscustomobject]@{ Id = 100; HasExited = $false }
        { Wait-StreamlitReady $started 'D:\project' 8503 1 } | Should Throw
    }
}

Describe 'Shutdown identity validation' {
    It 'refuses a reused PID before invoking Stop-Process' {
        Mock Get-CimInstance { [pscustomobject]@{ ProcessId = 123; CreationDate = [datetime]'2026-09-22' } }
        Mock Stop-Process { throw 'Must not run' }
        $old = [pscustomobject]@{ ProcessId = 123; CreationDate = [datetime]'2026-09-21' }
        { Stop-VerifiedStreamlit @($old) 'D:\project' 8503 1 } | Should Throw
        Assert-MockCalled Stop-Process -Times 0 -Exactly -Scope It
    }
}

Describe 'Restart transaction and failure reporting' {
    BeforeEach {
        $script:restartRoot = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory -Path $script:restartRoot | Out-Null
        Mock Assert-StreamlitRuntime {}
        Mock Get-ProjectStreamlit { @() }
        Mock Get-StreamlitListeners { @() }
        Mock Stop-VerifiedStreamlit {}
        Mock Wait-StreamlitReady { @(200) }
        Mock Start-Process {
            $process = [pscustomobject]@{ Id = 100; HasExited = $false }
            $process | Add-Member ScriptMethod Dispose {}
            return $process
        }
    }
    It 'returns success only after health verification and records the new PID' {
        (Invoke-StreamlitRestart $script:restartRoot 8503 1 1) | Should Be 0
        Assert-MockCalled Wait-StreamlitReady -Times 1 -Exactly -Scope It
        $log = Get-Content (Get-ChildItem $script:restartRoot -Recurse -Filter '*.json').FullName -Raw | ConvertFrom-Json
        $log.status | Should Be 'success'
        $log.launched_pid | Should Be 100
        $log.listener_pids[0] | Should Be 200
    }
    It 'keeps the current service when runtime preflight fails' {
        Mock Assert-StreamlitRuntime { throw 'Missing runtime' }
        (Invoke-StreamlitRestart $script:restartRoot 8503 1 1) | Should Be 1
        Assert-MockCalled Stop-VerifiedStreamlit -Times 0 -Exactly -Scope It
        Assert-MockCalled Start-Process -Times 0 -Exactly -Scope It
    }
    It 'does not stop anything when another application owns the port' {
        Mock Get-StreamlitListeners { @([pscustomobject]@{ OwningProcess = 456 }) }
        (Invoke-StreamlitRestart $script:restartRoot 8503 1 1) | Should Be 1
        Assert-MockCalled Stop-VerifiedStreamlit -Times 0 -Exactly -Scope It
        Assert-MockCalled Start-Process -Times 0 -Exactly -Scope It
    }
    It 'does not launch after a shutdown failure and records the failure' {
        Mock Stop-VerifiedStreamlit { throw 'Permission denied' }
        (Invoke-StreamlitRestart $script:restartRoot 8503 1 1) | Should Be 1
        Assert-MockCalled Start-Process -Times 0 -Exactly -Scope It
        $log = Get-Content (Get-ChildItem $script:restartRoot -Recurse -Filter '*.json').FullName -Raw | ConvertFrom-Json
        $log.status | Should Be 'failed'
        $log.error | Should Be 'Permission denied'
    }
    It 'cleans up a failed startup and returns failure' {
        Mock Wait-StreamlitReady { throw 'Health timeout' }
        (Invoke-StreamlitRestart $script:restartRoot 8503 1 1) | Should Be 1
        Assert-MockCalled Stop-VerifiedStreamlit -Times 2 -Exactly -Scope It
    }
    It 'refuses a concurrent restart before touching processes' {
        $logs = Join-Path $script:restartRoot 'output\logs\streamlit-startup'
        New-Item -ItemType Directory -Path $logs -Force | Out-Null
        $held = [System.IO.File]::Open((Join-Path $logs 'restart-8503.lock'), 'OpenOrCreate', 'ReadWrite', 'None')
        try {
            (Invoke-StreamlitRestart $script:restartRoot 8503 1 1) | Should Be 1
            Assert-MockCalled Stop-VerifiedStreamlit -Times 0 -Exactly -Scope It
            Assert-MockCalled Start-Process -Times 0 -Exactly -Scope It
        }
        finally { $held.Dispose() }
    }
}
