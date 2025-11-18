<#
Manage-GraphReplayConfig.ps1 - Configuration Management for Graph Replay
Creates, updates, and tests configuration files for the Graph Email Replay script
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet("Create", "Update", "Test", "List", "Show", "Encrypt")]
    [string]$Action,
    
    [string]$ConfigPath,
    [string]$ConfigName,
    [string]$ConfigDirectory = "C:\GraphReplay\Configs"
)

# Ensure config directory exists
if (!(Test-Path $ConfigDirectory)) {
    New-Item -ItemType Directory -Path $ConfigDirectory -Force | Out-Null
}

function New-ConfigFile {
    param([string]$Path)
    
    Write-Host "Creating new configuration file..." -ForegroundColor Cyan
    Write-Host "Please provide the following information:" -ForegroundColor Yellow
    
    # We'll keep it as a PSCustomObject for simplicity
    $config = [PSCustomObject]@{}
    
    # Required fields
    $tenantId = Read-Host "Tenant ID (required)"
    $clientId = Read-Host "Client ID (required)"
    
    if (-not $tenantId -or -not $clientId) {
        Write-Error "Tenant ID and Client ID are required."
        return
    }
    
    $config | Add-Member -NotePropertyName TenantId -NotePropertyValue $tenantId
    $config | Add-Member -NotePropertyName ClientId -NotePropertyValue $clientId
    
    # Handle secret securely
    $secretResponse = Read-Host "Client Secret (required)" -AsSecureString
    $plainSecret = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secretResponse)
    )
    
    # Source mailboxes
    $mailboxInput = Read-Host "Source Mailboxes (comma-separated)"
    $sourceMailboxes = $mailboxInput -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    
    if (-not $sourceMailboxes) {
        Write-Error "At least one source mailbox is required."
        return
    }
    
    $targetMailbox = Read-Host "Target Mailbox (required)"
    if (-not $targetMailbox) {
        Write-Error "Target mailbox is required."
        return
    }
    
    $config | Add-Member -NotePropertyName SourceMailboxes -NotePropertyValue $sourceMailboxes
    $config | Add-Member -NotePropertyName TargetMailbox   -NotePropertyValue $targetMailbox
    
    # Optional fields
    Write-Host "`nOptional settings (press Enter to skip):" -ForegroundColor Yellow
    
    $folder = Read-Host "Folder Name [Inbox]"
    if ($folder) {
        $config | Add-Member -NotePropertyName FolderName -NotePropertyValue $folder
    }
    
    $mode = Read-Host "Replay Mode (Transparent/Wrapper) [Transparent]"
    if ($mode) {
        $config | Add-Member -NotePropertyName ReplayMode -NotePropertyValue $mode
    }
    
    $attachOnly = Read-Host "Attachments Only (true/false) [false]"
    if ($attachOnly -eq 'true') {
        $config | Add-Member -NotePropertyName AttachmentsOnly -NotePropertyValue $true
    }
    
    $skipProcessed = Read-Host "Skip Already Processed (true/false) [false]"
    if ($skipProcessed -eq 'true') {
        $config | Add-Member -NotePropertyName SkipAlreadyProcessed -NotePropertyValue $true
    }
    
    $bcc = Read-Host "BCC Always (comma-separated)"
    if ($bcc) {
        $bccList = $bcc -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
        if ($bccList) {
            $config | Add-Member -NotePropertyName BccAlways -NotePropertyValue $bccList
        }
    }
    
    $logPath = Read-Host "Log Path"
    if ($logPath) {
        $config | Add-Member -NotePropertyName LogPath -NotePropertyValue $logPath
    }
    
    $maxMsg = Read-Host "Max Messages"
    if ($maxMsg) {
        $config | Add-Member -NotePropertyName MaxMessages -NotePropertyValue ([int]$maxMsg)
    }
    
    $batchSize = Read-Host "Batch Size [50]"
    if ($batchSize) {
        $config | Add-Member -NotePropertyName BatchSize -NotePropertyValue ([int]$batchSize)
    }
    
    $throttle = Read-Host "Throttle MS [100]"
    if ($throttle) {
        $config | Add-Member -NotePropertyName ThrottleMs -NotePropertyValue ([int]$throttle)
    }
    
    $header = Read-Host "Processed Header [X-GraphReplay-Processed]"
    if ($header) {
        $config | Add-Member -NotePropertyName ProcessedHeader -NotePropertyValue $header
    }
    
    # Add metadata
    $config | Add-Member -NotePropertyName CreatedDate -NotePropertyValue (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
    $config | Add-Member -NotePropertyName CreatedBy   -NotePropertyValue $env:USERNAME
    
    $desc = Read-Host "Description/Notes"
    if ($desc) {
        $config | Add-Member -NotePropertyName Description -NotePropertyValue $desc
    }
    
    # Encrypt the secret
    try {
        $secureString = ConvertTo-SecureString $plainSecret -AsPlainText -Force
        $encrypted    = ConvertFrom-SecureString $secureString
        $config | Add-Member -NotePropertyName ClientSecretEncrypted -NotePropertyValue $encrypted
        Write-Host "Client Secret encrypted successfully" -ForegroundColor Green
    }
    catch {
        Write-Warning "Could not encrypt Client Secret - storing in plain text"
        $config | Add-Member -NotePropertyName ClientSecret -NotePropertyValue $plainSecret
    }
    
    # Save configuration
    $config | ConvertTo-Json -Depth 10 | Set-Content -Path $Path -Encoding UTF8
    Write-Host "`nConfiguration saved to: $Path" -ForegroundColor Green
    
    # Offer to test
    $testNow = Read-Host "`nWould you like to test this configuration now? (y/n)"
    if ($testNow -eq 'y') {
        Test-ConfigFile -Path $Path
    }
}

function Update-ConfigFile {
    param([string]$Path)
    
    if (!(Test-Path $Path)) {
        Write-Error "Configuration file not found: $Path"
        return
    }
    
    $config = Get-Content $Path -Raw | ConvertFrom-Json
    
    Write-Host "Current configuration:" -ForegroundColor Cyan
    Show-ConfigFile -Path $Path
    
    Write-Host "`nEnter new values (press Enter to keep current):" -ForegroundColor Yellow
    
    $newTenant = Read-Host "Tenant ID [$($config.TenantId)]"
    if ($newTenant) { $config.TenantId = $newTenant }
    
    $newClient = Read-Host "Client ID [$($config.ClientId)]"
    if ($newClient) { $config.ClientId = $newClient }
    
    $updateSecret = Read-Host "Update Client Secret? (y/n)"
    if ($updateSecret -eq 'y') {
        $secretResponse = Read-Host "Client Secret" -AsSecureString
        $newSecret      = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secretResponse)
        )
        
        $secureString = ConvertTo-SecureString $newSecret -AsPlainText -Force
        $config.ClientSecretEncrypted = ConvertFrom-SecureString $secureString
        # Remove plain-text if present
        if ($config.PSObject.Properties['ClientSecret']) {
            $config.PSObject.Properties.Remove('ClientSecret')
        }
    }
    
    $newTarget = Read-Host "Target Mailbox [$($config.TargetMailbox)]"
    if ($newTarget) { $config.TargetMailbox = $newTarget }
    
    # Update metadata
    $config.ModifiedDate = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $config.ModifiedBy   = $env:USERNAME
    
    # Save updated configuration
    $config | ConvertTo-Json -Depth 10 | Set-Content -Path $Path -Encoding UTF8
    Write-Host "`nConfiguration updated successfully" -ForegroundColor Green
}

function Test-ConfigFile {
    param([string]$Path)
    
    if (!(Test-Path $Path)) {
        Write-Error "Configuration file not found: $Path"
        return
    }
    
    Write-Host "Testing configuration: $Path" -ForegroundColor Cyan
    
    try {
        # Load the config
        $configData = Get-Content $Path -Raw | ConvertFrom-Json
        
        # Decrypt secret if needed
        $secret = $null
        if ($configData.ClientSecretEncrypted) {
            try {
                $secureString = ConvertTo-SecureString $configData.ClientSecretEncrypted
                $secret = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
                    [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureString)
                )
            }
            catch {
                Write-Error "Could not decrypt Client Secret"
                return
            }
        }
        else {
            $secret = $configData.ClientSecret
        }
        
        if (-not $secret) {
            Write-Error "No client secret available in configuration."
            return
        }
        
        # Test authentication
        Write-Host "Testing authentication..." -ForegroundColor Yellow
        $body = @{
            client_id     = $configData.ClientId
            client_secret = $secret
            scope         = "https://graph.microsoft.com/.default"
            grant_type    = "client_credentials"
        }
        
        $tokenUrl = "https://login.microsoftonline.com/$($configData.TenantId)/oauth2/v2.0/token"
        $response = Invoke-RestMethod -Method Post -Uri $tokenUrl -ContentType "application/x-www-form-urlencoded" -Body $body
        
        if ($response.access_token) {
            Write-Host "✓ Authentication successful" -ForegroundColor Green
            
            # Test mailbox access
            Write-Host "Testing mailbox access..." -ForegroundColor Yellow
            $headers = @{
                "Authorization" = "Bearer $($response.access_token)"
            }
            
            foreach ($mailbox in $configData.SourceMailboxes) {
                try {
                    $uri = "https://graph.microsoft.com/v1.0/users/$mailbox/mailFolders/inbox/messages?`$top=1"
                    $null = Invoke-RestMethod -Uri $uri -Headers $headers
                    Write-Host "  ✓ $mailbox - Accessible" -ForegroundColor Green
                }
                catch {
                    Write-Host "  ✗ $mailbox - Not accessible: $_" -ForegroundColor Red
                }
            }
            
            # Test target mailbox
            try {
                $uri = "https://graph.microsoft.com/v1.0/users/$($configData.TargetMailbox)"
                $null = Invoke-RestMethod -Uri $uri -Headers $headers
                Write-Host "✓ Target mailbox accessible: $($configData.TargetMailbox)" -ForegroundColor Green
            }
            catch {
                Write-Host "✗ Target mailbox not accessible: $_" -ForegroundColor Red
            }
        }
    }
    catch {
        Write-Error "Configuration test failed: $_"
    }
}

function Show-ConfigFile {
    param([string]$Path)
    
    if (!(Test-Path $Path)) {
        Write-Error "Configuration file not found: $Path"
        return
    }
    
    $config = Get-Content $Path -Raw | ConvertFrom-Json
    
    Write-Host "`n=== Configuration Details ===" -ForegroundColor Cyan
    Write-Host "File: $Path" -ForegroundColor Gray
    
    $config.PSObject.Properties | ForEach-Object {
        if ($_.Name -eq 'ClientSecretEncrypted') {
            Write-Host "$($_.Name): [ENCRYPTED]" -ForegroundColor Yellow
        }
        elseif ($_.Name -eq 'ClientSecret') {
            Write-Host "$($_.Name): [HIDDEN]" -ForegroundColor Yellow
        }
        else {
            $value = if ($_.Value -is [array]) { $_.Value -join ', ' } else { $_.Value }
            Write-Host "$($_.Name): $value"
        }
    }
}

# Main execution
switch ($Action) {
    "Create" {
        if (-not $ConfigPath) {
            if (-not $ConfigName) {
                $ConfigName = Read-Host "Enter config name (e.g., Company1)"
            }
            $ConfigPath = Join-Path $ConfigDirectory "$ConfigName.json"
        }
        New-ConfigFile -Path $ConfigPath
    }
    
    "Update" {
        if (-not $ConfigPath) {
            if (-not $ConfigName) {
                $ConfigName = Read-Host "Enter config name"
            }
            $ConfigPath = Join-Path $ConfigDirectory "$ConfigName.json"
        }
        Update-ConfigFile -Path $ConfigPath
    }
    
    "Test" {
        if (-not $ConfigPath) {
            if (-not $ConfigName) {
                $ConfigName = Read-Host "Enter config name"
            }
            $ConfigPath = Join-Path $ConfigDirectory "$ConfigName.json"
        }
        Test-ConfigFile -Path $ConfigPath
    }
    
    "List" {
        Write-Host "`nAvailable configurations:" -ForegroundColor Cyan
        if (!(Test-Path $ConfigDirectory)) {
            Write-Host "No configuration directory found: $ConfigDirectory" -ForegroundColor Yellow
        }
        else {
            Get-ChildItem -Path $ConfigDirectory -Filter "*.json" | ForEach-Object {
                $config = Get-Content $_.FullName -Raw | ConvertFrom-Json
                Write-Host "`n$($_.BaseName)" -ForegroundColor Green
                Write-Host "  File: $($_.FullName)"
                Write-Host "  Tenant: $($config.TenantId)"
                Write-Host "  Target: $($config.TargetMailbox)"
                if ($config.SourceMailboxes) {
                    Write-Host "  Sources: $($config.SourceMailboxes -join ', ')"
                }
                if ($config.Description) {
                    Write-Host "  Description: $($config.Description)"
                }
                if ($config.CreatedDate) {
                    Write-Host "  Created: $($config.CreatedDate)"
                }
            }
        }
    }
    
    "Show" {
        if (-not $ConfigPath) {
            if (-not $ConfigName) {
                $ConfigName = Read-Host "Enter config name"
            }
            $ConfigPath = Join-Path $ConfigDirectory "$ConfigName.json"
        }
        Show-ConfigFile -Path $ConfigPath
    }
    
    "Encrypt" {
        # Re-encrypt all configs (useful after moving to new machine)
        Write-Host "Re-encrypting all configuration files..." -ForegroundColor Cyan
        if (!(Test-Path $ConfigDirectory)) {
            Write-Host "No configuration directory found: $ConfigDirectory" -ForegroundColor Yellow
        }
        else {
            Get-ChildItem -Path $ConfigDirectory -Filter "*.json" | ForEach-Object {
                try {
                    $config = Get-Content $_.FullName -Raw | ConvertFrom-Json
                    
                    if ($config.ClientSecret -and -not $config.ClientSecretEncrypted) {
                        $secureString = ConvertTo-SecureString $config.ClientSecret -AsPlainText -Force
                        $config.ClientSecretEncrypted = ConvertFrom-SecureString $secureString
                        if ($config.PSObject.Properties['ClientSecret']) {
                            $config.PSObject.Properties.Remove('ClientSecret')
                        }
                        $config | ConvertTo-Json -Depth 10 | Set-Content -Path $_.FullName -Encoding UTF8
                        Write-Host "✓ Encrypted: $($_.Name)" -ForegroundColor Green
                    }
                    else {
                        Write-Host "○ Already encrypted or no plain secret: $($_.Name)" -ForegroundColor Gray
                    }
                }
                catch {
                    Write-Host "✗ Failed: $($_.Name) - $_" -ForegroundColor Red
                }
            }
        }
    }
}
