function Dump-MailHeadersFromConfig {
    param(
        [Parameter(Mandatory)]
        [string]$ConfigPath,
        [string]$FolderName = "Inbox",
        [int]$Top = 50 # How many messages to fetch
    )
    # Load and decrypt config
    if (-not (Test-Path $ConfigPath)) {
        Write-Host "Config file not found: $ConfigPath" -ForegroundColor Red
        return
    }
    $cfg = Get-Content $ConfigPath -Raw | ConvertFrom-Json
    $TenantId = $cfg.TenantId
    $ClientId = $cfg.ClientId
    $Mailbox = $cfg.TargetMailbox
    # Secret logic
    if ($cfg.ClientSecret) {
        $ClientSecret = $cfg.ClientSecret
    } elseif ($cfg.ClientSecretEncrypted) {
        try {
            $sec = ConvertTo-SecureString $cfg.ClientSecretEncrypted
            $ClientSecret = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
                [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec)
            )
        } catch {
            Write-Host "Failed to decrypt ClientSecret." -ForegroundColor Red
            return
        }
    } else {
        Write-Host "No ClientSecret found in config." -ForegroundColor Red
        return
    }
    # Acquire token
    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    $body = @{
        client_id     = $ClientId
        client_secret = $ClientSecret
        scope         = "https://graph.microsoft.com/.default"
        grant_type    = "client_credentials"
    }
    try {
        $response = Invoke-RestMethod -Method Post -Uri $tokenUrl -ContentType "application/x-www-form-urlencoded" -Body $body
        $token = $response.access_token
        Write-Host "Token acquired." -ForegroundColor Green
    } catch {
        Write-Host "Failed to get token: $_" -ForegroundColor Red
        return
    }
    $headers = @{ Authorization = "Bearer $token" }
    # Determine output folder
    $headerDir = Join-Path (Split-Path $ConfigPath) "headers"
    if (!(Test-Path $headerDir)) { New-Item -ItemType Directory -Path $headerDir | Out-Null }

    # Query messages
    $folderNameGraph = switch -Regex ($FolderName) {
        "^(Inbox|Posteingang)$" { "inbox" }
        "^(Sent|Gesendete)" { "sentitems" }
        "^(Draft|Entwurf)" { "drafts" }
        "^(Deleted|Gelöscht)" { "deleteditems" }
        default { $FolderName }
    }
    $uri = "https://graph.microsoft.com/v1.0/users/$Mailbox/mailFolders/$folderNameGraph/messages?`$top=$Top&`$select=id,subject,from,receivedDateTime,internetMessageHeaders"
    Write-Host "Fetching $Top messages from $Mailbox/$FolderName..." -ForegroundColor Cyan
    try {
        $result = Invoke-RestMethod -Uri $uri -Headers $headers
        $msgs = $result.value
        Write-Host "Fetched $($msgs.Count) messages." -ForegroundColor Green
    } catch {
        Write-Host "Failed to fetch messages: $_" -ForegroundColor Red
        return
    }
    # Write headers to file per message
    foreach ($msg in $msgs) {
        $id = $msg.id -replace '[^a-zA-Z0-9]', '_'
        $file = Join-Path $headerDir "$Mailbox`_$id.headers.txt"
        $content = "Subject: $($msg.subject)`r`nFrom: $($msg.from.emailAddress.address)`r`nReceived: $($msg.receivedDateTime)`r`nMessageId: $($msg.id)`r`n--- Headers ---`r`n"
        foreach ($hdr in $msg.internetMessageHeaders) {
            $content += "$($hdr.name): $($hdr.value)`r`n"
        }
        Set-Content -Path $file -Value $content -Encoding UTF8
        Write-Host "Wrote: $file" -ForegroundColor Gray
    }
    Write-Host "`nDone. See: $headerDir" -ForegroundColor Magenta
}
