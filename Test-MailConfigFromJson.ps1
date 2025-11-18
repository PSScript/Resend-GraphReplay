function Test-MailConfigFromJson {
    param(
        [string]$ConfigPath
    )

    if (-not (Test-Path $ConfigPath)) {
        Write-Host "Config file not found: $ConfigPath" -ForegroundColor Red
        return
    }

    # --- Load config JSON and decrypt ClientSecret if needed
    $configJson = Get-Content $ConfigPath -Raw | ConvertFrom-Json
    $TenantId = $configJson.TenantId
    $ClientId = $configJson.ClientId
    $TargetMailbox = $configJson.TargetMailbox

    # Handle secret
    if ($configJson.ClientSecret) {
        $ClientSecret = $configJson.ClientSecret
    } elseif ($configJson.ClientSecretEncrypted) {
        try {
            $sec = ConvertTo-SecureString $configJson.ClientSecretEncrypted
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

    # --- Get token
    $tokenUrl = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    $body = @{
        client_id     = $ClientId
        client_secret = $ClientSecret
        scope         = "https://graph.microsoft.com/.default"
        grant_type    = "client_credentials"
    }

    try {
        $response = Invoke-RestMethod -Method Post -Uri $tokenUrl -ContentType "application/x-www-form-urlencoded" -Body $body
        $accessToken = $response.access_token
        Write-Host "Token acquired." -ForegroundColor Green
    } catch {
        Write-Host "Failed to get access token: $_" -ForegroundColor Red
        return
    }

    # --- Decode JWT (optional diagnostics)
    function Decode-JWT ($jwt) {
        $parts = $jwt -split '\.'
        if ($parts.Length -eq 3) {
            $header = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String(($parts[0] + '===').Substring(0, ($parts[0] + '===' ).Length - ($parts[0].Length % 4))))
            $payload = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String(($parts[1] + '===').Substring(0, ($parts[1] + '===' ).Length - ($parts[1].Length % 4))))
            return @{ header = $header | ConvertFrom-Json; payload = $payload | ConvertFrom-Json }
        }
        return $null
    }

    $jwt = Decode-JWT $accessToken
    if ($jwt) {
        Write-Host "Token roles:" $jwt.payload.roles
        Write-Host "Token audience: $($jwt.payload.aud)"
        Write-Host "AppId: $($jwt.payload.appid)"
    }

    $headers = @{ Authorization = "Bearer $accessToken" }

    # --- Validate mailbox user
    $userUri = "https://graph.microsoft.com/v1.0/users/$TargetMailbox"
    try {
        $user = Invoke-RestMethod -Method Get -Uri $userUri -Headers $headers
        Write-Host "Mailbox found: $($user.displayName) <$($user.userPrincipalName)>" -ForegroundColor Green
    } catch {
        Write-Host "Mailbox user NOT found or no access: $_" -ForegroundColor Red
        return
    }

    # --- Validate Inbox access
    $mailboxUri = "https://graph.microsoft.com/v1.0/users/$TargetMailbox/mailFolders/Inbox"
    try {
        $inbox = Invoke-RestMethod -Method Get -Uri $mailboxUri -Headers $headers
        Write-Host "Exchange mailbox and Inbox accessible." -ForegroundColor Green
    } catch {
        Write-Host "Cannot access mailbox or Inbox: $_" -ForegroundColor Red
        return
    }

    # --- Try to send test email
    $sendMailUri = "https://graph.microsoft.com/v1.0/users/$TargetMailbox/sendMail"
    $testSubject = "[Test-MailConfigFromJson] $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    $mailBody = @{
        message = @{
            subject = $testSubject
            body = @{
                contentType = "Text"
                content = "Test message from Test-MailConfigFromJson"
            }
            toRecipients = @(@{
                emailAddress = @{ address = $TargetMailbox }
            })
        }
        saveToSentItems = $false
    }

    try {
        Invoke-RestMethod -Method Post -Uri $sendMailUri -Headers $headers -Body ($mailBody | ConvertTo-Json -Depth 6)
        Write-Host "sendMail Succeeded! Check the inbox of $TargetMailbox" -ForegroundColor Green
    } catch {
        Write-Host "sendMail FAILED: $($_.Exception.Message)" -ForegroundColor Red
        if ($_.Exception.Response) {
            try {
                $reader = New-Object IO.StreamReader ($_.Exception.Response.GetResponseStream())
                $errText = $reader.ReadToEnd()
                Write-Host "Graph error response: $errText" -ForegroundColor Yellow
            } catch {}
        }
    }

    Write-Host "=== Test-MailConfigFromJson diagnostics complete ===" -ForegroundColor Cyan
}
