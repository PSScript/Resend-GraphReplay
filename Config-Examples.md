# Example Configuration Files for Graph Email Replay

## 1. COMPANY-A.json - Basic Configuration
```json
{
    "TenantId": "12345678-1234-1234-1234-123456789012",
    "ClientId": "87654321-4321-4321-4321-210987654321",
    "ClientSecretEncrypted": "AQAAANCMnd8BFdERjHoAwE/Cl+s...",
    "SourceMailboxes": [
        "inbox1@companya.com",
        "inbox2@companya.com"
    ],
    "TargetMailbox": "archive@companya.com",
    "FolderName": "Inbox",
    "ReplayMode": "Transparent",
    "AttachmentsOnly": true,
    "LogPath": "C:\\Logs\\CompanyA\\replay.log",
    "Description": "Company A production email replay",
    "CreatedDate": "2024-01-15 10:30:00",
    "CreatedBy": "admin"
}
```

## 2. COMPANY-B.json - Advanced Configuration with BCC
```json
{
    "TenantId": "98765432-5678-5678-5678-567890123456",
    "ClientId": "11111111-2222-3333-4444-555555555555",
    "ClientSecretEncrypted": "AQAAANCMnd8BFdERjHoAwE/Cl+s...",
    "SourceMailboxes": [
        "sales@companyb.com",
        "support@companyb.com",
        "info@companyb.com"
    ],
    "TargetMailbox": "connector-routing@companyb.com",
    "FolderName": "Inbox",
    "ReplayMode": "Wrapper",
    "AttachmentsOnly": false,
    "BccAlways": [
        "audit@companyb.com",
        "compliance@companyb.com"
    ],
    "SkipAlreadyProcessed": true,
    "ProcessedHeader": "X-CompanyB-Replayed",
    "LogPath": "D:\\EmailReplay\\CompanyB\\logs\\",
    "MaxMessages": 500,
    "BatchSize": 25,
    "ThrottleMs": 200,
    "Description": "Company B with compliance BCC and wrapper mode",
    "CreatedDate": "2024-01-20 14:15:00",
    "CreatedBy": "it-admin"
}
```

## 3. TEST-ENV.json - Test Environment Configuration
```json
{
    "TenantId": "test-tenant-id-here",
    "ClientId": "test-client-id-here",
    "ClientSecretEncrypted": "AQAAANCMnd8BFdERjHoAwE/Cl+s...",
    "SourceMailboxes": [
        "test1@testdomain.com",
        "test2@testdomain.com"
    ],
    "TargetMailbox": "test-archive@testdomain.com",
    "FolderName": "Inbox",
    "ReplayMode": "Transparent",
    "AttachmentsOnly": true,
    "SkipAlreadyProcessed": true,
    "ProcessedHeader": "X-Test-Processed",
    "LogPath": "C:\\Temp\\test_replay.log",
    "MaxMessages": 10,
    "BatchSize": 5,
    "ThrottleMs": 1000,
    "Description": "Test environment - limited to 10 messages",
    "CreatedDate": "2024-01-10 09:00:00",
    "CreatedBy": "developer"
}
```

## 4. MUNICIPAL.json - German Municipality Configuration
```json
{
    "TenantId": "gemeinde-tenant-id",
    "ClientId": "gemeinde-client-id",
    "ClientSecretEncrypted": "AQAAANCMnd8BFdERjHoAwE/Cl+s...",
    "SourceMailboxes": [
        "rathaus@stadt-muehlacker.de",
        "buergerservice@stadt-muehlacker.de",
        "verwaltung@stadt-muehlacker.de"
    ],
    "TargetMailbox": "archiv@stadt-muehlacker.de",
    "FolderName": "Posteingang",
    "ReplayMode": "Wrapper",
    "AttachmentsOnly": true,
    "BccAlways": [
        "datenschutz@stadt-muehlacker.de"
    ],
    "SkipAlreadyProcessed": true,
    "ProcessedHeader": "X-Muehlacker-Archiviert",
    "LogPath": "E:\\Archiv\\Protokolle\\email_replay.log",
    "BatchSize": 30,
    "ThrottleMs": 250,
    "Description": "Stadt Mühlacker E-Mail Archivierung",
    "CreatedDate": "2024-02-01 08:30:00",
    "CreatedBy": "it-stadtmuehlacker"
}
```

## Usage Examples

### Create New Configuration
```powershell
# Interactive creation
.\Manage-GraphReplayConfig.ps1 -Action Create -ConfigName "COMPANY-C"

# Create in specific location
.\Manage-GraphReplayConfig.ps1 -Action Create -ConfigPath "C:\Configs\NewCompany.json"
```

### Use Configuration
```powershell
# Use company configuration
.\Resend-GraphReplay.ps1 -Config "C:\GraphReplay\Configs\COMPANY-A.json"

# Override specific parameters from config
.\Resend-GraphReplay.ps1 -Config "C:\GraphReplay\Configs\COMPANY-B.json" -MaxMessages 100 -WhatIf

# Test mode with config
.\Resend-GraphReplay.ps1 -Config "C:\GraphReplay\Configs\TEST-ENV.json" -TestMode
```

### Manage Configurations
```powershell
# List all configurations
.\Manage-GraphReplayConfig.ps1 -Action List

# Show specific config details
.\Manage-GraphReplayConfig.ps1 -Action Show -ConfigName "COMPANY-A"

# Test configuration
.\Manage-GraphReplayConfig.ps1 -Action Test -ConfigName "COMPANY-B"

# Update configuration
.\Manage-GraphReplayConfig.ps1 -Action Update -ConfigName "MUNICIPAL"

# Re-encrypt all configs (after moving to new machine)
.\Manage-GraphReplayConfig.ps1 -Action Encrypt
```

## Security Notes

1. **Client Secrets are encrypted** using Windows Data Protection API (DPAPI)
   - Only the user who created the config can decrypt on the same machine
   - For shared configs, use Azure Key Vault or certificate-based auth

2. **For Production Use**:
   ```powershell
   # Store config files with restricted permissions
   $configPath = "C:\GraphReplay\Configs"
   $acl = Get-Acl $configPath
   $acl.SetAccessRuleProtection($true, $false)
   $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
       "$env:USERNAME", "FullControl", "Allow"
   )
   $acl.SetAccessRule($rule)
   Set-Acl $configPath $acl
   ```

3. **Automated/Scheduled Tasks**:
   - Use certificate authentication instead of secrets
   - Or use Azure Managed Identity if running from Azure

## Config File Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| TenantId | String | Yes | Azure AD Tenant ID |
| ClientId | String | Yes | App Registration Client ID |
| ClientSecret | String | Yes* | Client Secret (plain) |
| ClientSecretEncrypted | String | Yes* | Encrypted Client Secret |
| SourceMailboxes | Array | Yes | Source mailbox addresses |
| TargetMailbox | String | Yes | Target mailbox address |
| FolderName | String | No | Folder to process (default: Inbox) |
| ReplayMode | String | No | Transparent or Wrapper |
| AttachmentsOnly | Boolean | No | Only process with attachments |
| BccAlways | Array | No | Always BCC these addresses |
| SkipAlreadyProcessed | Boolean | No | Check for header |
| ProcessedHeader | String | No | Header name to check |
| LogPath | String | No | Log file location |
| MaxMessages | Integer | No | Message limit |
| BatchSize | Integer | No | API batch size |
| ThrottleMs | Integer | No | Throttle milliseconds |
| Description | String | No | Config description |
| CreatedDate | String | No | Creation timestamp |
| CreatedBy | String | No | Creator username |

*Either ClientSecret or ClientSecretEncrypted is required
