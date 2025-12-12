<#
.SYNOPSIS
    Example configuration using PowerShell splatting for Kopano IMAP to Graph Migration

.DESCRIPTION
    Copy this file and modify the parameters for your environment.
    Run with: .\Run-Migration-Example.ps1

.NOTES
    Splatting allows you to define all parameters in a hashtable
    and pass them cleanly to the script using @params syntax.
#>

# ============================================
# Microsoft Graph API Configuration
# ============================================
$GraphConfig = @{
    TenantId     = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # Your Azure AD Tenant ID
    ClientId     = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # App Registration Client ID
    ClientSecret = "your-client-secret-here"               # App Registration Secret
}

# ============================================
# IMAP Source Server Configuration
# ============================================
$ImapConfig = @{
    ImapServer             = "mail.kopano.local"   # Kopano IMAP server hostname
    ImapPort               = 993                    # 993 for SSL, 143 for STARTTLS
    ImapUseSsl             = $true                  # Use SSL/TLS connection
    ImapSkipCertValidation = $false                 # Set $true for self-signed certs
}

# ============================================
# User List Configuration
# ============================================
$UserConfig = @{
    UserCsvPath = ".\users.csv"                     # CSV with Email,Username,Password,TargetEmail
}

# ============================================
# Migration Options
# ============================================
$MigrationOptions = @{
    # Folder handling
    PreserveFolderStructure = $true                 # Recreate folder structure in target
    IncludeSubfolders       = $true                 # Include subfolders

    # Folders to exclude (common junk/trash folders)
    ExcludeFolders = @(
        "Junk"
        "Spam"
        "Trash"
        "Deleted Items"
        "Drafts"
        "Entwürfe"
        "Papierkorb"
        "Junk-E-Mail"
    )

    # Optional: Only migrate specific folders (comment out to migrate all)
    # FoldersToMigrate = @("INBOX", "Sent", "Sent Items")

    # Date preservation
    PreserveReceivedDate = $true                    # Keep original dates
}

# ============================================
# Date Filtering (Optional)
# ============================================
$DateFilter = @{
    # Uncomment to filter by date range
    # StartDate = [datetime]"2024-01-01"
    # EndDate   = [datetime]"2024-12-31"
}

# ============================================
# Processing Options
# ============================================
$ProcessingOptions = @{
    BatchSize       = 25                            # Messages per batch
    ThrottleMs      = 200                           # Delay between API calls (ms)
    MaxRetries      = 3                             # Retry attempts on failure
    ContinueOnError = $true                         # Don't stop on individual errors

    # Testing options
    # MaxMessagesPerMailbox = 100                   # Limit for testing
    # WhatIf = $true                                # Dry run mode
}

# ============================================
# Logging & Resume
# ============================================
$LoggingOptions = @{
    LogPath        = ".\migration_logs"             # Log directory
    VerboseLogging = $false                         # Enable detailed logging
    StateFile      = ".\migration_state.json"       # For resume capability
    # Resume       = $true                          # Uncomment to resume previous run
}

# ============================================
# Combine all parameters using splatting
# ============================================
$MigrationParams = @{}

# Merge all configuration hashtables
@($GraphConfig, $ImapConfig, $UserConfig, $MigrationOptions, $DateFilter, $ProcessingOptions, $LoggingOptions) | ForEach-Object {
    $_.GetEnumerator() | ForEach-Object {
        if ($null -ne $_.Value -and $_.Value -ne '') {
            $MigrationParams[$_.Key] = $_.Value
        }
    }
}

# ============================================
# Display configuration summary
# ============================================
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Kopano IMAP to Graph Migration" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "IMAP Server:    $($ImapConfig.ImapServer):$($ImapConfig.ImapPort)" -ForegroundColor White
Write-Host "SSL Enabled:    $($ImapConfig.ImapUseSsl)" -ForegroundColor White
Write-Host "User CSV:       $($UserConfig.UserCsvPath)" -ForegroundColor White
Write-Host "Log Path:       $($LoggingOptions.LogPath)" -ForegroundColor White
Write-Host ""

if ($MigrationParams.WhatIf) {
    Write-Host "*** DRY RUN MODE - No changes will be made ***" -ForegroundColor Yellow
    Write-Host ""
}

# ============================================
# Execute migration with splatted parameters
# ============================================
$scriptPath = Join-Path $PSScriptRoot "Kopano-IMAP-to-Graph-Migration.ps1"

if (!(Test-Path $scriptPath)) {
    Write-Error "Migration script not found at: $scriptPath"
    exit 1
}

# Run with splatting
& $scriptPath @MigrationParams
