<#
.SYNOPSIS
    Minimal splatting example for quick setup
#>

# All parameters in one hashtable
$params = @{
    # Graph API Auth
    TenantId                = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    ClientId                = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    ClientSecret            = "your-secret-here"

    # IMAP Source
    ImapServer              = "mail.kopano.local"
    ImapPort                = 993
    ImapUseSsl              = $true
    ImapSkipCertValidation  = $false                # $true for self-signed certs

    # Users
    UserCsvPath             = ".\users.csv"

    # Options
    PreserveFolderStructure = $true
    PreserveReceivedDate    = $true
    ContinueOnError         = $true

    # Logging
    LogPath                 = ".\migration_logs"
    StateFile               = ".\migration_state.json"

    # Uncomment for testing
    # WhatIf                = $true
    # MaxMessagesPerMailbox = 50
    # StartDate             = [datetime]"2024-01-01"
}

# Run migration
& "$PSScriptRoot\Kopano-IMAP-to-Graph-Migration.ps1" @params
