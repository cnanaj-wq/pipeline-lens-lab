# Installe (ou retire) le chien de garde du lab Airflow : une tâche planifiée Windows qui lance
# scripts\veille_airflow.py toutes les 5 minutes, sous votre compte, sans fenêtre.
#   .\scripts\installer-veille.ps1            # installer ou mettre à jour
#   .\scripts\installer-veille.ps1 -Retirer   # supprimer la tâche
param([switch]$Retirer)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$Nom = 'Veille Airflow (lab)'
$Racine = Split-Path -Parent $PSScriptRoot

if ($Retirer) {
    Unregister-ScheduledTask -TaskName $Nom -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Tâche « $Nom » retirée."
    return
}

$Python = (Get-Command pythonw.exe -ErrorAction SilentlyContinue).Source
if (-not $Python) { throw "pythonw.exe introuvable : installez Python 3 pour Windows." }

$Action = New-ScheduledTaskAction -Execute $Python -Argument "`"$Racine\scripts\veille_airflow.py`"" -WorkingDirectory $Racine
$Declencheur = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5)
$Reglages = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 2) -MultipleInstances IgnoreNew
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $Nom -Action $Action -Trigger $Declencheur -Settings $Reglages -Principal $Principal `
    -Description 'Vérifie toutes les 5 min que le lab Airflow tourne (API, planificateur, DAG en retard) et prévient par mail.' -Force | Out-Null
Write-Host "Tâche « $Nom » installée : toutes les 5 minutes, sous votre compte, sans fenêtre."
Write-Host "Journal : $Racine\logs\veille.log   ·   Essai immédiat : python scripts\veille_airflow.py --essai"
