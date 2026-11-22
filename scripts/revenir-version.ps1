<#
.SYNOPSIS
  Revient à la version d'Airflow précédente et restaure la base sauvegardée par monter-version.ps1.

.DESCRIPTION
  Lit AIRFLOW_VERSION_PRECEDENTE et AIRFLOW_SAUVEGARDE_PRECEDENTE dans .env, arrête les services,
  restaure la base (pg_restore --clean), remet l'ancienne version et redémarre.
  La base doit être restaurée : une base migrée vers une version plus récente n'est pas
  lisible par l'ancienne version d'Airflow.
#>
param([switch]$Oui)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$racine = Split-Path -Parent $PSScriptRoot
Set-Location $racine
$compose = @('compose', '-f', 'docker-compose.yaml', '-f', 'docker-compose.monitoring.yaml')
if (-not (Test-Path 'docker-compose.monitoring.yaml')) { $compose = @('compose', '-f', 'docker-compose.yaml') }
function Lire-Env($cle) {
  $l = Get-Content .env | Where-Object { $_ -match "^$cle=" } | Select-Object -First 1
  if ($l) { return $l.Split('=', 2)[1].Trim() } else { return $null }
}

$precedente = Lire-Env 'AIRFLOW_VERSION_PRECEDENTE'
$sauvegarde = Lire-Env 'AIRFLOW_SAUVEGARDE_PRECEDENTE'
if (-not $precedente -or -not $sauvegarde -or -not (Test-Path $sauvegarde)) { throw 'Aucune montée de version à annuler (version ou sauvegarde précédente introuvable dans .env).' }
Write-Host "Retour à Airflow $precedente avec la sauvegarde $sauvegarde."
if (-not $Oui) {
  $r = Read-Host 'La base actuelle sera remplacée par la sauvegarde. Continuer ? (oui/non)'
  if ($r -ne 'oui') { Write-Host 'Annulé.'; exit 0 }
}

docker @compose stop airflow-apiserver airflow-scheduler airflow-dag-processor
docker @compose cp $sauvegarde postgres:/tmp/restauration.dump
docker @compose exec -T postgres pg_restore -U airflow -d airflow --clean --if-exists /tmp/restauration.dump
(Get-Content .env) -replace '^AIRFLOW_VERSION=.*', "AIRFLOW_VERSION=$precedente" | Set-Content .env -Encoding utf8
docker @compose up -d
for ($i = 0; $i -lt 60; $i++) {
  try { if ((Invoke-RestMethod 'http://localhost:8085/api/v2/version' -TimeoutSec 3).version -eq $precedente) { Write-Host "Airflow $precedente répond à nouveau." -ForegroundColor Green; exit 0 } } catch { }
  Start-Sleep 3
}
throw "Airflow $precedente ne répond pas : consultez docker compose logs airflow-apiserver."
