<#
.SYNOPSIS
  Monte le lab vers une autre version d'Apache Airflow, avec sauvegarde, contrôles et retour arrière.

.DESCRIPTION
  1. Vérifie que l'image apache/airflow:<Version> existe.
  2. Sauvegarde la base de métadonnées (pg_dump) dans backups\.
  3. Vérifie le contrat d'API de Pipeline Lens contre la NOUVELLE version, avant toute migration.
  4. Met à jour AIRFLOW_VERSION dans .env (l'ancienne est conservée dans AIRFLOW_VERSION_PRECEDENTE).
  5. Télécharge l'image, migre la base (airflow db migrate), redémarre les services.
  6. Attend que l'API réponde avec la bonne version, puis lance les tests des DAG et les tests réels de Pipeline Lens.
  En cas d'échec : la commande de retour arrière est affichée (scripts\revenir-version.ps1).
  Aucun mot de passe n'est lu ni affiché par ce script.

.EXAMPLE
  .\scripts\monter-version.ps1 -Version 3.4.1
  .\scripts\monter-version.ps1 -Version 3.4.1 -Simulation   # contrôles seulement, rien n'est modifié
#>
param(
  [Parameter(Mandatory = $true)][ValidatePattern('^\d+\.\d+\.\d+([a-z0-9.\-]*)?$')][string]$Version,
  [switch]$Simulation
)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$racine = Split-Path -Parent $PSScriptRoot
Set-Location $racine
$compose = @('compose', '-f', 'docker-compose.yaml', '-f', 'docker-compose.monitoring.yaml')
if (-not (Test-Path 'docker-compose.monitoring.yaml')) { $compose = @('compose', '-f', 'docker-compose.yaml') }

function Etape($t) { Write-Host "`n== $t" -ForegroundColor Cyan }
function Lire-Env($cle) {
  $l = Get-Content .env | Where-Object { $_ -match "^$cle=" } | Select-Object -First 1
  if ($l) { return $l.Split('=', 2)[1].Trim() } else { return $null }
}
function Ecrire-Env($cle, $valeur) {
  $lignes = @(Get-Content .env)
  if ($lignes -match "^$cle=") { $lignes = $lignes -replace "^$cle=.*", "$cle=$valeur" } else { $lignes += "$cle=$valeur" }
  Set-Content .env $lignes -Encoding utf8
}
function Attendre-Api($attendue) {
  for ($i = 0; $i -lt 60; $i++) {
    try {
      $v = Invoke-RestMethod -Uri 'http://localhost:8085/api/v2/version' -TimeoutSec 3
      if ($v.version -eq $attendue) { return $true }
    } catch { }
    Start-Sleep -Seconds 3
  }
  return $false
}

$actuelle = Lire-Env 'AIRFLOW_VERSION'
if (-not $actuelle) { $actuelle = '3.4.0' }
Write-Host "Version actuelle : $actuelle  ->  cible : $Version"
if ($actuelle -eq $Version) { Write-Host 'Version identique : la procédure sert alors de répétition générale.' -ForegroundColor Yellow }

Etape "1. Image apache/airflow:$Version"
docker manifest inspect "apache/airflow:$Version" *> $null
if ($LASTEXITCODE -ne 0) { throw "L'image apache/airflow:$Version est introuvable sur Docker Hub." }
Write-Host 'Image disponible.'

Etape '2. Contrat d''API de Pipeline Lens contre la nouvelle version'
# On démarre un conteneur éphémère de la nouvelle version pour lire sa spécification OpenAPI,
# sans toucher au lab en service.
docker pull -q "apache/airflow:$Version" | Out-Null
$spec = Join-Path $env:TEMP "openapi-airflow-$Version.json"
# Seule la ligne marquée « @@OPENAPI@@ » est conservée : les autres messages du conteneur jetable sont ignorés.
$sortie = docker run --rm --entrypoint python "apache/airflow:$Version" -c "import json; from airflow.api_fastapi.app import create_app; print('@@OPENAPI@@' + json.dumps(create_app().openapi()))" 2>$null
($sortie | Where-Object { $_ -like '@@OPENAPI@@*' } | Select-Object -First 1) -replace '^@@OPENAPI@@', '' | Set-Content $spec -Encoding utf8
if ((Get-Item $spec).Length -lt 1000) { throw "Impossible de lire la spécification OpenAPI de la version $Version." }
Push-Location lens
node --input-type=module -e "import { readFileSync } from 'node:fs'; import { checkContract } from './proxy/airflow-contract.ts'; const e = checkContract(JSON.parse(readFileSync(process.argv[1], 'utf-8'))); if (e.length) { console.log(e.join('\n')); process.exit(1) } console.log('Contrat respecté : toutes les routes, paramètres et champs utilisés existent.')" $spec
$contratOk = $LASTEXITCODE -eq 0
Pop-Location
if (-not $contratOk) { throw "La version $Version casse le contrat de Pipeline Lens (voir ci-dessus) : adaptez lens\proxy avant de monter de version." }

if ($Simulation) { Write-Host "`nSimulation terminée : la version $Version est compatible. Rien n'a été modifié." -ForegroundColor Green; exit 0 }

Etape '3. Sauvegarde de la base de métadonnées'
New-Item -ItemType Directory -Force backups | Out-Null
$sauvegarde = "backups\airflow-$actuelle-$(Get-Date -Format 'yyyyMMdd-HHmmss').dump"
# Fichier écrit dans le conteneur puis copié : aucun passage par la console (format binaire intact).
docker @compose exec -T postgres pg_dump -U airflow -Fc -f /tmp/sauvegarde.dump airflow
if ($LASTEXITCODE -eq 0) { docker @compose cp postgres:/tmp/sauvegarde.dump $sauvegarde }
if ($LASTEXITCODE -ne 0 -or -not (Test-Path $sauvegarde) -or (Get-Item $sauvegarde).Length -lt 1000) { throw 'La sauvegarde a échoué : montée de version annulée.' }
Write-Host "Sauvegarde : $sauvegarde ($([math]::Round((Get-Item $sauvegarde).Length / 1KB)) Ko)"

Etape '4. Mise à jour de .env'
Ecrire-Env 'AIRFLOW_VERSION_PRECEDENTE' $actuelle
Ecrire-Env 'AIRFLOW_VERSION' $Version
Ecrire-Env 'AIRFLOW_SAUVEGARDE_PRECEDENTE' $sauvegarde

$retour = "  .\scripts\revenir-version.ps1"
try {
  Etape '5. Migration de la base et redémarrage'
  docker @compose pull -q
  docker @compose run --rm airflow-init
  if ($LASTEXITCODE -ne 0) { throw 'La migration de la base (airflow db migrate) a échoué.' }
  docker @compose up -d
  if (-not (Attendre-Api $Version)) { throw "L'API ne répond pas en version $Version après 3 minutes." }
  Write-Host "Airflow $Version répond."

  Etape '6. Tests des DAG'
  docker @compose run --rm --no-deps -v "${racine}\tests:/opt/airflow/tests" --entrypoint '' airflow-scheduler bash -c 'pip install -q pytest && python -m pytest /opt/airflow/tests -q -p no:cacheprovider'
  if ($LASTEXITCODE -ne 0) { throw 'Les tests des DAG échouent sur la nouvelle version.' }

  Etape '7. Tests réels de Pipeline Lens (contrat, lecture, droits, rejeu simulé)'
  Push-Location lens
  npm run test:live
  $ok = $LASTEXITCODE -eq 0
  Pop-Location
  if (-not $ok) { throw 'Les tests réels de Pipeline Lens échouent sur la nouvelle version.' }
}
catch {
  Write-Host "`nÉCHEC : $($_.Exception.Message)" -ForegroundColor Red
  Write-Host "Pour revenir à la version $actuelle et restaurer la base sauvegardée :" -ForegroundColor Yellow
  Write-Host $retour
  exit 1
}

Write-Host "`nMontée de version réussie : Airflow $Version, base sauvegardée dans $sauvegarde." -ForegroundColor Green
Write-Host 'Pensez à mettre à jour l''image des workflows GitHub (.github/workflows/dags.yml) et la fixture du contrat : cd lens; npm run contrat:enregistrer.'
