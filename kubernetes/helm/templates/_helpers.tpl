{{/*
Expand the name of the chart.
*/}}
{{- define "helpers.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "helpers.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "helpers.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "helpers.snellerdName" -}}
{{- if .Values.snellerd.name -}}
{{- .Values.snellerd.name }}
{{- else -}}
{{- printf "%s-snellerd" (include "helpers.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "helpers.sdbName" -}}
{{- if .Values.sdb.name -}}
{{- .Values.sdb.name }}
{{- else -}}
{{- printf "%s-sdb" (include "helpers.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "helpers.snellerdHeadlessName" -}}
{{- printf "%s-headless" (include "helpers.snellerdName" .) -}}
{{- end -}}

{{- define "helpers.s3ConfigMapName" -}}
{{- if .Values.configuration.name -}}
{{- .Values.configuration.name }}
{{- else -}}
{{- printf "%s-s3" (include "helpers.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "helpers.s3SecretName" -}}
{{- if .Values.s3SecretName -}}
{{- .Values.s3SecretName }}
{{- else -}}
{{- printf "%s-s3" (include "helpers.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "helpers.tokenSecretName" -}}
{{- if .Values.secrets.token.secretName -}}
{{- .Values.secrets.token.secretName }}
{{- else -}}
{{- printf "%s-token" (include "helpers.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "helpers.indexSecretName" -}}
{{- if .Values.secrets.index.secretName -}}
{{- .Values.secrets.index.secretName }}
{{- else -}}
{{- printf "%s-index" (include "helpers.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "helpers.snellerToken" -}}
{{- if .Values.secrets.token.values.snellerToken -}}
{{- .Values.secrets.token.values.snellerToken }}
{{- else -}}
{{- $randomToken := randAlphaNum 32 -}}
{{- $randomToken -}}
{{- end -}}
{{- end -}}

{{- define "helpers.snellerIndexKey" -}}
{{- if .Values.secrets.index.values.snellerIndexKey -}}
{{- .Values.secrets.index.values.snellerIndexKey }}
{{- else -}}
{{- $randomIndexKey := sha256sum (randAlphaNum 64) -}}
{{- $randomIndexKey -}}
{{- end -}}
{{- end -}}

{{- define "ingress.proto" -}}
{{- if .Values.ingress.tls -}}
{{- print "https" -}}
{{- else -}}
{{- print "http" -}}
{{- end -}}
{{- end -}}
