{{- define "benchmark-tenancy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "benchmark-tenancy.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "benchmark-tenancy.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "benchmark-tenancy.labels" -}}
app.kubernetes.io/name: {{ include "benchmark-tenancy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}
