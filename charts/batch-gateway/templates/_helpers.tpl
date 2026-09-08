{{/*
Expand the name of the chart.
*/}}
{{- define "batch-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "batch-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "batch-gateway.labels" -}}
helm.sh/chart: {{ include "batch-gateway.chart" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* ========== File Storage Helpers ========== */}}

{{/*
Volume for shared file storage.
- "fs": mounts a PersistentVolumeClaim; global.fileClient.fs.pvcName must be set.
- "s3", "mock": no volume needed.
*/}}
{{- define "batch-gateway.filesStorage.volume" -}}
{{- if eq .Values.global.fileClient.type "fs" }}
{{- if not .Values.global.fileClient.fs.pvcName }}
{{- fail "global.fileClient.fs.pvcName must be set when global.fileClient.type is \"fs\"" }}
{{- end }}
- name: files-storage
  persistentVolumeClaim:
    claimName: {{ .Values.global.fileClient.fs.pvcName }}
{{- end }}
{{- end }}

{{/*
VolumeMount for shared file storage. Only rendered when type is "fs".
*/}}
{{- define "batch-gateway.filesStorage.volumeMount" -}}
{{- if eq .Values.global.fileClient.type "fs" }}
- name: files-storage
  mountPath: {{ .Values.global.fileClient.fs.basePath }}
{{- end }}
{{- end }}

{{/* ========== TLS Validation Helper ========== */}}

{{/*
Validate TLS configuration for a component.
Ensures exactly one TLS mode is active: secretName or certManager.
Usage: {{ include "batch-gateway.validateTLS" (dict "tls" .Values.apiserver.tls "component" "apiserver") }}
*/}}
{{- define "batch-gateway.validateTLS" -}}
{{- if .tls.enabled -}}
  {{- if and .tls.secretName .tls.certManager.enabled -}}
    {{- fail (printf "%s.tls: secretName and certManager are mutually exclusive — set only one" .component) -}}
  {{- end -}}
  {{- if not (or .tls.secretName .tls.certManager.enabled) -}}
    {{- fail (printf "%s.tls: enabled but neither secretName nor certManager is configured" .component) -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/* ========== API Server Helpers ========== */}}

{{/*
API Server fullname
*/}}
{{- define "batch-gateway.apiserver.fullname" -}}
{{- if .Values.apiserver.fullnameOverride }}
{{- .Values.apiserver.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.apiserver.nameOverride }}
{{- if contains $name .Release.Name }}
{{- printf "%s-apiserver" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s-apiserver" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
API Server labels
*/}}
{{- define "batch-gateway.apiserver.labels" -}}
{{ include "batch-gateway.labels" . }}
app.kubernetes.io/name: {{ include "batch-gateway.name" . }}-apiserver
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: apiserver
{{- end }}

{{/*
API Server selector labels
*/}}
{{- define "batch-gateway.apiserver.selectorLabels" -}}
app.kubernetes.io/name: {{ include "batch-gateway.name" . }}-apiserver
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: apiserver
{{- end }}

{{/*
API Server service account name
*/}}
{{- define "batch-gateway.apiserver.serviceAccountName" -}}
{{- if .Values.apiserver.serviceAccount.create }}
{{- default (include "batch-gateway.apiserver.fullname" .) .Values.apiserver.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.apiserver.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
API Server image string
*/}}
{{- define "batch-gateway.apiserver.image" -}}
{{- if .Values.apiserver.image.digest -}}
{{- printf "%s@%s" .Values.apiserver.image.repository .Values.apiserver.image.digest }}
{{- else -}}
{{- $tag := .Values.apiserver.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.apiserver.image.repository $tag }}
{{- end -}}
{{- end }}

{{/* ========== Processor Helpers ========== */}}

{{/*
Processor fullname
*/}}
{{- define "batch-gateway.processor.fullname" -}}
{{- if .Values.processor.fullnameOverride }}
{{- .Values.processor.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.processor.nameOverride }}
{{- if contains $name .Release.Name }}
{{- printf "%s-processor" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s-processor" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Processor labels
*/}}
{{- define "batch-gateway.processor.labels" -}}
{{ include "batch-gateway.labels" . }}
app.kubernetes.io/name: {{ include "batch-gateway.name" . }}-processor
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: processor
{{- end }}

{{/*
Processor selector labels
*/}}
{{- define "batch-gateway.processor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "batch-gateway.name" . }}-processor
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: processor
{{- end }}

{{/*
Processor service account name
*/}}
{{- define "batch-gateway.processor.serviceAccountName" -}}
{{- if .Values.processor.serviceAccount.create }}
{{- default (include "batch-gateway.processor.fullname" .) .Values.processor.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.processor.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Processor image string
*/}}
{{- define "batch-gateway.processor.image" -}}
{{- if .Values.processor.image.digest -}}
{{- printf "%s@%s" .Values.processor.image.repository .Values.processor.image.digest }}
{{- else -}}
{{- $tag := .Values.processor.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.processor.image.repository $tag }}
{{- end -}}
{{- end }}

{{/* ========== Garbage Collector Helpers ========== */}}

{{/*
GC fullname
*/}}
{{- define "batch-gateway.gc.fullname" -}}
{{- if contains .Chart.Name .Release.Name }}
{{- printf "%s-gc" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s-gc" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
GC labels
*/}}
{{- define "batch-gateway.gc.labels" -}}
{{ include "batch-gateway.labels" . }}
app.kubernetes.io/name: {{ include "batch-gateway.name" . }}-gc
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: gc
{{- end }}

{{/*
GC selector labels
*/}}
{{- define "batch-gateway.gc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "batch-gateway.name" . }}-gc
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: gc
{{- end }}

{{/*
GC service account name
*/}}
{{- define "batch-gateway.gc.serviceAccountName" -}}
{{- if .Values.gc.serviceAccount.create }}
{{- default (include "batch-gateway.gc.fullname" .) .Values.gc.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.gc.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
GC image string
*/}}
{{- define "batch-gateway.gc.image" -}}
{{- if .Values.gc.image.digest -}}
{{- printf "%s@%s" .Values.gc.image.repository .Values.gc.image.digest }}
{{- else -}}
{{- $tag := .Values.gc.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.gc.image.repository $tag }}
{{- end -}}
{{- end }}

{{/* ========== Schema Migration Helper ========== */}}

{{/*
initContainer that runs the binary's `migrate` subcommand before the main
container starts. Only rendered for the postgresql backend. Concurrent runs
across pods serialize inside PostgreSQL, so every replica can carry it.
Usage: {{ include "batch-gateway.migrateInitContainer" (dict "root" . "component" .Values.apiserver "image" (include "batch-gateway.apiserver.image" .)) }}
*/}}
{{- define "batch-gateway.migrateInitContainer" -}}
{{- if eq .root.Values.global.dbClient.type "postgresql" }}
initContainers:
- name: migrate
  securityContext:
    {{- toYaml .component.securityContext | nindent 4 }}
  image: {{ .image }}
  imagePullPolicy: {{ .component.image.pullPolicy }}
  args:
  - migrate
  resources:
    {{- toYaml .component.resources | nindent 4 }}
  {{- if .root.Values.global.secretName }}
  volumeMounts:
  - name: secrets
    mountPath: /etc/.secrets
    readOnly: true
  {{- end }}
{{- end }}
{{- end }}
