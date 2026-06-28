{{- define "sndbx.fullname" -}}
{{- printf "%s-%s" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sndbx.secretName" -}}
{{- if .Values.secrets.name -}}
{{- .Values.secrets.name -}}
{{- else -}}
{{- printf "%s-secret" (include "sndbx.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "sndbx.labels" -}}
releaseVersion: {{ .Values.releaseVersion | default "NA" | quote }}
app: {{ .Chart.Name }}
tier: {{ .Values.tier }}
chartName: {{ .Values.chartName | default .Chart.Name }}
branchName: {{ .Values.branchName | default "master" }}
releaseName: {{ .Values.releaseName | default (include "sndbx.fullname" .) }}
itopsTicket: {{ .Values.itopsTicket | default "NA" }}
buildJobUrl: {{ .Values.buildJobUrl | default "NA" | quote }}
{{- end -}}

{{- define "sndbx.selectorLabels" -}}
app: {{ .Chart.Name }}
tier: {{ .Values.tier }}
{{- end -}}

{{- define "sndbx.image" -}}
{{- printf "%s/%s:%s" (.repo | toString) (.name | toString) (.tag | toString) -}}
{{- end -}}

{{- define "sndbx.apiServiceName" -}}
{{- printf "%s-api-service" (include "sndbx.fullname" .) -}}
{{- end -}}

{{- define "sndbx.runtimeGatewayName" -}}
{{- printf "%s-runtime-gateway" (include "sndbx.fullname" .) -}}
{{- end -}}

{{- define "sndbx.apiServiceFqdn" -}}
{{- printf "%s.%s.svc.cluster.local" (include "sndbx.apiServiceName" .) .Values.namespace -}}
{{- end -}}

{{- define "sndbx.runtimeGatewayServiceFqdn" -}}
{{- printf "%s.%s.svc.cluster.local" (include "sndbx.runtimeGatewayName" .) .Values.namespace -}}
{{- end -}}
