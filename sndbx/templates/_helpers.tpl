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

{{- define "sndbx.runtimeGatewayHeadlessName" -}}
{{- printf "%s-headless" (include "sndbx.runtimeGatewayName" .) -}}
{{- end -}}

{{- define "sndbx.apiServiceFqdn" -}}
{{- printf "%s.%s.svc.cluster.local" (include "sndbx.apiServiceName" .) .Values.namespace -}}
{{- end -}}

{{- define "sndbx.runtimeGatewayServiceFqdn" -}}
{{- printf "%s.%s.svc.cluster.local" (include "sndbx.runtimeGatewayName" .) .Values.namespace -}}
{{- end -}}

{{- define "sndbx.templateRegistryName" -}}
{{- printf "%s-template-registry" (include "sndbx.fullname" .) -}}
{{- end -}}

{{- define "sndbx.templateRegistryInternalEnabled" -}}
{{- $mode := "auto" -}}
{{- if and .Values.templateRegistry.internal (hasKey .Values.templateRegistry.internal "enabled") -}}
{{- $mode = (toString .Values.templateRegistry.internal.enabled | lower) -}}
{{- end -}}
{{- if eq $mode "true" -}}
true
{{- else if and (eq $mode "auto") .Values.templateRegistry.pushEnabled (eq (default "" .Values.templateRegistry.repoPrefix) "") -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{- define "sndbx.templateRegistryImage" -}}
{{- $image := default "" .Values.templateRegistry.internal.image -}}
{{- if ne $image "" -}}
{{- $image -}}
{{- else -}}
{{- include "sndbx.image" .Values.images.templateRegistry -}}
{{- end -}}
{{- end -}}

{{- define "sndbx.templateRegistryServer" -}}
{{- if eq (include "sndbx.templateRegistryInternalEnabled" .) "true" -}}
{{- printf "%s.%s.svc.cluster.local:%v" (include "sndbx.templateRegistryName" .) .Values.namespace .Values.templateRegistry.internal.servicePort -}}
{{- else -}}
{{- .Values.templateRegistry.server | default "" -}}
{{- end -}}
{{- end -}}

{{- define "sndbx.templateRegistryRepoPrefix" -}}
{{- if ne (default "" .Values.templateRegistry.repoPrefix) "" -}}
{{- .Values.templateRegistry.repoPrefix -}}
{{- else if eq (include "sndbx.templateRegistryInternalEnabled" .) "true" -}}
{{- printf "%s/templates" (include "sndbx.templateRegistryServer" .) -}}
{{- else -}}
{{- "" -}}
{{- end -}}
{{- end -}}

{{- define "sndbx.templateRegistryAuthRequired" -}}
{{- if eq (include "sndbx.templateRegistryInternalEnabled" .) "true" -}}
false
{{- else -}}
{{- .Values.templateRegistry.authRequired | toString -}}
{{- end -}}
{{- end -}}
