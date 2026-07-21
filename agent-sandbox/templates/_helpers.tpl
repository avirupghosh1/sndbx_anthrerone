{{- define "agent-sandbox.fullname" -}}
{{- printf "%s-%s" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent-sandbox.secretName" -}}
{{- if .Values.secrets.name -}}
{{- .Values.secrets.name -}}
{{- else -}}
{{- printf "%s-secret" (include "agent-sandbox.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "agent-sandbox.labels" -}}
releaseVersion: {{ .Values.releaseVersion | default "NA" | quote }}
app: {{ .Chart.Name }}
tier: {{ .Values.tier }}
chartName: {{ .Values.chartName | default .Chart.Name }}
branchName: {{ .Values.branchName | default "master" }}
releaseName: {{ .Values.releaseName | default (include "agent-sandbox.fullname" .) }}
itopsTicket: {{ .Values.itopsTicket | default "NA" }}
buildJobUrl: {{ .Values.buildJobUrl | default "NA" | quote }}
{{- end -}}

{{- define "agent-sandbox.selectorLabels" -}}
app: {{ .Chart.Name }}
tier: {{ .Values.tier }}
{{- end -}}

{{- define "agent-sandbox.image" -}}
{{- printf "%s/%s:%s" (.repo | toString) (.name | toString) (.tag | toString) -}}
{{- end -}}

{{- define "agent-sandbox.apiServiceDeploymentName" -}}
{{- printf "%s-api-%s-deployment" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent-sandbox.apiServiceName" -}}
{{- printf "%s-api-%s-service" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent-sandbox.apiServiceConfigName" -}}
{{- printf "%s-api-%s-config" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent-sandbox.runtimeGatewayStatefulSetName" -}}
{{- printf "%s-runtime-gateway-%s-statefulset" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent-sandbox.runtimeGatewayName" -}}
{{- printf "%s-runtime-gateway-%s-service" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent-sandbox.runtimeGatewayHeadlessName" -}}
{{- printf "%s-runtime-gateway-%s-headless" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent-sandbox.runtimeGatewayConfigName" -}}
{{- printf "%s-runtime-gateway-%s-config" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent-sandbox.apiServiceFqdn" -}}
{{- printf "%s.%s.svc.cluster.local" (include "agent-sandbox.apiServiceName" .) .Values.namespace -}}
{{- end -}}

{{- define "agent-sandbox.runtimeGatewayServiceFqdn" -}}
{{- printf "%s.%s.svc.cluster.local" (include "agent-sandbox.runtimeGatewayName" .) .Values.namespace -}}
{{- end -}}

{{- define "agent-sandbox.templateRegistryName" -}}
{{- include "agent-sandbox.templateRegistryDeploymentName" . -}}
{{- end -}}

{{- define "agent-sandbox.templateRegistryDeploymentName" -}}
{{- printf "%s-template-registry-%s-deployment" .Chart.Name .Values.tier | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent-sandbox.templateRegistryInternalEnabled" -}}
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

{{- define "agent-sandbox.templateRegistryImage" -}}
{{- $image := default "" .Values.templateRegistry.internal.image -}}
{{- if ne $image "" -}}
{{- $image -}}
{{- else -}}
{{- include "agent-sandbox.image" .Values.images.templateRegistry -}}
{{- end -}}
{{- end -}}

{{- define "agent-sandbox.templateRegistryServer" -}}
{{- if eq (include "agent-sandbox.templateRegistryInternalEnabled" .) "true" -}}
{{- printf "%s.%s.svc.cluster.local:%v" (include "agent-sandbox.templateRegistryName" .) .Values.namespace .Values.templateRegistry.internal.servicePort -}}
{{- else -}}
{{- .Values.templateRegistry.server | default "" -}}
{{- end -}}
{{- end -}}

{{- define "agent-sandbox.templateRegistryRepoPrefix" -}}
{{- if ne (default "" .Values.templateRegistry.repoPrefix) "" -}}
{{- .Values.templateRegistry.repoPrefix -}}
{{- else if eq (include "agent-sandbox.templateRegistryInternalEnabled" .) "true" -}}
{{- printf "%s/templates" (include "agent-sandbox.templateRegistryServer" .) -}}
{{- else -}}
{{- "" -}}
{{- end -}}
{{- end -}}

{{- define "agent-sandbox.templateRegistryAuthRequired" -}}
{{- if eq (include "agent-sandbox.templateRegistryInternalEnabled" .) "true" -}}
false
{{- else -}}
{{- .Values.templateRegistry.authRequired | toString -}}
{{- end -}}
{{- end -}}

{{- define "agent-sandbox.imageBuildingAuthRequired" -}}
{{- .Values.imageBuilding.authRequired | toString -}}
{{- end -}}
