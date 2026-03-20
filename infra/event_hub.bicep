// =============================================================================
// Infraestrutura Event Hub — Retail Media
// Deploy: az deployment group create -g rg-retail-media --template-file infra/event_hub.bicep
// =============================================================================

@description('Ambiente: dev, hml, prd')
@allowed(['dev', 'hml', 'prd'])
param environment string = 'dev'

@description('Região Azure')
param location string = resourceGroup().location

@description('Quantidade de Throughput Units')
@minValue(1)
@maxValue(20)
param capacity int = 1

var namespaceName  = 'evhns-retail-media-${environment}'
var eventHubName   = 'retail-media-events'
var producerPolicy = 'producer-policy'
var consumerPolicy = 'consumer-policy'

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------
resource namespace 'Microsoft.EventHub/namespaces@2022-10-01-preview' = {
  name: namespaceName
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: capacity
  }
  properties: {
    isAutoInflateEnabled: false
    minimumTlsVersion: '1.2'
  }
  tags: {
    project: 'retail-media'
    environment: environment
    managedBy: 'bicep'
  }
}

// ---------------------------------------------------------------------------
// Event Hub
// ---------------------------------------------------------------------------
resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2022-10-01-preview' = {
  parent: namespace
  name: eventHubName
  properties: {
    partitionCount: 4
    messageRetentionInDays: 1
  }
}

// ---------------------------------------------------------------------------
// Políticas de acesso (menor privilégio)
// ---------------------------------------------------------------------------
resource producerRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2022-10-01-preview' = {
  parent: eventHub
  name: producerPolicy
  properties: {
    rights: ['Send']
  }
}

resource consumerRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2022-10-01-preview' = {
  parent: eventHub
  name: consumerPolicy
  properties: {
    rights: ['Listen']
  }
}

// ---------------------------------------------------------------------------
// Consumer Group para o Databricks
// ---------------------------------------------------------------------------
resource databricksCG 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2022-10-01-preview' = {
  parent: eventHub
  name: 'databricks-cg'
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------
output namespaceName string  = namespace.name
output eventHubName  string  = eventHub.name
output producerPolicyName string = producerRule.name
output consumerPolicyName string = consumerRule.name
