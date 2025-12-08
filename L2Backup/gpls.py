slaListQuery = """
query SLAListQuery($after: String, $first: Int, $filter: [GlobalSlaFilterInput!], $sortBy: SlaQuerySortByField, $sortOrder: SortOrder, $shouldShowProtectedObjectCount: Boolean, $shouldShowPausedClusters: Boolean = false, $isOneToOneReplicationEnabled: Boolean!, $isCnpAwsS3MultipleBackupLocationsEnabled: Boolean!) {
  slaDomains(after: $after, first: $first, filter: $filter, sortBy: $sortBy, sortOrder: $sortOrder, shouldShowProtectedObjectCount: $shouldShowProtectedObjectCount, shouldShowPausedClusters: $shouldShowPausedClusters) {
    edges {
      node {
        name
        ...AllObjectSpecificConfigsForSLAFragment
        ...SlaAssignedToOrganizationsFragment
        ...BaseFrequencyFragment
        ...ArchivalLocationFragment
        ... on ClusterSlaDomain {
          id: fid
          protectedObjectCount
          cluster {
            id
            name
          }
          ownerOrg {
            id
            name
          }
          replicationSpecsV2 {
            ...DetailedReplicationSpecsV2ForSlaDomainFragment
          }
          localRetentionLimit {
            duration
            unit
          }
          isRetentionLockedSla
          retentionLockMode
          isReadOnly
        }
        ... on GlobalSlaReply {
          id
          objectTypes
          description
          protectedObjectCount
          ownerOrg {
            id
            name
            __typename
          }
          replicationSpecsV2 {
            ...DetailedReplicationSpecsV2ForSlaDomainFragment
            __typename
          }
          backupLocationSpecs @include(if: $isCnpAwsS3MultipleBackupLocationsEnabled) {
            archivalGroup {
              name
              __typename
            }
            __typename
          }
          localRetentionLimit {
            duration
            unit
            __typename
          }
          pausedClustersInfo @include(if: $shouldShowPausedClusters) {
            pausedClustersCount
            pausedClusters {
              id
              name
              __typename
            }
            __typename
          }
          objectTypes
          isRetentionLockedSla
          retentionLockMode
          isReadOnly
          __typename
        }
        __typename
      }
      __typename
    }
    pageInfo {
      endCursor
      hasNextPage
      hasPreviousPage
      __typename
    }
    __typename
  }
}

fragment BaseFrequencyFragment on SlaDomain {
  ... on GlobalSlaReply {
    objectSpecificConfigs {
      awsRdsConfig {
        logRetention {
          duration
          unit
          __typename
        }
        __typename
      }
      awsNativeS3SlaConfig {
        continuousBackupRetentionInDays
        __typename
      }
      __typename
    }
    snapshotSchedule {
      ...SnapshotSchedulesForSlaDomainFragment
      __typename
    }
    baseFrequency {
      duration
      unit
      __typename
    }
    __typename
  }
  ... on ClusterSlaDomain {
    snapshotSchedule {
      ...SnapshotSchedulesForSlaDomainFragment
      __typename
    }
    baseFrequency {
      duration
      unit
      __typename
    }
    __typename
  }
  __typename
}

fragment SnapshotSchedulesForSlaDomainFragment on SnapshotSchedule {
  minute {
    basicSchedule {
      frequency
      retention
      retentionUnit
      __typename
    }
    __typename
  }
  hourly {
    basicSchedule {
      frequency
      retention
      retentionUnit
      __typename
    }
    __typename
  }
  daily {
    basicSchedule {
      frequency
      retention
      retentionUnit
      __typename
    }
    __typename
  }
  weekly {
    basicSchedule {
      frequency
      retention
      retentionUnit
      __typename
    }
    dayOfWeek
    __typename
  }
  monthly {
    basicSchedule {
      frequency
      retention
      retentionUnit
      __typename
    }
    dayOfMonth
    __typename
  }
  quarterly {
    basicSchedule {
      frequency
      retention
      retentionUnit
      __typename
    }
    dayOfQuarter
    quarterStartMonth
    __typename
  }
  yearly {
    basicSchedule {
      frequency
      retention
      retentionUnit
      __typename
    }
    dayOfYear
    yearStartMonth
    __typename
  }
  __typename
}

fragment ArchivalLocationFragment on SlaDomain {
  ... on GlobalSlaReply {
    objectTypes
    archivalSpecs {
      storageSetting {
        id
        name
        groupType
        targetType
        __typename
      }
      archivalLocationToClusterMapping {
        cluster {
          id
          name
          __typename
        }
        location {
          id
          name
          targetType
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  ... on ClusterSlaDomain {
    archivalSpecs {
      archivalLocationName
      __typename
    }
    archivalSpec {
      archivalLocationName
      __typename
    }
    __typename
  }
  __typename
}

fragment AllObjectSpecificConfigsForSLAFragment on SlaDomain {
  objectSpecificConfigs {
    awsRdsConfig {
      logRetention {
        duration
        unit
        __typename
      }
      __typename
    }
    sapHanaConfig {
      incrementalFrequency {
        duration
        unit
        __typename
      }
      differentialFrequency {
        duration
        unit
        __typename
      }
      logRetention {
        duration
        unit
        __typename
      }
      storageSnapshotConfig {
        frequency {
          duration
          unit
          __typename
        }
        retention {
          duration
          unit
          __typename
        }
        __typename
      }
      __typename
    }
    db2Config {
      incrementalFrequency {
        duration
        unit
        __typename
      }
      differentialFrequency {
        duration
        unit
        __typename
      }
      logRetention {
        duration
        unit
        __typename
      }
      logArchivalMethod
      __typename
    }
    postgresDbClusterSlaConfig {
      logRetention {
        duration
        unit
        __typename
      }
      __typename
    }
    mysqldbSlaConfig {
      logFrequency {
        duration
        unit
        __typename
      }
      logRetention {
        duration
        unit
        __typename
      }
      __typename
    }
    oracleConfig {
      frequency {
        duration
        unit
        __typename
      }
      logRetention {
        duration
        unit
        __typename
      }
      hostLogRetention {
        duration
        unit
        __typename
      }
      __typename
    }
    informixSlaConfig {
      ...InformixSlaConfigFragment
      __typename
    }
    mongoConfig {
      logFrequency {
        duration
        unit
        __typename
      }
      logRetention {
        duration
        unit
        __typename
      }
      __typename
    }
    mssqlConfig {
      frequency {
        duration
        unit
        __typename
      }
      logRetention {
        duration
        unit
        __typename
      }
      __typename
    }
    oracleConfig {
      frequency {
        duration
        unit
        __typename
      }
      logRetention {
        duration
        unit
        __typename
      }
      hostLogRetention {
        duration
        unit
        __typename
      }
      __typename
    }
    vmwareVmConfig {
      logRetentionSeconds
      __typename
    }
    azureSqlDatabaseDbConfig {
      logRetentionInDays
      __typename
    }
    azureSqlManagedInstanceDbConfig {
      logRetentionInDays
      __typename
    }
    awsNativeS3SlaConfig {
      archivalLocationId
      archivalLocationName
      continuousBackupRetentionInDays
      __typename
    }
    azureBlobConfig {
      backupLocationId
      backupLocationName
      __typename
    }
    ncdSlaConfig {
      minutelyBackupLocations
      hourlyBackupLocations
      dailyBackupLocations
      weeklyBackupLocations
      monthlyBackupLocations
      quarterlyBackupLocations
      yearlyBackupLocations
      __typename
    }
    informixSlaConfig {
      ...InformixSlaConfigFragment
      __typename
    }
    __typename
  }
  __typename
}

fragment InformixSlaConfigFragment on InformixSlaConfig {
  logFrequency {
    duration
    unit
    __typename
  }
  logRetention {
    duration
    unit
    __typename
  }
  incrementalFrequency {
    duration
    unit
    __typename
  }
  incrementalRetention {
    duration
    unit
    __typename
  }
  __typename
}

fragment DetailedReplicationSpecsV2ForSlaDomainFragment on ReplicationSpecV2 {
  replicationPairs @include(if: $isOneToOneReplicationEnabled) {
    sourceCluster {
      id
      name
      __typename
    }
    targetCluster {
      id
      name
      version
      clusterInfo {
        ... on LocalClusterInfo {
          isIsolated: isAirGapped
          isConnected
          __typename
        }
        ... on CrossAccountClusterInfo {
          originAccount
          isConnected
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  replicationLocalRetentionDuration {
    duration
    unit
    __typename
  }
  cascadingArchivalSpecs {
    archivalTieringSpec {
      coldStorageClass
      shouldTierExistingSnapshots
      minAccessibleDurationInSeconds
      isInstantTieringEnabled
      __typename
    }
    archivalLocationToClusterMapping @include(if: $isOneToOneReplicationEnabled) {
      cluster {
        id
        name
        version
        clusterInfo {
          ... on LocalClusterInfo {
            isIsolated: isAirGapped
            isConnected
            __typename
          }
          ... on CrossAccountClusterInfo {
            originAccount
            isConnected
            __typename
          }
          __typename
        }
        __typename
      }
      location {
        id
        name
        targetType
        __typename
      }
      __typename
    }
    archivalLocation {
      id
      name
      targetType
      ... on RubrikManagedAwsTarget {
        storageClass
        immutabilitySettings {
          lockDurationDays
          isObjectLockEnabled
          __typename
        }
        __typename
      }
      ... on RubrikManagedAzureTarget {
        immutabilitySettings {
          lockDurationDays
          __typename
        }
        __typename
      }
      ... on CdmManagedAwsTarget {
        storageClass
        immutabilitySettings {
          lockDurationDays
          __typename
        }
        __typename
      }
      ... on CdmManagedAzureTarget {
        immutabilitySettings {
          lockDurationDays
          __typename
        }
        __typename
      }
      ... on RubrikManagedRcsTarget {
        immutabilityPeriodDays
        syncStatus
        tier
        __typename
      }
      ... on RubrikManagedS3CompatibleTarget {
        immutabilitySetting {
          bucketLockDurationDays
          __typename
        }
        __typename
      }
      __typename
    }
    frequency
    archivalThreshold {
      duration
      unit
      __typename
    }
    __typename
  }
  retentionDuration {
    duration
    unit
    __typename
  }
  cluster {
    id
    name
    version
    clusterInfo {
      ... on LocalClusterInfo {
        isIsolated: isAirGapped
        isConnected
        __typename
      }
      ... on CrossAccountClusterInfo {
        originAccount
        isConnected
        __typename
      }
      __typename
    }
    __typename
  }
  targetMapping {
    id
    name
    targets {
      id
      name
      cluster {
        id
        name
        __typename
      }
      __typename
    }
    __typename
  }
  awsTarget {
    accountId
    accountName
    region
    __typename
  }
  azureTarget {
    region
    __typename
  }
  __typename
}

fragment SlaAssignedToOrganizationsFragment on SlaDomain {
  ... on GlobalSlaReply {
    allOrgsHavingAccess {
      id
      name
      __typename
    }
    __typename
  }
  __typename
}
"""

slaListQueryVars = """
{
  "shouldShowPausedClusters": true,
  "filter": [],
  "sortBy": "NAME",
  "sortOrder": "ASC",
  "shouldShowProtectedObjectCount": true,
  "isOneToOneReplicationEnabled": true,
  "isCnpAwsS3MultipleBackupLocationsEnabled": false
}
"""

protectedObjectListQuery = """
query ProtectedObjectListQuery($slaIds: [UUID!]!, $first: Int, $after: String, $sortBy: ObjectQuerySortByParamInput, $filter: GetProtectedObjectsFilterInput) {
  slaProtectedObjects(slaIds: $slaIds, first: $first, after: $after, sortBy: $sortBy, filter: $filter) {
    edges {
      node {
        id
        name
        objectType
        slaPauseStatus
        protectionStatus
        isPrimary
        cluster {
          id
          name
        }
      }
    }
  }
}
"""

protectedObjectListQueryVars = """
{
  "slaIds": [
    "REPLACEME"
  ]
}
"""

odsSnapshotListfromSnappable = """
query SnapshotsListSingleQuery(
  $snappableId: String!
  $first: Int
  $after: String
  $snapshotFilter: [SnapshotQueryFilterInput!]
  $sortBy: SnapshotQuerySortByField
  $sortOrder: SortOrder
  $timeRange: TimeRangeInput
  $includeSapHanaAppMetadata: Boolean!
  $includeDb2AppMetadata: Boolean!
  $includeMongoSourceAppMetadata: Boolean!
  $isLegalHoldThroughRbacEnabled: Boolean = false
  $isStaticRetentionEnabled: Boolean = false
  $isBackupLocationSupported: Boolean = false
  $includeOnlySourceSnapshots: Boolean = false
) {
  snapshotsListConnection: snapshotOfASnappableConnection(
    workloadId: $snappableId
    first: $first
    after: $after
    snapshotFilter: $snapshotFilter
    sortBy: $sortBy
    sortOrder: $sortOrder
    timeRange: $timeRange
    includeOnlySourceSnapshots: $includeOnlySourceSnapshots
  ) {
    edges {

      node {
        ...CdmSnapshotLatestUserNotesFragment
        id
        date
        expirationDate
        isOnDemandSnapshot
        ... on CdmSnapshot {
          cdmVersion
          isRetentionLocked
          isDownloadedSnapshot
          cluster {
            id
            name
            version
            status
            timezone
            __typename
          }
          pendingSnapshotDeletion {
            id: snapshotFid
            status
            __typename
          }
          slaDomain {
            ...EffectiveSlaDomainFragment
            __typename
          }
          pendingSla {
            ...SLADomainFragment
            __typename
          }
          snapshotRetentionInfo {
            isCustomRetentionApplied
            archivalInfos {
              name
              isExpirationDateCalculated
              expirationTime
              locationId
              isSnapshotOnLegalHold @include(if: $isLegalHoldThroughRbacEnabled)
              __typename
            }
            localInfo {
              name
              isExpirationDateCalculated
              expirationTime
              isSnapshotOnLegalHold @include(if: $isLegalHoldThroughRbacEnabled)
              __typename
            }
            replicationInfos {
              name
              isExpirationDateCalculated
              expirationTime
              locationId
              isExpirationInformationUnavailable
              isSnapshotOnLegalHold @include(if: $isLegalHoldThroughRbacEnabled)
              __typename
            }
            __typename
          }
          sapHanaAppMetadata @include(if: $includeSapHanaAppMetadata) {
            backupId
            backupPrefix
            snapshotType
            files {
              backupFileSizeInBytes
              __typename
            }
            __typename
          }
          db2AppMetadata @include(if: $includeDb2AppMetadata) {
            backupId
            snapshotType
            files {
              backupFileSizeInBytes
              __typename
            }
            __typename
          }
          mongoSourceAppMetadata @include(if: $includeMongoSourceAppMetadata) {
            isFullSnapshot
            snapshotSize
            __typename
          }
          legalHoldInfo {
            shouldHoldInPlace
            __typename
          }
          __typename
        }
        ... on PolarisSnapshot {
          archivalLocationId
          archivalLocationName
          isDeletedFromSource
          isDownloadedSnapshot
          isReplica
          isArchivalCopy
          snappableId
          slaDomain {
            name
            ...EffectiveSlaDomainFragment
            ... on ClusterSlaDomain {
              fid
              cluster {
                id
                name
                __typename
              }
              __typename
            }
            ... on GlobalSlaReply {
              id
              __typename
            }
            __typename
          }
          pendingSla {
            ...SLADomainFragment
            __typename
          }
          polarisSpecificSnapshot @include(if: $isBackupLocationSupported) {
            snapshotId
            __typename
            ... on AwsNativeS3SpecificSnapshot {
              snapshotStartTime
              __typename
            }
          }
          ...PolarisSnapshotRetentionInfoFragment
            @include(if: $isStaticRetentionEnabled)
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}

fragment EffectiveSlaDomainFragment on SlaDomain {
  id
  name
  ... on GlobalSlaReply {
    isRetentionLockedSla
    retentionLockMode
    __typename
  }
  ... on ClusterSlaDomain {
    fid
    cluster {
      id
      name
      __typename
    }
    isRetentionLockedSla
    retentionLockMode
    __typename
  }
  __typename
}

fragment SLADomainFragment on SlaDomain {
  id
  name
  ... on ClusterSlaDomain {
    fid
    cluster {
      id
      name
      __typename
    }
    __typename
  }
  __typename
}

fragment CdmSnapshotLatestUserNotesFragment on CdmSnapshot {
  latestUserNote {
    time
    userName
    userNote
    __typename
  }
  __typename
}

fragment PolarisSnapshotRetentionInfoFragment on PolarisSnapshot {
  isRetentionLocked
  archivalLocationName
  snapshotRetentionInfo {
    isCustomRetentionApplied
    localInfo {
      locationName
      expirationTime
      isExpirationDateCalculated
      isSnapshotPresent
      __typename
    }
    archivalInfos {
      locationName
      expirationTime
      isExpirationDateCalculated
      isSnapshotPresent
      __typename
    }
    replicationInfos {
      locationName
      expirationTime
      isExpirationDateCalculated
      isSnapshotPresent
      __typename
    }
    __typename
  }
  __typename
}
"""

odsSnapshotListfromSnappableVars = """
{
  "isLegalHoldThroughRbacEnabled": true,
  "isStaticRetentionEnabled": true,
  "isBackupLocationSupported": false,
  "includeOnlySourceSnapshots": false,
  "snappableId": "REPLACEME",
  "sortBy": "CREATION_TIME",
  "sortOrder": "DESC",
  "includeSapHanaAppMetadata": false,
  "includeDb2AppMetadata": false,
  "includeMongoSourceAppMetadata": false,
  "snapshotFilter": [],
  "timeRange": null
}
"""

# ============================================================
# FILESETS QUERY – Windows & Linux (All Filesets in CDM)
# ============================================================

filesetTemplateQuery = """
query FilesetTemplateListQuery(
  $hostRoot: HostRoot!
  $first: Int!
  $after: String
  $filter: [Filter!]!
  $sortBy: HierarchySortByField
  $sortOrder: SortOrder
  $hostIdFilter: [Filter!]
) {
  filesetTemplates(
    hostRoot: $hostRoot
    first: $first
    after: $after
    filter: $filter
    sortBy: $sortBy
    sortOrder: $sortOrder
  ) {
    edges {
      node {
        id
        name
        cluster {
          name
          id
          status
          version
          __typename
        }
        physicalChildConnection(filter: $hostIdFilter) {
          count
          edges {
            node {
              id
              name
              cluster { name __typename }
              ... on WindowsFileset {
                isPassThrough
                __typename
              }
              ... on LinuxFileset {
                isPassThrough
                symlinkResolutionEnabled
                hardlinkSupportEnabled
                __typename
              }
              effectiveSlaDomain { name id __typename }
              physicalPath { name __typename }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}
"""

filesetWindowsVars = """
{
  "hostRoot": "WINDOWS_HOST_ROOT",
  "first": 100,
  "filter": [
    { "field": "IS_RELIC", "texts": ["false"] },
    { "field": "IS_REPLICATED", "texts": ["false"] }
  ],
  "sortBy": "NAME",
  "sortOrder": "ASC",
  "hostIdFilter": []
}
"""

filesetLinuxVars = """
{
  "hostRoot": "LINUX_HOST_ROOT",
  "first": 100,
  "filter": [
    { "field": "IS_RELIC", "texts": ["false"] },
    { "field": "IS_REPLICATED", "texts": ["false"] }
  ],
  "sortBy": "NAME",
  "sortOrder": "ASC",
  "hostIdFilter": []
}
"""
