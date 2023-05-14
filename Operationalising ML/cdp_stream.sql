SELECT 
  GetMetadataPropertyValue(grasstree, 'EventId') AS DataID,
  grasstree.name,
  null as holeID,
  TRY_CAST(value as float) as [value],
  [timestamp] as [timestamp],
  [site],
  System.Timestamp() as insertedTime,
  null as valueRaw
INTO
  [SensorData]
FROM
  [grasstree-001] AS grasstree
UNION 
SELECT 
  GetMetadataPropertyValue([grosvenor-001], 'EventId') AS DataID,
  [name],
  holeId as holeID,
  TRY_CAST([value] as float) as [value],
  [timestamp] as [timestamp],
  [site],
  System.Timestamp() as insertedTime,
  null as valueRaw
FROM
	[grosvenor-001]
UNION 
SELECT
  GetMetadataPropertyValue([moranbah-001], 'EventId') AS DataID,
  [name],
  holeId as holeID,
  TRY_CAST(value as float) as [value],
  [timestamp] as [timestamp],
  [site],
  System.Timestamp() as insertedTime,
  null as valueRaw
FROM [moranbah-001]
UNION
SELECT
  GetMetadataPropertyValue([cdp-eventhub-001], 'EventId') AS DataID,
  [name],
  holeId as holeID,
  TRY_CAST(value as float) as [value],
  [timestamp] as [timestamp],
  LOWER([site]) as [site],
  System.Timestamp() as insertedTime,
  value as valueRaw
FROM [cdp-eventhub-001]
WHERE TRY_CAST(COALESCE(isValueGood, 1) as bit) = 1

SELECT
	GetMetadataPropertyValue(moranbah, 'EventId') AS LatestSensorReadingsID,
	moranbah.site as SiteName,
	ss.SensorName SensorName,
	moranbah.[name] as [TagName],
	ss.SiteSensorID as SiteSensorID,
	moranbah.holeId as holeID,
	moranbah.[timestamp] as [SensorTimestamp],
	moranbah.[value] as SensorValue,
	CASE
		WHEN ss.HigherIsUnsafe = 0 AND ss.SensorName = 'Goaf Flow' AND moranbah.[value] <= 50 THEN 0
		WHEN ss.SensorSubType = 'Methane' AND moranbah.[value] < 0 THEN 4
		WHEN ss.HigherIsUnsafe = 1 AND moranbah.[value] < ss.ThresholdValueLevel1 THEN 1
		WHEN ss.HigherIsUnsafe = 1 AND moranbah.[value] >= ss.ThresholdValueLevel1 AND moranbah.[value] <= ss.ThresholdValueLevel2 THEN 2
		WHEN ss.HigherIsUnsafe = 1 AND moranbah.[value] > ss.ThresholdValueLevel2 AND moranbah.[value] < COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 3
		WHEN ss.HigherIsUnsafe = 1 AND moranbah.[value] >= COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 4
		WHEN ss.HigherIsUnsafe = 0 AND moranbah.[value] > ss.ThresholdValueLevel1 THEN 1
		WHEN ss.HigherIsUnsafe = 0 AND moranbah.[value] <= ss.ThresholdValueLevel1 AND ss.ThresholdValueLevel2 < moranbah.[value] THEN 2
		WHEN ss.HigherIsUnsafe = 0 AND moranbah.[value] <= ss.ThresholdValueLevel2 AND COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) < moranbah.[value] THEN 3
		WHEN ss.HigherIsUnsafe = 0 AND moranbah.[value] <= COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 4
		ELSE 0
	END AS alertLevel,
	CASE
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(moranbah.[value] as float) = 0 THEN 'Stopped'
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(moranbah.[value] as float) = 1 THEN 'Towards MG'
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(moranbah.[value] as float) = 2 THEN 'Towards TG'
		ELSE TRY_CAST(ROUND(moranbah.[value],2) as nvarchar(max))
	END AS details,
	CASE 
		WHEN moranbah.holeId != '' or moranbah.holeId != '-'  THEN CONCAT(moranbah.holeId, ' - ', ss.SensorName)
		ELSE ''
	END AS sensorHoleID,
	null as SensorValueRaw
INTO 
	LatestSensorReadings
FROM 
	[moranbah-001] moranbah
	JOIN SiteSensors ss
	ON ss.site = moranbah.[site]
	AND ss.Tag = moranbah.[name]
WHERE
	ss.SensorName <> 'TARP'
UNION
SELECT
	GetMetadataPropertyValue(grasstree, 'EventId') AS LatestSensorReadingsID,
	grasstree.site as SiteName,
	ss.SensorName SensorName,
	grasstree.[name] as [TagName],
	ss.SiteSensorID as SiteSensorID,
	grasstree.holeId as holeID,
	grasstree.[timestamp] as [SensorTimestamp],
	grasstree.[value] as SensorValue,
	CASE
		WHEN ss.HigherIsUnsafe = 0 AND ss.SensorName = 'Goaf Flow' AND grasstree.[value] <= 50 THEN 0
		WHEN ss.SensorSubType = 'Methane' AND grasstree.[value] < 0 THEN 4
		WHEN ss.HigherIsUnsafe = 1 AND grasstree.[value] < ss.ThresholdValueLevel1 THEN 1
		WHEN ss.HigherIsUnsafe = 1 AND grasstree.[value] >= ss.ThresholdValueLevel1 AND grasstree.[value] <= ss.ThresholdValueLevel2 THEN 2
		WHEN ss.HigherIsUnsafe = 1 AND grasstree.[value] > ss.ThresholdValueLevel2 AND grasstree.[value] < COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 3
		WHEN ss.HigherIsUnsafe = 1 AND grasstree.[value] >= COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 4
		WHEN ss.HigherIsUnsafe = 0 AND grasstree.[value] > ss.ThresholdValueLevel1 THEN 1
		WHEN ss.HigherIsUnsafe = 0 AND grasstree.[value] <= ss.ThresholdValueLevel1 AND ss.ThresholdValueLevel2 < grasstree.[value] THEN 2
		WHEN ss.HigherIsUnsafe = 0 AND grasstree.[value] <= ss.ThresholdValueLevel2 AND COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) < grasstree.[value] THEN 3
		WHEN ss.HigherIsUnsafe = 0 AND grasstree.[value] <= COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 4
		ELSE 0
	END AS alertLevel,
	CASE
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(grasstree.[value] as float) = 0 THEN 'Stopped'
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(grasstree.[value] as float) = 1 THEN 'Towards MG'
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(grasstree.[value] as float) = 2 THEN 'Towards TG'
		ELSE TRY_CAST(ROUND(grasstree.[value],2) as nvarchar(max))
	END AS details,
	CASE 
		WHEN grasstree.holeId != '' or grasstree.holeId != '-'  THEN CONCAT(grasstree.holeId, ' - ', ss.SensorName)
		ELSE ''
	END AS sensorHoleID,
	null as SensorValueRaw
FROM 
	[grasstree-001] grasstree
	JOIN SiteSensors ss
	ON ss.site = grasstree.[site]
	AND ss.Tag = grasstree.[name]
WHERE
	ss.SensorName <> 'TARP'
UNION
SELECT
	GetMetadataPropertyValue(grosvenor, 'EventId') AS LatestSensorReadingsID,
	grosvenor.site as SiteName,
	ss.SensorName SensorName,
	grosvenor.[name] as [TagName],
	ss.SiteSensorID as SiteSensorID,
	grosvenor.holeId as holeID,
	grosvenor.[timestamp] as [SensorTimestamp],
	grosvenor.[value] as SensorValue,
	CASE
		WHEN ss.HigherIsUnsafe = 0 AND ss.SensorName = 'Goaf Flow' AND grosvenor.[value] <= 50 THEN 0
		WHEN ss.SensorSubType = 'Methane' AND grosvenor.[value] < 0 THEN 4
		WHEN ss.HigherIsUnsafe = 1 AND grosvenor.[value] < ss.ThresholdValueLevel1 THEN 1
		WHEN ss.HigherIsUnsafe = 1 AND grosvenor.[value] >= ss.ThresholdValueLevel1 AND grosvenor.[value] <= ss.ThresholdValueLevel2 THEN 2
		WHEN ss.HigherIsUnsafe = 1 AND grosvenor.[value] > ss.ThresholdValueLevel2 AND grosvenor.[value] < COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 3
		WHEN ss.HigherIsUnsafe = 1 AND grosvenor.[value] >= COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 4
		WHEN ss.HigherIsUnsafe = 0 AND grosvenor.[value] > ss.ThresholdValueLevel1 THEN 1
		WHEN ss.HigherIsUnsafe = 0 AND grosvenor.[value] <= ss.ThresholdValueLevel1 AND ss.ThresholdValueLevel2 < grosvenor.[value] THEN 2
		WHEN ss.HigherIsUnsafe = 0 AND grosvenor.[value] <= ss.ThresholdValueLevel2 AND COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) < grosvenor.[value] THEN 3
		WHEN ss.HigherIsUnsafe = 0 AND grosvenor.[value] <= COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 4
		ELSE 0
	END AS alertLevel,
	CASE
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(grosvenor.[value] as float) = 0 THEN 'Stopped'
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(grosvenor.[value] as float) = 1 THEN 'Towards MG'
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(grosvenor.[value] as float) = 2 THEN 'Towards TG'
		ELSE TRY_CAST(ROUND(grosvenor.[value],2) as nvarchar(max))
	END AS details,
	CASE 
		WHEN grosvenor.holeId != '' or grosvenor.holeId != '-'  THEN CONCAT(grosvenor.holeId, ' - ', ss.SensorName)
		ELSE ''
	END AS sensorHoleID,
	null as SensorValueRaw
FROM 
	[grosvenor-001] grosvenor
	JOIN SiteSensors ss
	ON ss.site = grosvenor.[site]
	AND ss.Tag = grosvenor.[name]
WHERE
	ss.SensorName <> 'TARP'
UNION
SELECT
	GetMetadataPropertyValue(eh, 'EventId') AS LatestSensorReadingsID,
	LOWER(eh.site) as SiteName,
	ss.SensorName SensorName,
	eh.[name] as [TagName],
	ss.SiteSensorID as SiteSensorID,
	eh.holeId as holeID,
	eh.[timestamp] as [SensorTimestamp],
    TRY_CAST(eh.[value] as float) as SensorValue,
	CASE
		WHEN ss.HigherIsUnsafe = 0 AND ss.SensorName = 'Goaf Flow' AND TRY_CAST(eh.[value] as float) <= 50 THEN 0
		WHEN ss.SensorSubType = 'Methane' AND TRY_CAST(eh.[value] as float) < 0 THEN 4
		WHEN ss.HigherIsUnsafe = 1 AND TRY_CAST(eh.[value] as float) < ss.ThresholdValueLevel1 THEN 1
		WHEN ss.HigherIsUnsafe = 1 AND TRY_CAST(eh.[value] as float) >= ss.ThresholdValueLevel1 AND TRY_CAST(eh.[value] as float) <= ss.ThresholdValueLevel2 THEN 2
		WHEN ss.HigherIsUnsafe = 1 AND TRY_CAST(eh.[value] as float) > ss.ThresholdValueLevel2 AND TRY_CAST(eh.[value] as float) < COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 3
		WHEN ss.HigherIsUnsafe = 1 AND TRY_CAST(eh.[value] as float) >= COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 4
		WHEN ss.HigherIsUnsafe = 0 AND TRY_CAST(eh.[value] as float) > ss.ThresholdValueLevel1 THEN 1
		WHEN ss.HigherIsUnsafe = 0 AND TRY_CAST(eh.[value] as float) <= ss.ThresholdValueLevel1 AND ss.ThresholdValueLevel2 < TRY_CAST(eh.[value] as float) THEN 2
		WHEN ss.HigherIsUnsafe = 0 AND TRY_CAST(eh.[value] as float) <= ss.ThresholdValueLevel2 AND COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) < TRY_CAST(eh.[value] as float) THEN 3
		WHEN ss.HigherIsUnsafe = 0 AND TRY_CAST(eh.[value] as float) <= COALESCE(ss.ThresholdValueLevel3, ss.ThresholdValueLevel2) THEN 4
		ELSE 0
	END AS alertLevel,
	CASE
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(eh.[value] as float) = 0 THEN 'Stopped'
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(eh.[value] as float) = 1 THEN 'Towards MG'
		WHEN ss.SensorName = 'Shearer Direction' AND TRY_CAST(eh.[value] as float) = 2 THEN 'Towards TG'
		ELSE TRY_CAST(ROUND(TRY_CAST(eh.[value] as float),2) as nvarchar(max))
	END AS details,
	CASE 
		WHEN eh.holeId != '' or eh.holeId != '-'  THEN CONCAT(eh.holeId, ' - ', ss.SensorName)
		ELSE ''
	END AS sensorHoleID,
	eh.[value] as SensorValueRaw
FROM 
	[cdp-eventhub-001] eh
INNER JOIN SiteSensorsWithPaths ss ON LOWER(ss.site) = LOWER(eh.[site]) AND ss.SensorPath = eh.[name]
WHERE TRY_CAST(COALESCE(eh.isValueGood, 1) as bit) = 1

SELECT  
	GetMetadataPropertyValue([moranbah-001], 'EventId') AS DataID,
	ss.[site] AS SiteName,
	[moranbah-001].name AS TagName,
	[moranbah-001].[holeID],
	ss.SensorName,
	MAX([moranbah-001].[value]) AS GasValue,
	DATETIMEFROMPARTS(
		DATEPART(year,[moranbah-001].[timestamp]), 
		DATEPART(month,[moranbah-001].[timestamp]),
		DATEPART(day,[moranbah-001].[timestamp]),
		DATEPART(hour,[moranbah-001].[timestamp]),
		(DATEPART(minute,[moranbah-001].[timestamp])/3)*3, 0, 0
	) as BinnedTimeStamp,
	ss.SiteSensorId
INTO 
	[HistoryReadings]
FROM
	[moranbah-001]
	JOIN SiteSensors ss
	ON ss.site = [moranbah-001].[site] AND ss.Tag = [moranbah-001].[name]
GROUP BY
	TUMBLINGWINDOW(MINUTE,3),
	GetMetadataPropertyValue([moranbah-001], 'EventId'),
	ss.[site],
	ss.SensorSubtype,
	[moranbah-001].holeID,
	[moranbah-001].[timestamp],
	ss.SensorName,ss.TagLocation,
	[moranbah-001].[name],
	ss.SiteSensorId
UNION
SELECT  
	GetMetadataPropertyValue([grasstree-001], 'EventId') AS DataID,
	ss.[site] AS SiteName,
	[grasstree-001].name AS TagName,
	[grasstree-001].[holeID],
	ss.SensorName,
	MAX([grasstree-001].[value]) AS GasValue,
	DATETIMEFROMPARTS(
		DATEPART(year,[grasstree-001].[timestamp]), 
		DATEPART(month,[grasstree-001].[timestamp]),
		DATEPART(day,[grasstree-001].[timestamp]),
		DATEPART(hour,[grasstree-001].[timestamp]),
		(DATEPART(minute,[grasstree-001].[timestamp])/3)*3, 0, 0
	) as BinnedTimeStamp,
	ss.SiteSensorId
FROM
	[grasstree-001]
	JOIN SiteSensors ss
	ON ss.site = [grasstree-001].[site] AND ss.Tag = [grasstree-001].[name]
GROUP BY
	TUMBLINGWINDOW(MINUTE,3),
	GetMetadataPropertyValue([grasstree-001], 'EventId'),
	ss.[site],
	ss.SensorSubtype,
	[grasstree-001].holeID,
	[grasstree-001].[timestamp],
	ss.SensorName,ss.TagLocation,
	[grasstree-001].[name],
	ss.SiteSensorId
UNION
SELECT  
	GetMetadataPropertyValue([grosvenor-001], 'EventId') AS DataID,
	ss.[site] AS SiteName,
	[grosvenor-001].name AS TagName,
	[grosvenor-001].[holeID],
	ss.SensorName,
	MAX([grosvenor-001].[value]) AS GasValue,
	DATETIMEFROMPARTS(
		DATEPART(year,[grosvenor-001].[timestamp]), 
		DATEPART(month,[grosvenor-001].[timestamp]),
		DATEPART(day,[grosvenor-001].[timestamp]),
		DATEPART(hour,[grosvenor-001].[timestamp]),
		(DATEPART(minute,[grosvenor-001].[timestamp])/3)*3, 0, 0
	) as BinnedTimeStamp,
	ss.SiteSensorId
FROM
	[grosvenor-001]
	JOIN SiteSensors ss
	ON ss.site = [grosvenor-001].[site] AND ss.Tag = [grosvenor-001].[name]
GROUP BY
	TUMBLINGWINDOW(MINUTE,3),
	GetMetadataPropertyValue([grosvenor-001], 'EventId'),
	ss.[site],
	ss.SensorSubtype,
	[grosvenor-001].holeID,
	[grosvenor-001].[timestamp],
	ss.SensorName,ss.TagLocation,
	[grosvenor-001].[name],
	ss.SiteSensorId
UNION
SELECT  
	GetMetadataPropertyValue(eh, 'EventId') AS DataID,
	LOWER(ss.[site]) AS SiteName,
	eh.name AS TagName,
	eh.[holeID],
	ss.SensorName,
	MAX(TRY_CAST(eh.[value] as float)) AS GasValue,
	DATETIMEFROMPARTS(
		DATEPART(year,eh.[timestamp]), 
		DATEPART(month,eh.[timestamp]),
		DATEPART(day,eh.[timestamp]),
		DATEPART(hour,eh.[timestamp]),
		(DATEPART(minute,eh.[timestamp])/3)*3, 0, 0
	) as BinnedTimeStamp,
	ss.SiteSensorId
FROM
	[cdp-eventhub-001] eh
INNER JOIN SiteSensorsWithPaths ss ON LOWER(ss.site) = LOWER([eh].[site]) AND ss.SensorPath = [eh].[name]
WHERE TRY_CAST(COALESCE(eh.isValueGood, 1) as bit) = 1
GROUP BY
	TUMBLINGWINDOW(MINUTE,3),
	GetMetadataPropertyValue(eh, 'EventId'),
	LOWER(ss.[site]),
	ss.SensorSubtype,
	eh.holeID,
	eh.[timestamp],
	ss.SensorName,
    ss.TagLocation,
	eh.[name],
	ss.SiteSensorId