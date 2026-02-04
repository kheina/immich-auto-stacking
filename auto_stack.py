import asyncio
import os
from collections import defaultdict

from psycopg import AsyncClientCursor, AsyncConnection, AsyncCursor, Binary, OperationalError
from psycopg.sql import SQL
from psycopg_pool import AsyncConnectionPool


headers = [
	'asset.id',
	'asset.deviceAssetId',
	'asset.ownerId',
	'asset.deviceId',
	'asset.type',
	'asset.originalPath',
	'asset.fileCreatedAt',
	'asset.fileModifiedAt',
	'asset.isFavorite',
	'asset.duration',
	'asset.encodedVideoPath',
	'asset.checksum',
	'asset.livePhotoVideoId',
	'asset.updatedAt',
	'asset.createdAt',
	'asset.originalFileName',
	'asset.thumbhash',
	'asset.isOffline',
	'asset.libraryId',
	'asset.isExternal',
	'asset.deletedAt',
	'asset.localDateTime',
	'asset.stackId',
	'asset.duplicateId',
	'asset.status',
	'asset.updateId',
	'asset.visibility',
	'asset.width',
	'asset.height',
	'asset.isEdited',
	'asset_exif.make',
	'asset_exif.model',
	'asset_exif.exifImageWidth',
	'asset_exif.exifImageHeight',
	'asset_exif.fileSizeInByte',
	'asset_exif.orientation',
	'asset_exif.dateTimeOriginal',
	'asset_exif.modifyDate',
	'asset_exif.lensModel',
	'asset_exif.fNumber',
	'asset_exif.focalLength',
	'asset_exif.iso',
	'asset_exif.latitude',
	'asset_exif.longitude',
	'asset_exif.city',
	'asset_exif.state',
	'asset_exif.country',
	'asset_exif.description',
	'asset_exif.fps',
	'asset_exif.exposureTime',
	'asset_exif.livePhotoCID',
	'asset_exif.timeZone',
	'asset_exif.projectionType',
	'asset_exif.profileDescription',
	'asset_exif.colorspace',
	'asset_exif.bitsPerSample',
	'asset_exif.autoStackId',
	'asset_exif.rating',
	'asset_exif.updatedAt',
	'asset_exif.updateId',
	'asset_exif.lockedProperties',
	'asset_exif.tags',
	'asset_metadata.key',
	'asset_metadata.value',
	'asset_metadata.updateId',
	'asset_metadata.updatedAt',
]

query = """
select asset."id",
	asset."deviceAssetId",
	asset."ownerId",
	asset."deviceId",
	asset."type",
	asset."originalPath",
	asset."fileCreatedAt",
	asset."fileModifiedAt",
	asset."isFavorite",
	asset."duration",
	asset."encodedVideoPath",
	asset."checksum",
	asset."livePhotoVideoId",
	asset."updatedAt",
	asset."createdAt",
	asset."originalFileName",
	asset."thumbhash",
	asset."isOffline",
	asset."libraryId",
	asset."isExternal",
	asset."deletedAt",
	asset."localDateTime",
	asset."stackId",
	asset."duplicateId",
	asset."status",
	asset."updateId",
	asset."visibility",
	asset."width",
	asset."height",
	asset."isEdited",
	asset_exif."make",
	asset_exif."model",
	asset_exif."exifImageWidth",
	asset_exif."exifImageHeight",
	asset_exif."fileSizeInByte",
	asset_exif."orientation",
	asset_exif."dateTimeOriginal",
	asset_exif."modifyDate",
	asset_exif."lensModel",
	asset_exif."fNumber",
	asset_exif."focalLength",
	asset_exif."iso",
	asset_exif."latitude",
	asset_exif."longitude",
	asset_exif."city",
	asset_exif."state",
	asset_exif."country",
	asset_exif."description",
	asset_exif."fps",
	asset_exif."exposureTime",
	asset_exif."livePhotoCID",
	asset_exif."timeZone",
	asset_exif."projectionType",
	asset_exif."profileDescription",
	asset_exif."colorspace",
	asset_exif."bitsPerSample",
	asset_exif."autoStackId",
	asset_exif."rating",
	asset_exif."updatedAt",
	asset_exif."updateId",
	asset_exif."lockedProperties",
	asset_exif."tags",
	asset_metadata."key",
	asset_metadata."value",
	asset_metadata."updateId",
	asset_metadata."updatedAt"
from asset
left join asset_exif
	on asset.id = asset_exif."assetId"
left join asset_metadata
	on asset.id = asset_metadata."assetId"
left join asset_job_status
	on asset.Id = asset_job_status."assetId"
		and "metadataExtractedAt" is not null
where asset."updatedAt" >= 'epoch'::timestamptz
	and asset."createdAt" < now() - interval '1 hour'
	and asset."stackId" is null
order by asset."updatedAt" asc
limit 1000;
"""


async def stack(conn_str) :
	pool = AsyncConnectionPool(conn_str, open=False)
	await pool.open(wait=True, timeout=5)

	if os.path.exists('./.latest') :
		latest = open('./.latest').read().strip()
		sql = SQL(query.replace('epoch', latest))
	else :
		sql = SQL(query)

	for _ in range(3) :
		async with pool.connection() as conn :
			try :
				async with AsyncClientCursor(conn) as cur :
					await cur.execute(sql)
					got = await cur.fetchall()
					res = list(map(lambda x : dict(zip(headers, x)), got))
					break

			except OperationalError :
				pass

			except Exception as e :
				raise

	metadata = defaultdict(dict)
	for i in res :
		if not all([i.get('asset.id'), i.get('asset_metadata.key'), i.get('asset_metadata.value')]) :
			continue
		metadata[i['asset.id']][i['asset_metadata.key']] = i['asset_metadata.value']
		del i['asset_metadata.key']
		del i['asset_metadata.value']
		del i['asset_metadata.updateId']
		del i['asset_metadata.updatedAt']

	assets = { }
	for i in res :
		if not i.get('asset.id') :
			continue
		i['asset_metadata'] = metadata[i['asset.id']]
		assets[i['asset.id']] = i

	print(assets)


if __name__ == '__main__' :
	db_user = os.environ.get('DB_USERNAME')
	db_pass = os.environ.get('DB_PASSWORD')
	db_name = os.environ.get('DB_DATABASE_NAME')
	db_port = os.environ.get('DB_PORT', '5432')
	asyncio.run(stack(f'user={db_user} password={db_pass} dbname={db_name} host=127.0.0.1 port={db_port}'))
