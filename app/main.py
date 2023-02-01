import os
import time
import requests
import psycopg2
from splitstream import splitfile
from io import BytesIO
import json
from zipfile import ZipFile

EXPLORER_URL = os.getenv("EXPLORER_URL", "https://ergo-explorer.anetabtc.io/")
SPECTRUM_ERGO_TREE = os.getenv("SPECTRUM_ERGO_TREE", "1999030f0400040204020404040405feffffffffffffffff0105feffffffffffffffff01050004d00f040004000406050005000580dac409d819d601b2a5730000d602e4c6a70404d603db63087201d604db6308a7d605b27203730100d606b27204730200d607b27203730300d608b27204730400d6099973058c720602d60a999973068c7205027209d60bc17201d60cc1a7d60d99720b720cd60e91720d7307d60f8c720802d6107e720f06d6117e720d06d612998c720702720fd6137e720c06d6147308d6157e721206d6167e720a06d6177e720906d6189c72117217d6199c72157217d1ededededededed93c27201c2a793e4c672010404720293b27203730900b27204730a00938c7205018c720601938c7207018c72080193b17203730b9593720a730c95720e929c9c721072117e7202069c7ef07212069a9c72137e7214067e9c720d7e72020506929c9c721372157e7202069c7ef0720d069a9c72107e7214067e9c72127e7202050695ed720e917212730d907216a19d721872139d72197210ed9272189c721672139272199c7216721091720b730e")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "test")
ERGO_NODE = os.getenv("ERGO_NODE", "http://192.168.1.137:9053")

connection = psycopg2.connect(user=POSTGRES_USER,
                                  password=POSTGRES_PASSWORD,
                                  host=POSTGRES_HOST,
                                  port=POSTGRES_PORT,
                                  database=POSTGRES_DB)

latestOffset = None

limit = 500

syncing = True

blockHeaders = {}
timestamps = {}

def initDb():
    initializeQuery = f"""
    CREATE TABLE IF NOT EXISTS public.spectrum_boxes
    (
        box_id character varying(64) NOT NULL,
        value bigint,
        "tokenName" character varying(100),
        "poolId" character varying(64),
        "tokenAmount" bigint,
        "tokenDecimals" smallint,
        "timestamp" timestamp without time zone,
        global_index bigint,
        PRIMARY KEY (box_id)
    );

    ALTER TABLE IF EXISTS public.spectrum_boxes
        OWNER to {POSTGRES_USER};

    CREATE OR REPLACE FUNCTION public.getohlcv(
        pool_id character varying,
        time_resolution interval,
        from_time timestamp with time zone,
        to_time timestamp with time zone,
        flipped boolean DEFAULT false)
        RETURNS TABLE(open double precision, high double precision, low double precision, close double precision, volume double precision, "time" integer) 
        LANGUAGE 'sql'
        COST 100
        VOLATILE PARALLEL UNSAFE
        ROWS 1000

    AS $BODY$
    with cte as (
        SELECT 
            CAST("value" AS DOUBLE PRECISION)/1000000000 as "value", 
            LAG(CAST("value" AS DOUBLE PRECISION)/1000000000,1) OVER(ORDER BY "timestamp", "global_index") AS "previousValue", 
            CAST("tokenAmount" AS DOUBLE PRECISION)/(10^"tokenDecimals") as "tokenAmount", 
            LAG(CAST("tokenAmount" AS DOUBLE PRECISION)/(10^"tokenDecimals"),1) OVER(ORDER BY "timestamp", "global_index") AS "previousTokenAmount", 
            to_timestamp(
                floor(EXTRACT(epoch FROM "timestamp") / EXTRACT(epoch FROM time_resolution))
                * EXTRACT(epoch FROM time_resolution)) as "time", 
            "global_index"
        FROM public.spectrum_boxes
        where "poolId" = pool_id
        and "timestamp" >= from_time
        and "timestamp" < to_time
    ) ,
    cte2 as (select first_value(case when flipped then "previousValue"/"previousTokenAmount" else "previousTokenAmount"/"previousValue" end) over (partition by "time" order by "global_index") as "open", 
        max(case when flipped then greatest("value"/"tokenAmount","previousValue"/"previousTokenAmount") else greatest("tokenAmount"/"value","previousTokenAmount"/"previousValue") end) over (partition by "time") as "high", 
        min(case when flipped then least("value"/"tokenAmount","previousValue"/"previousTokenAmount") else least("tokenAmount"/"value","previousTokenAmount"/"previousValue") end) over (partition by "time") as "low", 
        first_value(case when flipped then "value"/"tokenAmount" else "tokenAmount"/"value" end) over (partition by "time" order by "global_index" desc) as "close", 
        sum(abs("previousValue"-"value")) over (partition by "time") as "volume",
        "time"
            from cte
    where (("tokenAmount" > "previousTokenAmount" and "value" < "previousValue") or
            ("tokenAmount" < "previousTokenAmount" and "value" > "previousValue"))
    )
    select max("open") , max("high"), max("low"), max("close"), max("volume"), floor(extract(epoch from ts.dd))
    from (select dd from generate_series
            ( from_time 
            , to_time
            , time_resolution) dd) as ts left join cte2 on ts.dd = cte2."time"
    group by ts.dd
    order by ts.dd;
    $BODY$;

    ALTER FUNCTION public.getohlcv(character varying, interval, timestamp with time zone, timestamp with time zone, boolean)
    OWNER TO {POSTGRES_USER};

    CREATE MATERIALIZED VIEW IF NOT EXISTS public.pool_tvl_mat
    TABLESPACE pg_default
    AS
    WITH cte AS (
            SELECT spectrum_boxes."poolId",
                first_value(spectrum_boxes.value) OVER (PARTITION BY spectrum_boxes."poolId" ORDER BY spectrum_boxes.global_index DESC) AS value,
                spectrum_boxes."tokenName"
            FROM spectrum_boxes
            )
    SELECT cte."poolId",
        max(cte.value) AS value,
        cte."tokenName"
    FROM cte
    GROUP BY cte."poolId", cte."tokenName"
    WITH DATA;

    ALTER TABLE IF EXISTS public.pool_tvl_mat
        OWNER TO {POSTGRES_USER};
    """

    cursor = connection.cursor()
    cursor.execute(initializeQuery)
    connection.commit()
    cursor.close()

def getLatestOffset():
    getLatestOffsetQuery = f"""
    SELECT max(global_index) as count FROM public.spectrum_boxes
    """

    cursor = connection.cursor()
    cursor.execute(getLatestOffsetQuery)
    count = cursor.fetchone()[0]
    cursor.close()
    if count is None:
        with ZipFile('app/bootstrap/spectrum_boxes.zip', 'r') as zip:
            with zip.open('spectrum_boxes') as unzipped:

                cursor = connection.cursor()
                cursor.copy_from(unzipped, "spectrum_boxes", sep=",")
                cursor.close()
                connection.commit()
    return count

initDb()

while True:
    if latestOffset is None:
        latestOffset = getLatestOffset()
    res = requests.get(f'{EXPLORER_URL}/api/v1/boxes/byGlobalIndex/stream?minGix={latestOffset}&limit={limit}')
    if res.ok:
        boxFound = 0
        spectrumBoxFound = False
        for rawBox in splitfile(BytesIO(res.content),format="json"):
            item = json.loads(rawBox)
            boxFound += 1
            if item['ergoTree'] == SPECTRUM_ERGO_TREE:
                settlementHeight = item['settlementHeight']
                if settlementHeight not in blockHeaders.keys():
                    blockHeaderRes = requests.get(f'{ERGO_NODE}/blocks/at/{settlementHeight}')
                    if blockHeaderRes.ok:
                        blockHeaders[settlementHeight] = blockHeaderRes.json()[0]
                        blockInfoRes = requests.get(f'{ERGO_NODE}/blocks/{blockHeaders[settlementHeight]}/header')
                        if blockInfoRes.ok:
                            timestamps[blockHeaders[settlementHeight]] = blockInfoRes.json()['timestamp']
                timestamp = timestamps[blockHeaders[settlementHeight]]
                if len(item['assets']) > 2:
                    insertQuery = f"""
                        INSERT INTO public.spectrum_boxes 
                        (box_id, value, "tokenName", "poolId", "tokenAmount", "tokenDecimals", timestamp, global_index) 
                        VALUES
                        ('{item['boxId']}',
                        {item['value']},
                        '{item['assets'][2]['name']}',
                        '{item['assets'][0]['tokenId']}',
                        {item['assets'][2]['amount']},
                        {item['assets'][2]['decimals']},
                        to_timestamp({timestamp}/1000),
                        {item['globalIndex']}
                        ) ON CONFLICT ON CONSTRAINT spectrum_boxes_pkey DO NOTHING
                    """

                    cursor = connection.cursor()
                    cursor.execute(insertQuery)
                    cursor.close()

                    spectrumBoxFound = True
        if boxFound == limit:
            syncing = True
        else:
            syncing = False
        if not syncing and spectrumBoxFound:
            cursor = connection.cursor()
            cursor.execute("REFRESH MATERIALIZED VIEW public.pool_tvl_mat WITH DATA;")
            cursor.close()
        latestOffset += boxFound
        print(latestOffset)
    connection.commit()
    if syncing:
        time.sleep(0.5)
    else:
        time.sleep(30)