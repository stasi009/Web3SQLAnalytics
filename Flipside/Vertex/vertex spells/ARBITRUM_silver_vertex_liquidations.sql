{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH health_groups AS (

    SELECT
        product_id,
        health_group,
        health_group_symbol
    FROM
        {{ ref('silver__vertex_dim_products') }}
    GROUP BY
        ALL
),
logs AS (
    SELECT
        *
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xf96988bb67b73fa61d64c48dc2e91ae6b697ebb8c5a496d238309aa20fbf6458',
            '0x494f937f5cc892f798248aa831acfb4ad7c4bf35edd8498c5fb431ce1e38b035'
        )
        AND contract_address = '0xae1ec28d6225dce2ff787dcb8ce11cf6d3ae064f'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
logs_pull_v1 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'Liquidation' AS event_name,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        topics [1] :: STRING AS digest,
        LEFT(
            topics [2] :: STRING,
            42
        ) AS trader,
        topics [2] :: STRING AS subaccount,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INT AS MODE,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INT AS health_group,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) :: INT AS amount_quote,
        utils.udf_hex_to_int(  segmented_data [3] :: STRING ) :: INT AS insurance_cover,
        NULL AS is_encoded_spread,
        _log_id,
        _inserted_timestamp
    FROM
        logs
    WHERE
        topics [0] :: STRING = '0xf96988bb67b73fa61d64c48dc2e91ae6b697ebb8c5a496d238309aa20fbf6458'
        AND contract_address = '0xae1ec28d6225dce2ff787dcb8ce11cf6d3ae064f'
),
logs_pull_v2 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'Liquidation' AS event_name,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(  segmented_data [0] :: STRING ) :: INT AS product_id,
        topics [1] :: STRING AS digest,
        NULL AS MODE,
        LEFT( topics [2] :: STRING,  42 ) AS trader,
        topics [2] :: STRING AS subaccount,
        utils.udf_hex_to_int(  's2c',  segmented_data [2] :: STRING ) :: INT AS amount,
        utils.udf_hex_to_int( 's2c',  segmented_data [3] :: STRING ) :: INT AS amount_quote,
        NULL AS insurance_cover,
        udf_hex_to_int(  segmented_data [1] :: STRING ) AS is_encoded_spread,
        _log_id,
        _inserted_timestamp
    FROM
        logs
    WHERE
        topics [0] :: STRING = '0x494f937f5cc892f798248aa831acfb4ad7c4bf35edd8498c5fb431ce1e38b035'
        AND contract_address = '0xae1ec28d6225dce2ff787dcb8ce11cf6d3ae064f'
),
v2_vertex_decode AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        event_index,
        is_encoded_spread,
        digest,
        MODE,
        trader,
        subaccount,
        amount,
        amount_quote,
        insurance_cover,
        CASE
            WHEN is_encoded_spread = 1 THEN utils.udf_int_to_binary(product_id)
            ELSE NULL
        END AS bin_product_ids,
        CASE
            WHEN is_encoded_spread = 1 THEN ARRAY_CONSTRUCT(
                utils.udf_binary_to_int(SUBSTR(bin_product_ids, -16)),
                utils.udf_binary_to_int(
                    SUBSTR(
                        bin_product_ids,
                        1,
                        GREATEST(len(bin_product_ids) - 16, 1))
                    )
                )
                ELSE NULL
        END AS decoded_spread_product_ids,

        CASE
            WHEN is_encoded_spread = 1 THEN decoded_spread_product_ids [0] :: STRING
            ELSE product_id
        END AS product_id,

        _log_id,
        _inserted_timestamp
    FROM
        logs_pull_v2
    ),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        digest,
        trader,
        subaccount,
        'v1' AS version,
        MODE,
        NULL AS product_id,
        l.health_group,
        p.health_group_symbol,
        amount AS amount_unadj,
        amount / pow(
            10,
            18
        ) AS amount,
        amount_quote AS amount_quote_unadj,
        amount_quote / pow(
            10,
            18
        ) AS amount_quote,
        insurance_cover AS insurance_cover_unadj,
        insurance_cover / pow(
            10,
            18
        ) AS insurance_cover,
        is_encoded_spread,
        ARRAY_CONSTRUCT(
            NULL,
            NULL
        ) AS spread_product_ids,
        _log_id,
        _inserted_timestamp
    FROM
        logs_pull_v1 l
        LEFT JOIN health_groups p
        ON l.health_group = p.health_group
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        digest,
        trader,
        subaccount,
        'v2' AS version,
        MODE,
        l.product_id,
        p.health_group,
        p.health_group_symbol,
        amount AS amount_unadj,
        amount / pow(
            10,
            18
        ) AS amount,
        amount_quote AS amount_quote_unadj,
        amount_quote / pow(
            10,
            18
        ) AS amount_quote,
        NULL AS insurance_cover_unadj,
        insurance_cover,
        CASE
            WHEN is_encoded_spread = 1 THEN TRUE
            ELSE FALSE
        END AS is_encoded_spread,
        decoded_spread_product_ids AS spread_product_ids,
        _log_id,
        _inserted_timestamp
    FROM
        v2_vertex_decode l
        LEFT JOIN health_groups p
        ON l.product_id = p.product_id
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS vertex_liquidation_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1