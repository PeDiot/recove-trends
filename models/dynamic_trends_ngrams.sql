{{
    config(
        materialized="table",
        alias="trends_" ~ var("ngrams_n") ~ "grams_" ~ var("lookback_hours") ~ "h_top" ~ var("top_k"),
    )
}}

with
    rawsearches as (
        select
            created_at,
            user_id,
            item_id,
            text,
            pin_image_url as search_image_url,
            false as is_image_area
        from {{ source("recove_backup", "search_history") }}
        where
            user_id is not null
            and user_id not in (
                select user_id from {{ source("recove_backup", "excluded_user_ids") }}
            )
            and created_at >= timestamp_sub(
                current_timestamp(), interval {{ var("lookback_hours") }} hour
            )

        union all

        select
            created_at,
            user_id,
            item_id,
            text,
            original_image_url as search_image_url,
            true as is_image_area
        from {{ source("recove_backup", "search_area_history") }}
        where
            user_id is not null
            and user_id not in (
                select user_id from {{ source("recove_backup", "excluded_user_ids") }}
            )
            and created_at >= timestamp_sub(
                current_timestamp(), interval {{ var("lookback_hours") }} hour
            )
    ),

    searchqueries as (
        select
            concat(
                ifnull(user_id, ''),
                '|',
                ifnull(search_image_url, ''),
                '|',
                lower(trim(text))
            ) as id,
            user_id,
            item_id,
            created_at,
            lower(trim(text)) as text,
            is_image_area
        from rawsearches
        where text is not null and trim(text) != ''
    ),

    clicks as (
        select user_id, item_id, min(created_at) as created_at
        from {{ source("recove_backup", "click_out") }}
        where
            user_id not in (
                select user_id from {{ source("recove_backup", "excluded_user_ids") }}
            )
            and created_at >= timestamp_sub(
                current_timestamp(),
                interval {{ var("attribution_lookback_hours") }} hour
            )
        group by 1, 2
    ),

    saves as (
        select user_id, item_id, min(created_at) as created_at
        from {{ source("recove_backup", "saved") }}
        where
            user_id not in (
                select user_id from {{ source("recove_backup", "excluded_user_ids") }}
            )
            and created_at >= timestamp_sub(
                current_timestamp(),
                interval {{ var("attribution_lookback_hours") }} hour
            )
        group by 1, 2
    ),

    groupedsearchevents as (
        select
            sq.id,
            any_value(sq.user_id) as user_id,
            any_value(sq.text) as text,
            any_value(sq.is_image_area) as is_image_area,
            count(c.item_id) as num_click_out_events,
            count(s.item_id) as num_save_events
        from searchqueries sq
        left join
            clicks c
            on sq.user_id = c.user_id
            and sq.item_id = c.item_id
            and c.created_at >= sq.created_at
        left join
            saves s
            on sq.user_id = s.user_id
            and sq.item_id = s.item_id
            and s.created_at >= sq.created_at
        group by sq.id
    ),

    convertedevents as (
        select
            id,
            user_id,
            is_image_area,
            num_click_out_events,
            num_save_events,
            (
                select array_agg(token)
                from unnest(regexp_extract_all(text, r'[a-z]+')) as token
                where
                    length(token) > {{ var("minimum_token_length") }}
                    and token not in unnest({{ var("stop_words") }})
            ) as clean_token_array
        from groupedsearchevents
    ),

    extractedngrams as (
        select
            id,
            user_id,
            is_image_area,
            num_click_out_events,
            num_save_events,
            array_to_string(
                array(
                    select token
                    from unnest(clean_token_array) as token
                    with
                    offset as idx
                    where idx >= pos and idx < pos + {{ var("ngrams_n") }}
                    order by idx
                ),
                ' '
            ) as ngram
        from
            convertedevents,
            unnest(
                generate_array(
                    0,
                    greatest(array_length(clean_token_array) - {{ var("ngrams_n") }}, 0)
                )
            ) as pos
        where array_length(clean_token_array) >= {{ var("ngrams_n") }}
    ),

    aggregatedstats as (
        select
            ngram,
            count(id) as num_search_query_events,
            sum(num_click_out_events) as num_click_out_events,
            sum(num_save_events) as num_save_events,
            count(
                distinct if(
                    num_click_out_events > 0 or num_save_events > 0, user_id, null
                )
            ) as num_converting_users,

            sum(
                (
                    (num_click_out_events * {{ var("click_out_multiplier") }})
                    + (num_save_events * {{ var("saved_item_multiplier") }})
                )
                * if(is_image_area, {{ var("image_area_multiplier") }}, 1)
            ) * count(
                distinct if(
                    num_click_out_events > 0 or num_save_events > 0, user_id, null
                )
            ) as engagement_score

        from extractedngrams
        group by ngram
    )

select
    ngram,
    num_search_query_events,
    num_click_out_events,
    num_save_events,
    num_converting_users,
    engagement_score
from aggregatedstats
order by engagement_score desc
limit {{ var("top_k") }}