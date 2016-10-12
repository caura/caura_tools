#!/bin/bash

function au_psql {
    psql -E -h <DATABASE>.rds.amazonaws.com -U master -d <USERNAME> -p <PASSWORD>
}


# create tables
#
  au_psql -c <<EOF
  DROP TABLE IF EXISTS public.facebook;
  CREATE TABLE public.facebook (
    ad_set_id bigint NOT NULL,
    ad_set_name character varying(255),
    results integer,
    result_type character varying(60),
    reach integer,
    cost_per_result double precision DEFAULT 0,
    amount_spent double precision DEFAULT 0,
    ends date,
    starts date,
    costs_per_all_actions double precision DEFAULT 0,
    frequency double precision DEFAULT 0,
    impressions integer DEFAULT 0,
    actions integer DEFAULT 0,
    people_taking_action integer DEFAULT 0,
    page_likes integer DEFAULT 0,
    link_clicks integer DEFAULT 0,
    website_actions integer DEFAULT 0,
    page_engagement integer DEFAULT 0,
    photo_views integer DEFAULT 0,
    post_shares integer DEFAULT 0,
    post_comments integer DEFAULT 0,
    post_engagement integer DEFAULT 0,
    post_reactions integer DEFAULT 0,
    unique_clicks_to_link integer DEFAULT 0,
    cost_per_website_action double precision DEFAULT 0,
    clicks integer DEFAULT 0,
    social_clicks integer DEFAULT 0,
    unique_clicks integer DEFAULT 0,
    unique_social_clicks integer DEFAULT 0
  );
  \copy public.facebook from '~/caura_tools/data/facebook.csv' CSV DELIMITER ',' HEADER;
EOF
  au_psql -c <<EOF
  DROP TABLE IF EXISTS public.heap;
  CREATE TABLE public.heap (
    user_id bigint NOT NULL
    ,initial_browser character varying(60)
    ,initial_city character varying(60)
    ,initial_country character varying(60)
    ,initial_device_type character varying(20)
    ,initial_landing_page character varying(120)
    ,initial_platform character varying(60)
    ,initial_referrer text
    ,initial_region character varying(60)
    ,initial_utm_campaign character varying(60)
    ,initial_utm_content character varying(60)
    ,initial_utm_medium character varying(60)
    ,initial_utm_source character varying(60)
    ,initial_utm_term character varying(60)
    ,joindate timestamp without time zone
    ,lastseen timestamp without time zone
  );
  \copy public.heap from '~/caura_tools/data/heap.csv' CSV DELIMITER ',' HEADER;
EOF
#
  au_psql -c <<EOF
  DROP TABLE IF EXISTS public.yandex;
  CREATE TABLE public.yandex (
    traffic_sources character varying(60)
    , landing_page_path  character varying(255)
    , time_visit timestamp without time zone
    , area character varying(60)
    , referrer text
    , utm_source character varying(60)
    , utm_medium character varying(60)
    , seesions integer
    , users integer
    , page_depth double precision DEFAULT 0
    , page_views integer
    , visit_duration character varying(60)
  );
  \copy public.yandex from '~/caura_tools/data/yandex.csv' CSV DELIMITER ',' HEADER;
EOF
