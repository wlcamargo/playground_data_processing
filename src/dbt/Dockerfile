FROM wlcamargo/dbt-trino-base

COPY ./jaffle_shop_delta/ /usr/app/

COPY ./profiles.yml /root/.dbt/profiles.yml

CMD [ "run" ]

