FROM php:8.1-cli-trixie

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ARG SNOWFLAKE_ODBC_VERSION=3.10.0
ARG SNOWFLAKE_GPG_KEY_ID=2A3149C82551A34A
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR /code/

COPY docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        locales \
        unzip \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        gpg \
        debsig-verify \
        libicu-dev \
        gnupg \
        curl \
        python3 \
        python3-pip \
        python3-setuptools \
	&& rm -r /var/lib/apt/lists/* \
	&& sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
	&& locale-gen \
	&& chmod +x /tmp/composer-install.sh \
	&& /tmp/composer-install.sh

RUN docker-php-ext-configure intl \
    && docker-php-ext-install intl

# Install PHP odbc extension
# https://github.com/docker-library/php/issues/103
RUN set -x \
    && docker-php-source extract \
    && cd /usr/src/php/ext/odbc \
    && phpize \
    && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
    && ./configure --with-unixODBC=shared,/usr \
    && docker-php-ext-install odbc \
    && docker-php-source delete

#snoflake download + verify package
COPY docker/drivers/snowflake-odbc-policy.pol /etc/debsig/policies/$SNOWFLAKE_GPG_KEY_ID/generic.pol
COPY docker/drivers/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# check installed driver
RUN mkdir -p ~/.gnupg \
    && chmod 700 ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && mkdir -p /etc/gnupg \
    && echo "allow-weak-digest-algos" >> /etc/gnupg/gpg.conf \
    && mkdir -p /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY_ID \
    && gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys $SNOWFLAKE_GPG_KEY_ID \
    && gpg --export $SNOWFLAKE_GPG_KEY_ID > /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY_ID/debsig.gpg \
    && curl https://sfc-repo.snowflakecomputing.com/odbc/linux/$SNOWFLAKE_ODBC_VERSION/snowflake-odbc-$SNOWFLAKE_ODBC_VERSION.x86_64.deb --output /tmp/snowflake-odbc.deb \
    && debsig-verify /tmp/snowflake-odbc.deb \
    && gpg --batch --delete-key --yes $SNOWFLAKE_GPG_KEY_ID \
    && dpkg -i /tmp/snowflake-odbc.deb \
    && rm /tmp/snowflake-odbc.deb

# Snowflake ODBC
RUN cat <<EOF > /etc/odbcinst.ini
[ODBC Drivers]
SnowflakeDSIIDriver=Installed

[SnowflakeDSIIDriver]
APILevel=1
ConnectFunctions=YYY
Description=Snowflake DSII
Driver=/usr/lib/snowflake/odbc/lib/libSnowflake.so
DriverODBCVer=$SNOWFLAKE_ODBC_VERSION
SQLLevel=1
EOF

# Install data-diff
RUN pip3 install data-diff --break-system-packages --no-deps
RUN pip3 install 'data-diff[snowflake]' --break-system-packages --no-deps

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader

# Copy rest of the app
COPY . /code/

# Run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["php", "/code/src/run.php"]
