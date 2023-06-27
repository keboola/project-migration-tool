FROM php:8-cli

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ARG SNOWFLAKE_ODBC_VERSION=2.25.6
ARG SNOWFLAKE_ODBC_GPG_KEY=630D9F3CAB551AF3
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
        python3 \
        python3-pip \
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
COPY docker/drivers/snowflake-odbc-policy.pol /etc/debsig/policies/$SNOWFLAKE_ODBC_GPG_KEY/generic.pol
COPY docker/drivers/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# check installed driver
RUN mkdir -p ~/.gnupg \
    && chmod 700 ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && mkdir /usr/share/debsig/keyrings/$SNOWFLAKE_ODBC_GPG_KEY \
    && if ! gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys $SNOWFLAKE_ODBC_GPG_KEY; then \
          gpg --keyserver hkp://keys.gnupg.net --recv-keys $SNOWFLAKE_ODBC_GPG_KEY; \
      fi \
    && gpg --export $SNOWFLAKE_ODBC_GPG_KEY > snowflakeKey.asc \
    && touch /usr/share/debsig/keyrings/$SNOWFLAKE_ODBC_GPG_KEY/debsig.gpg \
    && gpg --no-default-keyring --keyring /usr/share/debsig/keyrings/$SNOWFLAKE_ODBC_GPG_KEY/debsig.gpg --import snowflakeKey.asc \
    && curl https://sfc-repo.snowflakecomputing.com/odbc/linux/$SNOWFLAKE_ODBC_VERSION/snowflake-odbc-$SNOWFLAKE_ODBC_VERSION.x86_64.deb --output /tmp/snowflake-odbc.deb \
    && debsig-verify -v /tmp/snowflake-odbc.deb \
    && gpg --batch --delete-key --yes $SNOWFLAKE_ODBC_GPG_KEY \
    && dpkg -i /tmp/snowflake-odbc.deb

# INstall data-diff
RUN pip install data-diff
RUN pip install 'data-diff[snowflake]'

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
