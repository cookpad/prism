###################################################################################
FROM sorah/ruby:3.0-dev AS build

RUN apt-get update && apt-get install -y libpq-dev
RUN gem install bundler:2.2.27

WORKDIR /app

COPY ./Gemfile .
COPY ./Gemfile.lock .
RUN bundle config set --local deployment true && \
    bundle config set --local without 'development test' && \
    bundle install -j4

###################################################################################
FROM sorah/ruby:3.0

RUN apt-get update && apt-get install -y libpq5
RUN gem install bundler:2.2.27

WORKDIR /app

COPY --from=build /app/ .
RUN bundle check

COPY . .
