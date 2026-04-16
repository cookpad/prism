###################################################################################
FROM public.ecr.aws/sorah/ruby:4.0-dev AS build

RUN apt-get update && apt-get install -y libpq-dev

WORKDIR /app

COPY ./Gemfile ./Gemfile.lock ./
RUN bundle config set --local deployment true && \
    bundle config set --local without 'development test' && \
    bundle install -j4

###################################################################################
FROM public.ecr.aws/sorah/ruby:4.0

RUN apt-get update && apt-get install -y libpq5

WORKDIR /app

COPY --from=build /app/ .
RUN bundle check

COPY . .
