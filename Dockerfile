FROM node:21-alpine as base

ENV HOME /home/node
ENV LC_ALL en_US.UTF-8
ENV NODE_ENV production
ARG GITLAB_TOKEN

RUN npm i -g npm@10.5

RUN npm config set @docebo:registry https://gitlab.com/api/v4/packages/npm/
RUN npm config set '//gitlab.com/api/v4/packages/npm/:_authToken' $GITLAB_TOKEN

FROM base as build

# Work dir
WORKDIR $HOME

# Install all dependencies
COPY package*.json ./
RUN npm i --include=dev

# Transpile
COPY tsconfig.json .
COPY src ./src
COPY types ./types
RUN npm run build

FROM base as final

WORKDIR $HOME

# Install only dependencies required at runtime
COPY package*.json ./
RUN npm i --omit=dev

# Copy the transpiled files
COPY --from=build $HOME/build ./src/

# Cleanup and make js files available to the "node" user
RUN rm .npmrc
RUN rm -Rf .npm
RUN chown -R node:node .

# Run the application
USER node
EXPOSE 3000
CMD ["node", "src/app.js"]
