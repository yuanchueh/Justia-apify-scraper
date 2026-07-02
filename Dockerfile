FROM apify/actor-node:20

# NOTE: do NOT pass --omit=optional. impit (browser-TLS HTTP client) ships its
# native binding as a platform-specific optionalDependency (impit-linux-x64-gnu);
# omitting optional deps drops it and the actor fails at runtime with
# "cannot find native binding".
COPY package*.json ./
RUN npm install --omit=dev

COPY . ./
CMD npm start
