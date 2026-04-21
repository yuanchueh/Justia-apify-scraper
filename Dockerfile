FROM apify/actor-node:18

COPY package*.json ./
RUN npm install --omit=dev --omit=optional

COPY . ./
CMD npm start
