# Playwright + Chromium base image. The tag pins playwright 1.60.0 — the
# "playwright" version in package.json MUST match it exactly, otherwise the
# npm module looks for browser binaries of a different revision than the
# ones preinstalled in the image and the actor dies at launch.
FROM apify/actor-node-playwright-chrome:20-1.60.0

COPY --chown=myuser package*.json ./

RUN npm --quiet set progress=false \
    && npm install --omit=dev --omit=optional \
    && echo "Installed NPM packages:" \
    && (npm list --omit=dev --all || true) \
    && echo "Node.js version:" \
    && node --version \
    && echo "NPM version:" \
    && npm --version

COPY --chown=myuser . ./

CMD npm start --silent
