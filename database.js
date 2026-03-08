const { createClient } = require("redis");

const redisUrl = typeof process.env.REDIS_URL === "string"
    ? process.env.REDIS_URL.trim()
    : "";

let client = null;
let cacheEnabled = false;
let cacheDisabled = false;
let failureLogged = false;

function logCacheDisabled(err) {
    if (failureLogged) return;
    failureLogged = true;

    const reason = err && (err.code || err.message)
        ? `${err.code || err.message}`
        : "unknown error";

    console.warn(`[Cache] Redis unavailable. Continuing without cache (${reason}).`);
}

function closeClient() {
    if (!client) return;

    try {
        client.removeAllListeners();
    } catch (err) {
        // Ignore cleanup errors.
    }

    try {
        if (typeof client.destroy === "function") {
            client.destroy();
        } else if (typeof client.disconnect === "function") {
            client.disconnect();
        }
    } catch (err) {
        // Ignore cleanup errors.
    }

    client = null;
}

async function initializeCache() {
    if (!redisUrl || cacheDisabled || client) return;

    client = createClient({
        url: redisUrl,
        socket: {
            reconnectStrategy: false,
            connectTimeout: 1500
        }
    });

    client.on("error", (err) => {
        if (!cacheEnabled) return;

        cacheEnabled = false;
        cacheDisabled = true;
        logCacheDisabled(err);
        closeClient();
    });

    try {
        await client.connect();
        cacheEnabled = client.isReady;

        if (cacheEnabled) {
            console.log(`[Cache] Redis enabled (${redisUrl})`);
        }
    } catch (err) {
        cacheEnabled = false;
        cacheDisabled = true;
        logCacheDisabled(err);
        closeClient();
    }
}

void initializeCache();

async function get(key) {
    if (!cacheEnabled || !client || !client.isReady) return null;

    try {
        const value = await client.get(key);
        if (!value) return null;

        try {
            return JSON.parse(value);
        } catch (err) {
            return value;
        }
    } catch (err) {
        cacheEnabled = false;
        cacheDisabled = true;
        logCacheDisabled(err);
        closeClient();
        return null;
    }
}

async function set(key, value, ttlSeconds = 86400) {
    if (!cacheEnabled || !client || !client.isReady) return;

    try {
        await client.set(key, JSON.stringify(value), {
            EX: ttlSeconds
        });
    } catch (err) {
        cacheEnabled = false;
        cacheDisabled = true;
        logCacheDisabled(err);
        closeClient();
    }
}

module.exports = { get, set };
