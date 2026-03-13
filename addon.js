const { addonBuilder, getRouter } = require("stremio-addon-sdk");
const express = require('express');
const { AsyncLocalStorage } = require('async_hooks');
const storage = new AsyncLocalStorage();
const fetch = require("node-fetch");
const path = require('path');
const cache = require('./database');

const ADDON_URL = process.env.ADDON_URL || "http://localhost:7000";
const CACHE_TTL_SECONDS = {
    metaMovie: 12 * 3600,  // 12 hours
    metaSeries: 3 * 3600,  // 3 hours
    detailsMovie: 24 * 3600, // 24 hours
    detailsSeries: 6 * 3600, // 6 hours
    providers: 12 * 3600, // 12 hours
    top10: 6 * 3600 // 6 hours
};
const NEGATIVE_CACHE_TTL_SECONDS = 5 * 60; // 5 minutes
const CACHE_TTL_JITTER_PERCENT = 0.15; // +/-15%
const NEGATIVE_CACHE_MARKER = "__negative_cache__";
const TOP10_GLOBAL_CATALOG_ID = "top10_italia";
const TOP10_MOVIE_CONFIG_ID = "top10_italia_movie";
const TOP10_SERIES_CONFIG_ID = "top10_italia_series";

function withTtlJitter(baseTtlSeconds, jitterPercent = CACHE_TTL_JITTER_PERCENT) {
    const delta = Math.floor(baseTtlSeconds * jitterPercent);
    const jitter = Math.floor((Math.random() * ((delta * 2) + 1)) - delta);
    return Math.max(60, baseTtlSeconds + jitter);
}

function createNegativeCache(reason) {
    return { [NEGATIVE_CACHE_MARKER]: true, reason, ts: Date.now() };
}

function isNegativeCache(value) {
    return !!(value && typeof value === "object" && value[NEGATIVE_CACHE_MARKER] === true);
}

async function getImdbRating(imdbId, type) {
    if (!imdbId) return null;
    try {
        const response = await fetch(`https://v3-cinemeta.strem.io/meta/${type}/${imdbId}.json`);
        const data = await response.json();
        return data && data.meta && data.meta.imdbRating ? data.meta.imdbRating : null;
    } catch (e) {
        console.warn(`Error fetching IMDb rating for ${imdbId}:`, e.message);
        return null;
    }
}

function normalizeImdbId(imdbId) {
    if (!imdbId) return null;
    const imdbStr = String(imdbId).trim();
    if (!imdbStr) return null;
    return imdbStr.toLowerCase().startsWith("tt") ? imdbStr : `tt${imdbStr}`;
}

function getPrimaryMediaId(imdbId, tmdbId) {
    const normalizedImdbId = normalizeImdbId(imdbId);
    return normalizedImdbId || `tmdb:${tmdbId}`;
}

function getRequestConfig(config = null) {
    if (config && typeof config === "object") return config;
    const store = storage.getStore();
    return store && store.config && typeof store.config === "object" ? store.config : {};
}

function shouldReturnStreams(config = null) {
    const resolvedConfig = getRequestConfig(config);
    if (typeof resolvedConfig.returnStreams === "boolean") {
        return resolvedConfig.returnStreams;
    }

    if (typeof resolvedConfig.returnStreams === "string") {
        const normalizedValue = resolvedConfig.returnStreams.trim().toLowerCase();
        if (["false", "0", "off", "no"].includes(normalizedValue)) {
            return false;
        }
    }

    return true;
}

function encodeConfigSegment(config = null) {
    const resolvedConfig = getRequestConfig(config);
    if (!resolvedConfig || Object.keys(resolvedConfig).length === 0) return null;

    return Buffer
        .from(JSON.stringify(resolvedConfig), "utf-8")
        .toString("base64")
        .replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/=+$/g, "");
}

function decodeConfigSegment(encodedConfig) {
    if (!encodedConfig) return {};

    const normalizedConfig = encodedConfig.replace(/-/g, "+").replace(/_/g, "/");
    const paddingLength = normalizedConfig.length % 4;
    const paddedConfig = paddingLength === 0
        ? normalizedConfig
        : normalizedConfig + "=".repeat(4 - paddingLength);

    return JSON.parse(Buffer.from(paddedConfig, "base64").toString("utf-8"));
}

function getManifestUrl(config = null) {
    const encodedConfig = encodeConfigSegment(config);
    return encodedConfig
        ? `${ADDON_URL}/${encodedConfig}/manifest.json`
        : `${ADDON_URL}/manifest.json`;
}

function normalizeConfiguredCatalogEntryKey(key) {
    const rawKey = String(key || "").trim();
    if (!rawKey) return "";

    const legacyMap = {
        [TOP10_MOVIE_CONFIG_ID]: TOP10_GLOBAL_CATALOG_ID,
        [TOP10_SERIES_CONFIG_ID]: TOP10_GLOBAL_CATALOG_ID,
        [LEGACY_TOP10_MOVIE_CONFIG_ID]: TOP10_GLOBAL_CATALOG_ID,
        [LEGACY_TOP10_SERIES_CONFIG_ID]: TOP10_GLOBAL_CATALOG_ID,
        anime_movie: "anime_tmdb_movie",
        anime_series: "anime_tmdb_series",
        anime_search_movie: "anime_tmdb_search_movie",
        anime_search_series: "anime_tmdb_search_series",
        anime_popular_movie: "anime_kitsu_popular_movie",
        anime_popular_series: "anime_kitsu_popular_series",
        anime_ova: "anime_kitsu_ova",
        anime_ona: "anime_kitsu_ona",
        anime_special: "anime_kitsu_special",
        sky: "now",
        sky_original: "now_original",
        sky_catalog: "now_catalog",
        sky_top10: "now_top10"
    };

    return legacyMap[rawKey] || rawKey;
}

function normalizeConfiguredCatalogNameKey(key) {
    const normalizedKey = normalizeConfiguredCatalogEntryKey(key);
    if (!normalizedKey) return "";
    if (normalizedKey.endsWith("_original")) return normalizedKey.slice(0, -9);
    if (normalizedKey.endsWith("_catalog")) return normalizedKey.slice(0, -8);
    if (normalizedKey.endsWith("_top10")) return normalizedKey.slice(0, -6);
    return normalizedKey;
}

function getConfiguredCatalogNames(config = null) {
    const resolvedConfig = getRequestConfig(config);
    const rawNames = resolvedConfig && resolvedConfig.catalogNames;
    if (!rawNames || typeof rawNames !== "object" || Array.isArray(rawNames)) return {};

    return Object.entries(rawNames).reduce((accumulator, [key, value]) => {
        const normalizedKey = normalizeConfiguredCatalogNameKey(key);
        const cleanValue = typeof value === "string" ? value.trim() : "";
        if (normalizedKey && cleanValue) {
            accumulator[normalizedKey] = cleanValue;
        }
        return accumulator;
    }, {});
}

function getConfiguredCatalogShapes(config = null) {
    const resolvedConfig = getRequestConfig(config);
    const rawShapes = resolvedConfig && resolvedConfig.catalogShapes;
    if (!rawShapes) return new Set();

    const normalized = new Set();
    const addKey = key => {
        const normalizedKey = normalizeConfiguredCatalogEntryKey(key);
        if (normalizedKey) normalized.add(normalizedKey);
    };

    if (typeof rawShapes === "string") {
        rawShapes
            .split(",")
            .map(value => value.trim())
            .filter(Boolean)
            .forEach(addKey);
        return normalized;
    }

    if (Array.isArray(rawShapes)) {
        rawShapes.forEach(addKey);
        return normalized;
    }

    if (typeof rawShapes !== "object") return normalized;

    Object.entries(rawShapes).forEach(([key, value]) => {
        if (!value) return;
        addKey(key);
    });

    return normalized;
}

function resolveConfiguredCatalogName(lookupKey, catalog, customNames = {}) {
    const rawLookupKey = String(lookupKey || "").trim();
    if (!rawLookupKey || !catalog || !customNames || typeof customNames !== "object") return "";

    const normalizedLookupKey = normalizeConfiguredCatalogNameKey(rawLookupKey);
    const baseCustomName = normalizedLookupKey ? customNames[normalizedLookupKey] : "";
    if (!baseCustomName) return "";

    if (rawLookupKey.endsWith("_original")) return `${baseCustomName} Original`;
    if (rawLookupKey.endsWith("_top10")) return `${baseCustomName} Top 10 Italia`;
    return baseCustomName;
}

function applyConfiguredCatalogName(catalog, lookupKey, customNames = {}) {
    const customName = resolveConfiguredCatalogName(lookupKey, catalog, customNames);
    if (!customName || customName === catalog.name) return catalog;
    return { ...catalog, name: customName };
}

function applyConfiguredCatalogShape(catalog, lookupKey, shapes) {
    if (!catalog || !shapes || shapes.size === 0) return catalog;

    const normalizedKey = normalizeConfiguredCatalogEntryKey(lookupKey);
    if (!normalizedKey || !shapes.has(normalizedKey)) return catalog;
    if (catalog.posterShape === "landscape") return catalog;

    return { ...catalog, posterShape: "landscape" };
}

function getTextBackdropFromDetails(details, preferredLangs = ["it", "en"]) {
    const backdrops = details && details.images && Array.isArray(details.images.backdrops)
        ? details.images.backdrops
        : [];
    if (backdrops.length === 0) return null;

    for (const lang of preferredLangs) {
        const match = backdrops.find(b => b && b.iso_639_1 === lang && b.file_path);
        if (match) return `https://image.tmdb.org/t/p/original${match.file_path}`;
    }
    return null;
}

function isSearchCatalog(catalog) {
    return !!(
        catalog &&
        Array.isArray(catalog.extra) &&
        catalog.extra.some(extra => extra && extra.name === "search")
    );
}

function isTop10Catalog(catalog) {
    return !!(
        catalog &&
        typeof catalog.id === "string" &&
        (
            catalog.id.startsWith(TOP10_MANIFEST_PREFIX) ||
            catalog.id.startsWith(LEGACY_TOP10_MANIFEST_PREFIX)
        )
    );
}

function usesKitsuAnimeIds(catalogId) {
    return false;
}

const ERDB_RATING_PROVIDERS = new Set([
    "tmdb",
    "mdblist",
    "imdb",
    "tomatoes",
    "tomatoesaudience",
    "letterboxd",
    "metacritic",
    "metacriticuser",
    "trakt",
    "rogerebert",
    "myanimelist",
    "anilist",
    "kitsu"
]);
const ERDB_RATING_STYLES = new Set(["glass", "square", "plain"]);
const ERDB_IMAGE_TEXTS = new Set(["original", "clean", "alternative"]);
const ERDB_POSTER_LAYOUTS = new Set(["top", "bottom", "left", "right", "top-bottom", "left-right"]);
const ERDB_BACKDROP_LAYOUTS = new Set(["center", "right", "right-vertical"]);

function normalizeErdbBaseUrl(value) {
    if (typeof value !== "string") return "";
    const trimmed = value.trim();
    if (!trimmed) return "";
    return trimmed.replace(/\/+$/g, "");
}

function normalizeErdbRatings(rawRatings) {
    if (!rawRatings) return [];
    const values = Array.isArray(rawRatings)
        ? rawRatings
        : typeof rawRatings === "string"
            ? rawRatings.split(",")
            : [];
    return values
        .map(value => String(value || "").trim().toLowerCase())
        .filter(value => ERDB_RATING_PROVIDERS.has(value));
}

function normalizeErdbStyle(value) {
    const normalized = String(value || "").trim().toLowerCase();
    return ERDB_RATING_STYLES.has(normalized) ? normalized : "";
}

function normalizeErdbImageText(value) {
    const normalized = String(value || "").trim().toLowerCase();
    return ERDB_IMAGE_TEXTS.has(normalized) ? normalized : "";
}

function normalizeErdbPosterLayout(value) {
    const normalized = String(value || "").trim().toLowerCase();
    return ERDB_POSTER_LAYOUTS.has(normalized) ? normalized : "";
}

function normalizeErdbBackdropLayout(value) {
    const normalized = String(value || "").trim().toLowerCase();
    return ERDB_BACKDROP_LAYOUTS.has(normalized) ? normalized : "";
}

function normalizeErdbLang(value) {
    const normalized = String(value || "").trim().toLowerCase();
    if (!normalized) return "";
    return /^[a-z]{2}$/.test(normalized) ? normalized : "";
}

function decodeErdbConfig(value) {
    if (typeof value !== "string") return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    try {
        return JSON.parse(Buffer.from(trimmed, "base64url").toString("utf8"));
    } catch (err) {
        return null;
    }
}

function normalizeErdbId(value) {
    const rawValue = String(value || "").trim();
    if (!rawValue) return null;

    const imdbMatch = rawValue.match(/^tt\d+$/i);
    if (imdbMatch) return rawValue.toLowerCase();

    const tmdbTypedMatch = rawValue.match(/^tmdb:(movie|tv):(\d+)$/i);
    if (tmdbTypedMatch) {
        return `tmdb:${tmdbTypedMatch[1].toLowerCase()}:${tmdbTypedMatch[2]}`;
    }

    const providerMatch = rawValue.match(/^(tmdb|kitsu|anilist|myanimelist|mal):(\d+)$/i);
    if (providerMatch) {
        return `${providerMatch[1].toLowerCase()}:${providerMatch[2]}`;
    }

    return null;
}

function normalizeErdbMediaType(value) {
    const normalized = String(value || "").trim().toLowerCase();
    if (!normalized) return "";
    if (normalized === "movie") return "movie";
    if (normalized === "tv") return "tv";
    if (normalized === "series") return "tv";
    return "";
}

function resolveErdbMediaId(imdbId, tmdbId, mediaIdOverride = null, mediaType = null) {
    const normalizedType = normalizeErdbMediaType(mediaType);
    const overrideId = normalizeErdbId(mediaIdOverride);
    if (overrideId) {
        if (normalizedType && /^tmdb:\d+$/i.test(overrideId)) {
            const [, idPart] = overrideId.split(":");
            return `tmdb:${normalizedType}:${idPart}`;
        }
        return overrideId;
    }

    const normalizedImdb = normalizeImdbId(imdbId);
    if (normalizedImdb) return normalizedImdb;

    const tmdbValue = String(tmdbId || "").trim();
    if (!tmdbValue) return null;
    if (tmdbValue.toLowerCase().startsWith("tmdb:")) {
        const normalized = normalizeErdbId(tmdbValue);
        if (!normalized) return null;
        if (normalizedType && /^tmdb:\d+$/i.test(normalized)) {
            const [, idPart] = normalized.split(":");
            return `tmdb:${normalizedType}:${idPart}`;
        }
        return normalized;
    }
    if (/^\d+$/.test(tmdbValue)) {
        return normalizedType
            ? `tmdb:${normalizedType}:${tmdbValue}`
            : `tmdb:${tmdbValue}`;
    }
    return null;
}

function getErdbConfig(config = null) {
    const resolvedConfig = getRequestConfig(config);
    const encodedConfig = resolvedConfig && typeof resolvedConfig.erdbConfig === "string"
        ? resolvedConfig.erdbConfig
        : "";
    const rawConfig = decodeErdbConfig(encodedConfig);
    if (!rawConfig || typeof rawConfig !== "object") return null;

    const baseUrl = normalizeErdbBaseUrl(rawConfig.baseUrl);
    const tmdbKey = typeof rawConfig.tmdbKey === "string" ? rawConfig.tmdbKey.trim() : "";
    const mdblistKey = typeof rawConfig.mdblistKey === "string" ? rawConfig.mdblistKey.trim() : "";

    if (!baseUrl || !tmdbKey || !mdblistKey) return null;

    const ratings = normalizeErdbRatings(rawConfig.ratings);
    const lang = normalizeErdbLang(rawConfig.lang);
    const posterRatingStyle = normalizeErdbStyle(rawConfig.posterRatingStyle);
    const backdropRatingStyle = normalizeErdbStyle(rawConfig.backdropRatingStyle);
    const logoRatingStyle = normalizeErdbStyle(rawConfig.logoRatingStyle);
    const posterImageText = normalizeErdbImageText(rawConfig.posterImageText);
    const backdropImageText = normalizeErdbImageText(rawConfig.backdropImageText);
    const posterRatingsLayout = normalizeErdbPosterLayout(rawConfig.posterRatingsLayout);
    const backdropRatingsLayout = normalizeErdbBackdropLayout(rawConfig.backdropRatingsLayout);
    const maxPerSideRaw = rawConfig.posterRatingsMaxPerSide;
    const maxPerSideParsed = parseInt(maxPerSideRaw, 10);
    const posterRatingsMaxPerSide = Number.isFinite(maxPerSideParsed) && maxPerSideParsed >= 1 && maxPerSideParsed <= 20
        ? maxPerSideParsed
        : null;

    const enabledTypesRaw = resolvedConfig && typeof resolvedConfig.erdbTypes === "object"
        ? resolvedConfig.erdbTypes
        : {};
    const enabledTypes = {
        poster: enabledTypesRaw.poster !== false,
        backdrop: enabledTypesRaw.backdrop !== false,
        logo: enabledTypesRaw.logo !== false
    };

    return {
        baseUrl,
        tmdbKey,
        mdblistKey,
        ratings,
        lang,
        posterRatingStyle,
        backdropRatingStyle,
        logoRatingStyle,
        posterImageText,
        backdropImageText,
        posterRatingsLayout,
        posterRatingsMaxPerSide,
        backdropRatingsLayout,
        enabledTypes
    };
}

function buildErdbUrl(config, assetType, erdbId) {
    if (!config || !erdbId) return null;

    const query = new URLSearchParams();
    query.set("tmdbKey", config.tmdbKey);
    query.set("mdblistKey", config.mdblistKey);
    if (Array.isArray(config.ratings) && config.ratings.length > 0) {
        query.set("ratings", config.ratings.join(","));
    }
    if (config.lang) query.set("lang", config.lang);
    const ratingStyle = assetType === "poster"
        ? config.posterRatingStyle
        : assetType === "backdrop"
            ? config.backdropRatingStyle
            : config.logoRatingStyle;
    if (ratingStyle) query.set("ratingStyle", ratingStyle);

    if (assetType !== "logo") {
        const imageText = assetType === "backdrop" ? config.backdropImageText : config.posterImageText;
        if (imageText) query.set("imageText", imageText);
    }

    if (assetType === "poster") {
        if (config.posterRatingsLayout) {
            query.set("posterRatingsLayout", config.posterRatingsLayout);
        }
        if (config.posterRatingsMaxPerSide) {
            query.set("posterRatingsMaxPerSide", String(config.posterRatingsMaxPerSide));
        }
    }

    if (assetType === "backdrop" && config.backdropRatingsLayout) {
        query.set("backdropRatingsLayout", config.backdropRatingsLayout);
    }

    const baseUrl = config.baseUrl.replace(/\/+$/g, "");
    const encodedId = encodeURIComponent(erdbId);
    return `${baseUrl}/${assetType}/${encodedId}.jpg?${query.toString()}`;
}

function getConfiguredAssetUrl(config, assetType, imdbId, tmdbId, mediaIdOverride = null, mediaType = null) {
    const resolvedConfig = getRequestConfig(config);
    if (!resolvedConfig || typeof resolvedConfig !== "object") return null;

    const erdbConfig = getErdbConfig(resolvedConfig);
    if (erdbConfig) {
        if (!erdbConfig.enabledTypes[assetType]) return null;
        const erdbId = resolveErdbMediaId(imdbId, tmdbId, mediaIdOverride, mediaType);
        if (!erdbId) return null;
        return buildErdbUrl(erdbConfig, assetType, erdbId);
    }

    if (assetType !== "poster") return null;

    const topStreamingKey = typeof resolvedConfig.topStreamingKey === "string"
        ? resolvedConfig.topStreamingKey.trim()
        : "";
    if (topStreamingKey) {
        if (imdbId) {
            const imdbForTopStreaming = normalizeImdbId(imdbId);
            return `https://api.top-streaming.stream/${topStreamingKey}/imdb/poster-default/${imdbForTopStreaming}.jpg`;
        }
        if (!tmdbId) return null;
        return `https://api.top-streaming.stream/${topStreamingKey}/tmdb/poster-default/${tmdbId}.jpg`;
    }

    return null;
}

// Helper function to enrich and map TMDB items to Stremio Meta objects
async function enrichAndMapItems(results, stremioType, tmdbType, config = null, allowFuture = false, skipRegionCheck = false, seriesAvailabilityRegion = null, preferKitsuId = false) {
    const metaObjects = await Promise.all(results.map(async (item) => {
        let imdbId = null;
        let runtime = null;
        let cast = [];
        let director = [];
        let logo = null;
        let genres = [];
        let links = [];
        let trailers = [];
        let trailerStreams = [];
    let releaseInfo = (item.release_date || item.first_air_date || "").substring(0, 4);
    let exactReleaseDate = item.release_date || item.first_air_date;
    let details = null;
    let textBackdrop = null;
    let kitsuId = null;
        const resolvedConfig = getRequestConfig(config);
        const manifestUrl = getManifestUrl(resolvedConfig);

        if (item.genre_ids) {
            genres = item.genre_ids.map(id => GENRE_ID_TO_NAME[id]).filter(Boolean);
        }

        try {
            const typePath = tmdbType === "movie" ? "movie" : "tv";
            details = await fetchTmdbDetails(typePath, item.id, resolvedConfig);

            if (details) {
                textBackdrop = getTextBackdropFromDetails(details);
                if (tmdbType === "movie" && details.release_dates && details.release_dates.results) {
                    const itRelease = details.release_dates.results.find(r => r.iso_3166_1 === "IT");
                    if (itRelease && itRelease.release_dates && itRelease.release_dates.length > 0) {
                        const theatrical = itRelease.release_dates.find(d => d.type === 3);
                        if (theatrical) {
                            exactReleaseDate = theatrical.release_date.split("T")[0];
                        } else {
                            exactReleaseDate = itRelease.release_dates[0].release_date.split("T")[0];
                        }
                    }
                } else if (stremioType === "series") {
                    if (details.next_episode_to_air) {
                        exactReleaseDate = details.next_episode_to_air.air_date;
                    } else if (details.last_air_date) {
                        exactReleaseDate = details.last_air_date;
                    }
                }

                if (tmdbType === "movie" && exactReleaseDate) {
                    releaseInfo = exactReleaseDate;
                }

                if (stremioType === "series" && releaseInfo) {
                    if (details.in_production) {
                        releaseInfo = `${releaseInfo}-`;
                    } else if (details.last_air_date) {
                        const endYear = details.last_air_date.split("-")[0];
                        if (endYear && endYear !== releaseInfo) {
                            releaseInfo = `${releaseInfo}-${endYear}`;
                        }
                    }
                }

                if (tmdbType === "movie" && !allowFuture && !skipRegionCheck) {
                    let hasValidRelease = false;
                    if (details.release_dates && details.release_dates.results) {
                        const itRelease = details.release_dates.results.find(r => r.iso_3166_1 === "IT");
                        if (itRelease && itRelease.release_dates) {
                            const today = new Date().toISOString().split("T")[0];
                            const valid = itRelease.release_dates.some(d =>
                                d.release_date && d.release_date.split("T")[0] <= today
                            );
                            if (valid) hasValidRelease = true;
                        }
                    }

                    if (!hasValidRelease) {
                        return null;
                    }
                }

                if (tmdbType === "tv" && seriesAvailabilityRegion) {
                    const providersData = await fetchTmdbWatchProviders("tv", item.id, resolvedConfig);
                    const hasRegionAvailability = providersData &&
                        providersData.results &&
                        providersData.results[seriesAvailabilityRegion];
                    if (!hasRegionAvailability) {
                        return null;
                    }
                }

                if (details.external_ids && details.external_ids.imdb_id) {
                    imdbId = details.external_ids.imdb_id;
                    links.push({
                        name: item.vote_average ? item.vote_average.toFixed(1) : "N/A",
                        category: "imdb",
                        url: `https://imdb.com/title/${imdbId}`
                    });
                }

                if (details.belongs_to_collection) {
                    links.push({
                        name: details.belongs_to_collection.name,
                        category: "Collection",
                        url: `stremio:///search?search=${encodeURIComponent(details.belongs_to_collection.name)}`
                    });
                }

                if (details.production_companies) {
                    details.production_companies.slice(0, 2).forEach(c => {
                        links.push({
                            name: c.name,
                            category: "Production",
                            url: `stremio:///search?search=${encodeURIComponent(c.name)}`
                        });
                    });
                }

                if (details.networks) {
                    details.networks.slice(0, 2).forEach(n => {
                        links.push({
                            name: n.name,
                            category: "Networks",
                            url: `stremio:///search?search=${encodeURIComponent(n.name)}`
                        });
                    });
                }

                if (details.genres) {
                    genres = details.genres.map(g => g.name);
                    details.genres.forEach(g => {
                        links.push({
                            name: g.name,
                            category: "Genres",
                            url: `stremio:///discover/${encodeURIComponent(manifestUrl)}/${stremioType}/tmdb.${stremioType === "movie" ? "movie" : "series"}.popular?genre=${encodeURIComponent(g.name)}`
                        });
                    });
                }

                if (tmdbType === "movie") {
                    if (details.runtime) runtime = `${details.runtime} min`;
                } else if (details.episode_run_time && details.episode_run_time.length > 0) {
                    runtime = `${details.episode_run_time[0]} min`;
                }

                if (details.credits) {
                    if (details.credits.cast) {
                        cast = details.credits.cast.slice(0, 3).map(c => c.name);
                        details.credits.cast.slice(0, 3).forEach(c => {
                            links.push({
                                name: c.name,
                                category: "Cast",
                                url: `stremio:///search?search=${encodeURIComponent(c.name)}`
                            });
                        });
                    }
                    if (details.credits.crew) {
                        director = details.credits.crew.filter(c => c.job === "Director").map(c => c.name);
                        details.credits.crew.filter(c => c.job === "Director").forEach(c => {
                            links.push({
                                name: c.name,
                                category: "Directors",
                                url: `stremio:///search?search=${encodeURIComponent(c.name)}`
                            });
                        });
                    }
                }

                if (details.images && details.images.logos && details.images.logos.length > 0) {
                    const logoItem = details.images.logos.find(l => l.iso_639_1 === "it") ||
                        details.images.logos.find(l => l.iso_639_1 === "en") ||
                        details.images.logos[0];
                    if (logoItem) {
                        logo = `https://image.tmdb.org/t/p/w500${logoItem.file_path}`;
                    }
                }

                if (details.videos && details.videos.results && details.videos.results.length > 0) {
                    const tmdbTrailers = details.videos.results.filter(v => v.site === "YouTube" && v.type === "Trailer");
                    tmdbTrailers.sort((a, b) => {
                        const langA = a.iso_639_1 === "it" ? 2 : (a.iso_639_1 === "en" ? 1 : 0);
                        const langB = b.iso_639_1 === "it" ? 2 : (b.iso_639_1 === "en" ? 1 : 0);

                        if (langA !== langB) {
                            return langB - langA;
                        }

                        return (b.size || 0) - (a.size || 0);
                    });

                    if (tmdbTrailers.length > 0) {
                        tmdbTrailers.forEach(trailer => {
                            trailers.push({
                                source: trailer.key,
                                type: "Trailer"
                            });
                            trailerStreams.push({
                                title: trailer.name,
                                ytId: trailer.key
                            });
                        });
                    }
                }
            }
        } catch (error) {
            console.warn(`[Easy Catalogs] Failed to fetch details for ${item.id}`, error);
        }

        if (preferKitsuId) {
            try {
                kitsuId = await resolveKitsuIdForAnimeMedia(item, details, stremioType);
            } catch (error) {
                kitsuId = null;
            }

            if (!kitsuId) return null;
        }

        const formattedDate = exactReleaseDate ? exactReleaseDate.split("-").reverse().join("/") : null;
        if (tmdbType === "movie" && formattedDate) {
            releaseInfo = formattedDate;
        }

        let posterUrl = item.poster_path ? `https://image.tmdb.org/t/p/w500${item.poster_path}` : null;
        const configuredPosterUrl = getConfiguredAssetUrl(resolvedConfig, "poster", imdbId, item.id, null, stremioType);
        if (configuredPosterUrl) posterUrl = configuredPosterUrl;

        let backgroundUrl = item.backdrop_path ? `https://image.tmdb.org/t/p/original${item.backdrop_path}` : null;
        const configuredBackdropUrl = getConfiguredAssetUrl(resolvedConfig, "backdrop", imdbId, item.id, null, stremioType);
        if (configuredBackdropUrl) backgroundUrl = configuredBackdropUrl;

        let logoUrl = logo;
        const configuredLogoUrl = getConfiguredAssetUrl(resolvedConfig, "logo", imdbId, item.id, null, stremioType);
        if (configuredLogoUrl) logoUrl = configuredLogoUrl;

        const metaId = kitsuId ? `kitsu:${kitsuId}` : (imdbId || `tmdb:${item.id}`);

        return {
            id: metaId,
            type: stremioType,
            name: item.title || item.name,
            poster: posterUrl,
            background: backgroundUrl,
            textBackdrop: textBackdrop || undefined,
            logo: logoUrl,
            description: item.overview || (details && details.overview) || undefined,
            releaseInfo: releaseInfo,
            released: safeToIsoString(exactReleaseDate),
            imdbRating: item.vote_average ? item.vote_average.toFixed(1) : null,
            runtime: runtime,
            genres: genres,
            cast: cast,
            director: director,
            links: links,
            trailers: trailers,
            trailerStreams: trailerStreams,
            behaviorHints: {
                defaultVideoId: stremioType === "movie" ? metaId : null,
                hasScheduledVideos: stremioType === "series"
            }
        };
    }));

    return metaObjects.filter(Boolean);
}

const DEFAULT_TMDB_API_KEY = process.env.TMDB_API_KEY || "68e094699525b18a70bab2f86b1fa706";
const BASE_URL = "https://api.themoviedb.org/3";
function decodeBase64Literal(value) {
    return Buffer.from(String(value || ""), "base64").toString("utf8");
}

const LEGACY_TOP10_MOVIE_CONFIG_ID = decodeBase64Literal("anVzdHdhdGNoX3RvcDEwX21vdmll");
const LEGACY_TOP10_SERIES_CONFIG_ID = decodeBase64Literal("anVzdHdhdGNoX3RvcDEwX3Nlcmllcw==");
const TOP10_MANIFEST_PREFIX = "t10.";
const TOP10_MOVIE_MANIFEST_ID = `${TOP10_MANIFEST_PREFIX}movie.top10`;
const TOP10_SERIES_MANIFEST_ID = `${TOP10_MANIFEST_PREFIX}series.top10`;
const LEGACY_TOP10_MANIFEST_PREFIX = decodeBase64Literal("ancu");
const LEGACY_TOP10_MOVIE_MANIFEST_ID = decodeBase64Literal("ancubW92aWUudG9wMTA=");
const LEGACY_TOP10_SERIES_MANIFEST_ID = decodeBase64Literal("ancuc2VyaWVzLnRvcDEw");

const TOP10_SOURCE_BASE_URL = decodeBase64Literal("aHR0cHM6Ly93d3cuanVzdHdhdGNoLmNvbQ==");
const TOP10_SOURCE_GRAPHQL_URL = decodeBase64Literal("aHR0cHM6Ly9hcGlzLmp1c3R3YXRjaC5jb20vZ3JhcGhxbA==");
const TOP10_SOURCE_IMAGE_BASE_URL = decodeBase64Literal("aHR0cHM6Ly9pbWFnZXMuanVzdHdhdGNoLmNvbQ==");
const TOP10_SOURCE_COUNTRY = "IT";
const TOP10_PROVIDER_PAGE_SLUGS = {
    netflix: "netflix",
    amazon: "amazon-prime-video",
    disney: "disney-plus",
    apple: "apple-tv-plus",
    hbo: "hbo-max",
    paramount: "paramount-plus",
    now: "now-tv",
    sky: "now-tv",
    mediaset: "mediaset-infinity",
    timvision: "timvision"
};

function buildTop10ManifestId(type, slug = null) {
    return slug
        ? `${TOP10_MANIFEST_PREFIX}${type}.${slug}_top10`
        : `${TOP10_MANIFEST_PREFIX}${type}.top10`;
}

function normalizeTop10ManifestId(catalogId) {
    const normalizedCatalogId = String(catalogId || "").trim();
    if (!normalizedCatalogId) return normalizedCatalogId;
    if (normalizedCatalogId === LEGACY_TOP10_MOVIE_MANIFEST_ID) return TOP10_MOVIE_MANIFEST_ID;
    if (normalizedCatalogId === LEGACY_TOP10_SERIES_MANIFEST_ID) return TOP10_SERIES_MANIFEST_ID;
    if (normalizedCatalogId.startsWith(LEGACY_TOP10_MANIFEST_PREFIX)) {
        return `${TOP10_MANIFEST_PREFIX}${normalizedCatalogId.slice(LEGACY_TOP10_MANIFEST_PREFIX.length)}`;
    }
    return normalizedCatalogId;
}

const CATALOG_ID_TO_SHAPE_KEY = {
    "tmdb.movie.upcoming": "upcoming_movie",
    "tmdb.series.upcoming": "upcoming_series",
    "tmdb.movie.now_playing": "now_playing_movie",
    "tmdb.movie.popular": "popular_movie",
    "tmdb.series.popular": "popular_series",
    "tmdb.movie.trending": "trending_movie",
    "tmdb.series.trending": "trending_series",
    "tmdb.movie.top_rated": "top_rated_movie",
    "tmdb.series.top_rated": "top_rated_series",
    "tmdb.movie.year": "year_movie",
    "tmdb.series.year": "year_series",
    "tmdb.movie.kids": "kids_movie",
    "tmdb.series.kids": "kids_series",
    "tmdb.movie.search": "search_movie",
    "tmdb.series.search": "search_series",
    "tmdb.movie.anime": "anime_tmdb_movie",
    "tmdb.series.anime": "anime_tmdb_series",
    "tmdb.movie.anime_search": "anime_tmdb_search_movie",
    "tmdb.series.anime_search": "anime_tmdb_search_series"
};

const KITSU_CATALOG_ID_TO_SHAPE_KEY = {
    "kitsu.series.latest": "anime_kitsu_latest_series",
    "kitsu.series.popular": "anime_kitsu_popular_series",
    "kitsu.movie.latest": "anime_kitsu_latest_movie",
    "kitsu.movie.popular": "anime_kitsu_popular_movie",
    "kitsu.series.ova": "anime_kitsu_ova",
    "kitsu.series.ova_latest": "anime_kitsu_latest_ova",
    "kitsu.series.ona": "anime_kitsu_ona",
    "kitsu.series.ona_latest": "anime_kitsu_latest_ona",
    "kitsu.series.special": "anime_kitsu_special",
    "kitsu.series.special_latest": "anime_kitsu_latest_special",
    "kitsu.movie.search": "anime_kitsu_search_movie",
    "kitsu.series.search": "anime_kitsu_search_series",
    "kitsu.series.ova_search": "anime_kitsu_search_ova",
    "kitsu.series.ona_search": "anime_kitsu_search_ona",
    "kitsu.series.special_search": "anime_kitsu_search_special"
};

function getCatalogShapeLookupKey(catalogId) {
    const normalizedId = String(catalogId || "").trim();
    if (!normalizedId) return "";

    if (isTop10CatalogId(normalizedId)) {
        const normalizedTop10 = normalizeTop10ManifestId(normalizedId);
        const providerMatch = normalizedTop10.match(/^t10\.(movie|series)\.([a-z0-9]+)_top10$/i);
        if (providerMatch) {
            return `${providerMatch[2].toLowerCase()}_top10`;
        }
        return TOP10_GLOBAL_CATALOG_ID;
    }

    if (CATALOG_ID_TO_SHAPE_KEY[normalizedId]) {
        return CATALOG_ID_TO_SHAPE_KEY[normalizedId];
    }

    if (KITSU_CATALOG_ID_TO_SHAPE_KEY[normalizedId]) {
        return KITSU_CATALOG_ID_TO_SHAPE_KEY[normalizedId];
    }

    if (normalizedId.startsWith("tmdb.")) {
        const parts = normalizedId.split(".");
        if (parts.length >= 3) {
            const lastPart = parts[2];
            if (!lastPart) return "";
            if (lastPart.endsWith("_catalog")) return lastPart;
            return `${lastPart}_original`;
        }
    }

    return "";
}

function shouldLandscapeCatalog(catalogId, shapes) {
    if (!shapes || shapes.size === 0) return false;
    const lookupKey = getCatalogShapeLookupKey(catalogId);
    if (!lookupKey) return false;
    const normalizedKey = normalizeConfiguredCatalogEntryKey(lookupKey);
    return !!(normalizedKey && shapes.has(normalizedKey));
}

function applyLandscapeToMetas(metas, shouldLandscape, config = null) {
    if (!shouldLandscape || !Array.isArray(metas)) return metas;
    const erdbConfig = getErdbConfig(config);
    const erdbPosterEnabled = !!(erdbConfig && erdbConfig.enabledTypes && erdbConfig.enabledTypes.poster);
    const erdbBackdropEnabled = !!(erdbConfig && erdbConfig.enabledTypes && erdbConfig.enabledTypes.backdrop);
    metas.forEach(meta => {
        if (meta && typeof meta === "object") {
            const landscapeImage = erdbBackdropEnabled
                ? (meta.background || meta.textBackdrop)
                : (meta.textBackdrop || meta.background);
            meta.posterShape = "landscape";
            if (landscapeImage) {
                if (erdbBackdropEnabled) {
                    meta.poster = landscapeImage;
                } else if (!erdbPosterEnabled) {
                    meta.poster = landscapeImage;
                }
            }
        }
    });
    return metas;
}
const KITSU_BASE_URL = "https://kitsu.io/api/edge";
const ANIME_MAPPING_BASE_URL = "https://animemapping.stremio.dpdns.org";
const EASY_STREAMS_BASE_URL = "https://easystreams.stremio.dpdns.org";
const KITSU_TTL_SECONDS = {
    anime: 24 * 3600,
    episodes: 12 * 3600,
    externalLookup: 24 * 3600,
    search: 6 * 3600,
    catalog: 6 * 3600,
    animeMapping: 30 * 24 * 3600
};

function getTmdbApiKey(config = null) {
    const resolvedConfig = getRequestConfig(config);
    const customKey = typeof resolvedConfig.tmdbApiKey === "string"
        ? resolvedConfig.tmdbApiKey.trim()
        : "";
    const isValidFormat = /^[a-f0-9]{32}$/i.test(customKey);
    return (isValidFormat ? customKey : "") || DEFAULT_TMDB_API_KEY;
}

function normalizeKitsuId(value) {
    const rawValue = String(value || "").trim();
    if (!rawValue) return null;

    const match = rawValue.match(/^kitsu:(\d+)$/i) || rawValue.match(/^(\d+)$/);
    return match ? match[1] : null;
}

const STREAMING_PROVIDER_TYPES = ["flatrate", "ads", "free"];
const PROVIDER_NAME_ALIASES = {
    "NOW": "Sky Go / NOW",
    "NOW TV": "Sky Go / NOW",
    "Now TV": "Sky Go / NOW",
    "Sky": "Sky Go / NOW",
    "Sky Go": "Sky Go / NOW"
};

function normalizeProviderName(providerName) {
    const rawProviderName = String(providerName || "").trim();
    if (!rawProviderName) return "";
    return PROVIDER_NAME_ALIASES[rawProviderName] || rawProviderName;
}

function getProviderRegions(providerName) {
    const normalizedProviderName = normalizeProviderName(providerName);
    return normalizedProviderName === "HBO Max" ? ["IT", "US"] : ["IT"];
}

function getProviderRegion(providerName) {
    return getProviderRegions(providerName)[0];
}

function getPrimaryReleaseDate(item) {
    return (item && (item.release_date || item.first_air_date)) || "";
}

function filterCatalogItems(results, catalogId, allowFuture = false) {
    const today = new Date().toISOString().split("T")[0];
    return (Array.isArray(results) ? results : []).filter(item => {
        const date = getPrimaryReleaseDate(item);

        if (!catalogId.includes("anime")) {
            if (item.genre_ids && item.genre_ids.includes(16) && item.original_language === "ja") {
                return false;
            }
        }

        if (allowFuture) return !!date;
        return !!date && date <= today;
    });
}

function getGenreIdForTmdbType(tmdbType, genre) {
    if (!genre) return null;
    return tmdbType === "movie" ? MOVIE_GENRES[genre] : TV_GENRES[genre];
}

async function fetchTmdbWatchProviders(tmdbType, tmdbId, config = null) {
    const providersCacheKey = `tmdb:watchproviders:${tmdbType}:${tmdbId}`;
    let providersData = await cache.get(providersCacheKey);

    if (isNegativeCache(providersData)) {
        return null;
    }

    if (providersData) {
        return providersData;
    }

    try {
        const providersUrl = `${BASE_URL}/${tmdbType}/${tmdbId}/watch/providers?api_key=${getTmdbApiKey(config)}`;
        const providersRes = await fetch(providersUrl);
        providersData = await providersRes.json();

        if (providersData && !providersData.status_message) {
            await cache.set(providersCacheKey, providersData, withTtlJitter(CACHE_TTL_SECONDS.providers));
            return providersData;
        }
    } catch (error) {
        console.warn(`[Easy Catalogs] Watch providers fetch failed for ${tmdbType}:${tmdbId}:`, error.message);
    }

    await cache.set(providersCacheKey, createNegativeCache("providers_fetch_failed"), NEGATIVE_CACHE_TTL_SECONDS);
    return null;
}

function getRegionStreamingProviderIds(providersData, region) {
    const regionData = providersData &&
        providersData.results &&
        providersData.results[region];

    if (!regionData) return [];

    const providerIds = new Set();
    STREAMING_PROVIDER_TYPES.forEach(bucketName => {
        const bucketProviders = Array.isArray(regionData[bucketName]) ? regionData[bucketName] : [];
        bucketProviders.forEach(provider => {
            if (provider && provider.provider_id !== undefined && provider.provider_id !== null) {
                providerIds.add(String(provider.provider_id));
            }
        });
    });

    return Array.from(providerIds);
}

function isExclusiveToProvider(providersData, region, providerId) {
    const providerIds = getRegionStreamingProviderIds(providersData, region);
    const allowedProviderIds = String(providerId || "")
        .split("|")
        .map(value => value.trim())
        .filter(Boolean);
    if (providerIds.length === 0 || allowedProviderIds.length === 0) return false;
    if (providerIds.length === 1) return allowedProviderIds.includes(providerIds[0]);
    return providerIds.every(id => allowedProviderIds.includes(id));
}

function replaceWatchRegion(queryParams, region) {
    if (!queryParams.includes("&watch_region=")) {
        return `${queryParams}&watch_region=${region}`;
    }

    return queryParams.replace(/&watch_region=[A-Z]{2}/, `&watch_region=${region}`);
}

function replaceProviderQueryRegion(queryParams, region) {
    let nextQueryParams = replaceWatchRegion(queryParams, region);

    if (nextQueryParams.includes("&region=")) {
        nextQueryParams = nextQueryParams.replace(/&region=[A-Z]{2}/, `&region=${region}`);
    }

    return nextQueryParams;
}

async function fetchTmdbPagedResults(endpoint, queryParams, options = {}) {
    const startPage = Number.isInteger(options.startPage) && options.startPage > 0 ? options.startPage : 1;
    const maxPages = Number.isInteger(options.maxPages) && options.maxPages > 0 ? options.maxPages : null;
    const minItems = Number.isInteger(options.minItems) && options.minItems > 0 ? options.minItems : null;
    const itemFilter = typeof options.itemFilter === "function" ? options.itemFilter : null;
    const items = [];
    let page = startPage;
    let totalPages = null;
    let pagesFetched = 0;

    while (true) {
        const currentUrl = `${BASE_URL}/${endpoint}?${queryParams}&page=${page}`;
        console.log(`[Easy Catalogs] Fetching Page ${page}: ${currentUrl}`);

        try {
            const response = await fetch(currentUrl);
            const data = await response.json();
            const rawResults = Array.isArray(data && data.results) ? data.results : [];
            if (Number.isFinite(data && data.total_pages)) {
                totalPages = data.total_pages;
            }

            if (rawResults.length === 0) {
                break;
            }

            const filteredResults = itemFilter
                ? await itemFilter(rawResults, page)
                : rawResults;
            if (Array.isArray(filteredResults) && filteredResults.length > 0) {
                items.push(...filteredResults);
            }
        } catch (error) {
            console.error(`[Easy Catalogs] Fetch Error on page ${page}:`, error);
            break;
        }

        pagesFetched += 1;

        const reachedMinItems = minItems && items.length >= minItems;
        const reachedMaxPages = maxPages && pagesFetched >= maxPages;
        const reachedTotalPages = totalPages && page >= totalPages;

        if (reachedMinItems || reachedMaxPages || reachedTotalPages) {
            break;
        }

        page += 1;
    }

    return items;
}

function sortProviderOriginalEntries(a, b) {
    const dateA = getPrimaryReleaseDate(a.item);
    const dateB = getPrimaryReleaseDate(b.item);
    const dateCompare = dateB.localeCompare(dateA);
    if (dateCompare !== 0) return dateCompare;

    if (a.sourceRank !== b.sourceRank) {
        return a.sourceRank - b.sourceRank;
    }

    return (b.item.popularity || 0) - (a.item.popularity || 0);
}

async function fetchProviderOriginalMergedResults({
    id,
    tmdbType,
    providerName,
    extra = {},
    config = null,
    allowFuture = false,
    skip = 0
}) {
    const canonicalProviderName = normalizeProviderName(providerName);
    const providerId = PROVIDERS[canonicalProviderName];
    const originalSourceId = tmdbType === "movie"
        ? COMPANY_IDS[canonicalProviderName]
        : NETWORK_IDS[canonicalProviderName];

    if (!providerId || !originalSourceId) {
        return null;
    }

    const dateField = tmdbType === "movie" ? "primary_release_date" : "first_air_date";
    const endpoint = tmdbType === "movie" ? "discover/movie" : "discover/tv";
    const originalFilterName = tmdbType === "movie" ? "with_companies" : "with_networks";
    const genreId = getGenreIdForTmdbType(tmdbType, extra.genre);
    const today = new Date().toISOString().split("T")[0];
    const targetCount = skip + 20;
    const providerRegions = getProviderRegions(canonicalProviderName);

    for (const region of providerRegions) {
        let baseQueryParams = `api_key=${getTmdbApiKey(config)}&language=it-IT&sort_by=${dateField}.desc&${dateField}.lte=${today}`;
        if (tmdbType === "movie") {
            baseQueryParams += `&region=${region}`;
        }
        if (genreId) {
            baseQueryParams += `&with_genres=${genreId}`;
        }

        const sharedProviderParams = `&with_watch_providers=${providerId}&watch_region=${region}`;
        const originalQueryParams = `${baseQueryParams}&${originalFilterName}=${originalSourceId}${sharedProviderParams}`;

        const originalItems = await fetchTmdbPagedResults(endpoint, originalQueryParams, {
            minItems: targetCount,
            itemFilter: async rawResults => filterCatalogItems(rawResults, id, allowFuture)
        });
        const originalIds = new Set(originalItems.map(item => String(item.id)));

        const exclusiveQueryParams = `${baseQueryParams}${sharedProviderParams}`;
        const exclusiveItems = await fetchTmdbPagedResults(endpoint, exclusiveQueryParams, {
            minItems: targetCount,
            itemFilter: async rawResults => {
                const filteredItems = filterCatalogItems(rawResults, id, allowFuture);
                const exclusiveCandidates = await Promise.all(filteredItems.map(async item => {
                    if (originalIds.has(String(item.id))) {
                        return null;
                    }

                    const providersData = await fetchTmdbWatchProviders(tmdbType, item.id, config);
                    return isExclusiveToProvider(providersData, region, providerId) ? item : null;
                }));

                return exclusiveCandidates.filter(Boolean);
            }
        });

        const mergedEntries = new Map();
        originalItems.forEach(item => {
            mergedEntries.set(String(item.id), { item, sourceRank: 0 });
        });
        exclusiveItems.forEach(item => {
            const itemKey = String(item.id);
            if (!mergedEntries.has(itemKey)) {
                mergedEntries.set(itemKey, { item, sourceRank: 1 });
            }
        });

        const items = Array.from(mergedEntries.values())
            .sort(sortProviderOriginalEntries)
            .slice(skip, skip + 20)
            .map(entry => entry.item);

        if (items.length > 0 || region === providerRegions[providerRegions.length - 1]) {
            return { items, region };
        }
    }

    return { items: [], region: getProviderRegion(providerName) };
}

async function fetchCatalogMetasForQuery({
    endpoint,
    queryParams,
    id,
    type,
    tmdbType,
    config,
    allowFuture,
    providerRegion = null,
    startPage = 1,
    maxPages = null,
    pageOffset = 0
}) {
    const skipRegionCheck = (id === "tmdb.movie.anime" || id === "tmdb.series.anime");
    const preferKitsuId = usesKitsuAnimeIds(id);
    let seriesAvailabilityRegion = null;
    if (tmdbType === "tv" && !id.includes("anime")) {
        seriesAvailabilityRegion = providerRegion || "IT";
    }
    const metas = [];
    let page = Number.isInteger(startPage) && startPage > 0 ? startPage : 1;
    let totalPages = null;
    let pagesFetched = 0;
    let remainingOffset = Number.isInteger(pageOffset) && pageOffset > 0 ? pageOffset : 0;

    while (true) {
        const currentUrl = `${BASE_URL}/${endpoint}?${queryParams}&page=${page}`;
        console.log(`[Easy Catalogs] Fetching Page ${page}: ${currentUrl}`);

        try {
            const response = await fetch(currentUrl);
            const data = await response.json();
            const rawResults = Array.isArray(data && data.results) ? data.results : [];
            if (Number.isFinite(data && data.total_pages)) {
                totalPages = data.total_pages;
            }

            if (rawResults.length === 0) {
                break;
            }

            let filteredResults = filterCatalogItems(rawResults, id, allowFuture);
            if (remainingOffset > 0 && filteredResults.length > 0) {
                filteredResults = filteredResults.slice(remainingOffset);
                remainingOffset = 0;
            }
            if (filteredResults.length > 0) {
                const mapped = await enrichAndMapItems(
                    filteredResults,
                    type,
                    tmdbType,
                    config,
                    allowFuture,
                    skipRegionCheck,
                    seriesAvailabilityRegion,
                    preferKitsuId
                );
                metas.push(...mapped);
            }
        } catch (error) {
            console.error(`[Easy Catalogs] Fetch Error on page ${page}:`, error);
            break;
        }

        pagesFetched += 1;
        const reachedMaxPages = Number.isInteger(maxPages) && maxPages > 0 && pagesFetched >= maxPages;
        const reachedTotalPages = totalPages && page >= totalPages;
        if (metas.length >= 20 || reachedMaxPages || reachedTotalPages) {
            break;
        }

        page += 1;
    }

    return metas.slice(0, 20);
}

function uniqueNonEmptyStrings(values) {
    return [...new Set(
        (Array.isArray(values) ? values : [])
            .map(value => String(value || "").trim())
            .filter(Boolean)
    )];
}

function normalizeMatchTitle(value) {
    return String(value || "")
        .normalize("NFKD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim();
}

function getYearFromValue(value) {
    const match = String(value || "").match(/\b(\d{4})\b/);
    return match ? Number.parseInt(match[1], 10) : null;
}

function getTmdbGenreIds(payload = {}) {
    if (Array.isArray(payload.genre_ids)) {
        return payload.genre_ids
            .map(value => Number.parseInt(String(value || ""), 10))
            .filter(value => Number.isFinite(value));
    }

    if (Array.isArray(payload.genres)) {
        return payload.genres
            .map(entry => Number.parseInt(String(entry && entry.id || ""), 10))
            .filter(value => Number.isFinite(value));
    }

    return [];
}

function hasTmdbCountryCode(payload = {}, code) {
    const normalizedCode = String(code || "").trim().toUpperCase();
    if (!normalizedCode) return false;

    const originCountries = Array.isArray(payload.origin_country) ? payload.origin_country : [];
    const productionCountries = Array.isArray(payload.production_countries)
        ? payload.production_countries.map(entry => entry && entry.iso_3166_1)
        : [];

    return [...originCountries, ...productionCountries]
        .some(entry => String(entry || "").trim().toUpperCase() === normalizedCode);
}

function hasTmdbLanguageCode(payload = {}, code) {
    const normalizedCode = String(code || "").trim().toLowerCase();
    if (!normalizedCode) return false;

    if (String(payload.original_language || "").trim().toLowerCase() === normalizedCode) {
        return true;
    }

    const spokenLanguages = Array.isArray(payload.spoken_languages) ? payload.spoken_languages : [];
    return spokenLanguages.some(entry =>
        String(entry && entry.iso_639_1 || "").trim().toLowerCase() === normalizedCode
    );
}

function isTmdbAnimeDetails(payload = {}) {
    if (!payload || typeof payload !== "object") return false;

    const genreIds = getTmdbGenreIds(payload);
    const isAnimation = genreIds.includes(16);
    if (!isAnimation) return false;

    return hasTmdbLanguageCode(payload, "ja") || hasTmdbCountryCode(payload, "JP");
}

function safeToIsoString(value) {
    if (!value) return null;
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function isReleasedOnOrBeforeToday(value) {
    const rawValue = String(value || "").trim();
    if (!rawValue) return false;

    const parsed = new Date(rawValue);
    if (Number.isNaN(parsed.getTime())) return false;

    const today = new Date();
    today.setHours(23, 59, 59, 999);
    return parsed.getTime() <= today.getTime();
}

function getMetaReleaseTimestamp(meta = {}) {
    const releasedTimestamp = meta && meta.released
        ? Date.parse(meta.released)
        : Number.NaN;
    if (Number.isFinite(releasedTimestamp)) {
        return releasedTimestamp;
    }

    const releaseYear = getYearFromValue(meta && (meta.releaseInfo || meta.year));
    return Number.isFinite(releaseYear)
        ? Date.UTC(releaseYear, 0, 1)
        : 0;
}

function sortMetasByReleaseDesc(metas = []) {
    return (Array.isArray(metas) ? [...metas] : []).sort((left, right) =>
        getMetaReleaseTimestamp(right) - getMetaReleaseTimestamp(left)
    );
}

function getLastPathSegment(value) {
    const parts = String(value || "")
        .split("/")
        .map(part => part.trim())
        .filter(Boolean);
    return parts.length > 0 ? parts[parts.length - 1] : "";
}

function getTitleFromSlugPath(fullPath) {
    const slug = getLastPathSegment(fullPath);
    return slug ? slug.replace(/-/g, " ").trim() : "";
}

function extractEmbeddedJsonObject(html, marker) {
    if (typeof html !== "string" || !marker) return null;

    const markerIndex = html.indexOf(marker);
    if (markerIndex === -1) return null;

    const startIndex = html.indexOf("{", markerIndex + marker.length);
    if (startIndex === -1) return null;

    let depth = 0;
    let inString = false;
    let escaped = false;

    for (let index = startIndex; index < html.length; index += 1) {
        const character = html[index];

        if (escaped) {
            escaped = false;
            continue;
        }

        if (inString && character === "\\") {
            escaped = true;
            continue;
        }

        if (character === "\"") {
            inString = !inString;
            continue;
        }

        if (inString) continue;

        if (character === "{") {
            depth += 1;
            continue;
        }

        if (character === "}") {
            depth -= 1;
            if (depth === 0) {
                return html.slice(startIndex, index + 1);
            }
        }
    }

    return null;
}

function extractTop10ApolloState(html) {
    const jsonString = extractEmbeddedJsonObject(html, "window.__APOLLO_STATE__=");
    if (!jsonString) return null;

    try {
        const parsed = JSON.parse(jsonString);
        if (parsed && typeof parsed === "object" && parsed.defaultClient && typeof parsed.defaultClient === "object") {
            return parsed.defaultClient;
        }
        return parsed;
    } catch (error) {
        return null;
    }
}

function getStateReferenceId(reference) {
    return reference && typeof reference === "object" && typeof reference.id === "string"
        ? reference.id
        : null;
}

function resolveTop10StateEntry(state, reference) {
    const referenceId = getStateReferenceId(reference);
    if (referenceId && state && typeof state === "object" && state[referenceId]) {
        return state[referenceId];
    }

    return reference && typeof reference === "object" ? reference : null;
}

function buildTop10ImageUrl(pathValue) {
    const rawPath = String(pathValue || "").trim();
    if (!rawPath) return null;
    if (/^https?:\/\//i.test(rawPath)) return rawPath;
    if (rawPath.startsWith("/poster/") || rawPath.startsWith("/backdrop/") || rawPath.startsWith("/icon/")) {
        return `${TOP10_SOURCE_IMAGE_BASE_URL}${rawPath}`;
    }
    if (rawPath.startsWith("/")) {
        return `${TOP10_SOURCE_BASE_URL}${rawPath}`;
    }
    return rawPath;
}

function getTop10ContentEntry(state, nodeEntry) {
    if (!nodeEntry || typeof nodeEntry !== "object") return null;

    const contentKey = Object.keys(nodeEntry).find(key => key.startsWith("content("));
    return contentKey ? resolveTop10StateEntry(state, nodeEntry[contentKey]) : null;
}

function getTop10ScoringEntry(state, contentEntry) {
    if (!contentEntry || typeof contentEntry !== "object") return null;

    const scoringKey = Object.keys(contentEntry).find(key => key.startsWith("scoring"));
    return scoringKey ? resolveTop10StateEntry(state, contentEntry[scoringKey]) : null;
}

function getTop10PrimaryBackdropUrl(state, contentEntry) {
    if (!contentEntry || typeof contentEntry !== "object") return null;

    const backdropsKey = Object.keys(contentEntry).find(key => key.startsWith("backdrops("));
    const backdrops = backdropsKey && Array.isArray(contentEntry[backdropsKey])
        ? contentEntry[backdropsKey]
        : [];
    if (backdrops.length === 0) return null;

    const backdropEntry = resolveTop10StateEntry(state, backdrops[0]);
    return backdropEntry && backdropEntry.backdropUrl
        ? buildTop10ImageUrl(backdropEntry.backdropUrl)
        : null;
}

function extractTop10ChartEntries(state, options = {}) {
    if (!state || typeof state !== "object") return [];

    const objectType = String(options.objectType || "").trim().toUpperCase();
    const requiresPackages = options.requiresPackages === true;
    if (!objectType) return [];

    const queryKey = Object.keys(state).find(key =>
        key.startsWith("$ROOT_QUERY.streamingCharts(") &&
        state[key] &&
        Array.isArray(state[key].edges) &&
        key.includes(`"country":"${TOP10_SOURCE_COUNTRY}"`) &&
        key.includes(`"objectType":"${objectType}"`) &&
        key.includes("\"first\":10") &&
        key.includes("\"category\":\"WEEKLY_POPULARITY_SAME_CONTENT_TYPE\"") &&
        (requiresPackages ? key.includes("\"packages\":[") : !key.includes("\"packages\":["))
    );
    if (!queryKey) return [];

    const queryEntry = state[queryKey];
    const edgeReferences = Array.isArray(queryEntry && queryEntry.edges) ? queryEntry.edges : [];

    return edgeReferences
        .map((edgeReference, index) => {
            const edgeEntry = resolveTop10StateEntry(state, edgeReference);
            const nodeEntry = resolveTop10StateEntry(state, edgeEntry && edgeEntry.node);
            const contentEntry = getTop10ContentEntry(state, nodeEntry);
            if (!nodeEntry || !contentEntry || !contentEntry.title) return null;

            const scoringEntry = getTop10ScoringEntry(state, contentEntry);
            const chartInfoEntry = resolveTop10StateEntry(state, edgeEntry && edgeEntry.streamingChartInfo);

            return {
                jwId: typeof nodeEntry.id === "string" ? nodeEntry.id : null,
                title: String(contentEntry.title).trim(),
                fullPath: typeof contentEntry.fullPath === "string" ? contentEntry.fullPath : null,
                year: getYearFromValue(contentEntry.originalReleaseYear),
                poster: buildTop10ImageUrl(
                    contentEntry['posterUrl({"format":"JPG","profile":"S166"})'] ||
                    contentEntry['posterUrl({})'] ||
                    contentEntry.posterUrl
                ),
                background: getTop10PrimaryBackdropUrl(state, contentEntry),
                imdbRating: scoringEntry && Number.isFinite(Number(scoringEntry.imdbScore))
                    ? Number(scoringEntry.imdbScore)
                    : null,
                rank: Number.parseInt(String(chartInfoEntry && chartInfoEntry.rank || ""), 10) || (index + 1),
                trend: chartInfoEntry && chartInfoEntry.trend ? String(chartInfoEntry.trend) : null,
                trendDifference: Number.parseInt(String(chartInfoEntry && chartInfoEntry.trendDifference || ""), 10) || 0,
                topRank: Number.parseInt(String(chartInfoEntry && chartInfoEntry.topRank || ""), 10) || null
            };
        })
        .filter(Boolean);
}

async function fetchTop10ChartEntriesFromGraphql(options = {}) {
    const objectType = String(options.objectType || "").trim().toUpperCase();
    if (!objectType) return [];

    const cacheKey = `top10:graphql:v1:${objectType}:${TOP10_SOURCE_COUNTRY}`;
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return [];
    if (cached) return cached;

    const query = `
        query GetStreamingCharts($country: Country!, $language: Language!, $filter: StreamingChartsFilter!, $first: Int!, $after: String!) {
            streamingCharts(country: $country, filter: $filter, first: $first, after: $after) {
                edges {
                    streamingChartInfo {
                        rank
                        trend
                        trendDifference
                        topRank
                    }
                    node {
                        id
                        content(country: $country, language: $language) {
                            title
                            fullPath
                            originalReleaseYear
                            posterUrl(format: JPG, profile: S166)
                        }
                    }
                }
            }
        }
    `;

    try {
        const response = await fetch(TOP10_SOURCE_GRAPHQL_URL, {
            method: "POST",
            headers: {
                Accept: "application/json",
                "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
            },
            body: JSON.stringify({
                query,
                variables: {
                    country: TOP10_SOURCE_COUNTRY,
                    language: "it",
                    first: 10,
                    after: "",
                    filter: {
                        category: "WEEKLY_POPULARITY_SAME_CONTENT_TYPE",
                        objectType
                    }
                }
            })
        });
        const payload = await response.json();
        const edges = Array.isArray(payload && payload.data && payload.data.streamingCharts && payload.data.streamingCharts.edges)
            ? payload.data.streamingCharts.edges
            : [];

        const entries = edges
            .map((edge, index) => {
                const node = edge && edge.node && typeof edge.node === "object" ? edge.node : null;
                const content = node && node.content && typeof node.content === "object" ? node.content : null;
                const chartInfo = edge && edge.streamingChartInfo && typeof edge.streamingChartInfo === "object"
                    ? edge.streamingChartInfo
                    : null;
                if (!node || !content || !content.title) return null;

                return {
                    jwId: typeof node.id === "string" ? node.id : null,
                    title: String(content.title).trim(),
                    fullPath: typeof content.fullPath === "string" ? content.fullPath : null,
                    year: getYearFromValue(content.originalReleaseYear),
                    poster: buildTop10ImageUrl(content.posterUrl),
                    background: null,
                    imdbRating: null,
                    rank: Number.parseInt(String(chartInfo && chartInfo.rank || ""), 10) || (index + 1),
                    trend: chartInfo && chartInfo.trend ? String(chartInfo.trend) : null,
                    trendDifference: Number.parseInt(String(chartInfo && chartInfo.trendDifference || ""), 10) || 0,
                    topRank: Number.parseInt(String(chartInfo && chartInfo.topRank || ""), 10) || null
                };
            })
            .filter(Boolean);

        if (response.ok && entries.length > 0) {
            await cache.set(cacheKey, entries, withTtlJitter(CACHE_TTL_SECONDS.top10));
            return entries;
        }
    } catch (error) {
        // Fall through to negative cache below.
    }

    await cache.set(cacheKey, createNegativeCache("top10_graphql_chart_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
    return [];
}

async function fetchTop10ChartEntriesFromPage(pageUrl, options = {}) {
    const objectType = String(options.objectType || "").trim().toUpperCase();
    const requiresPackages = options.requiresPackages === true;
    if (!pageUrl || !objectType) return [];

    const cacheKey = `top10:charts:v1:${requiresPackages ? "provider" : "global"}:${objectType}:${pageUrl}`;
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return [];
    if (cached) return cached;

    try {
        const response = await fetch(pageUrl, {
            headers: {
                Accept: "text/html,application/xhtml+xml",
                "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
            }
        });
        const html = await response.text();
        const state = extractTop10ApolloState(html);
        const entries = extractTop10ChartEntries(state, { objectType, requiresPackages });

        if (response.ok && entries.length > 0) {
            await cache.set(cacheKey, entries, withTtlJitter(CACHE_TTL_SECONDS.top10));
            return entries;
        }
    } catch (error) {
        // Fall through to negative cache below.
    }

    await cache.set(cacheKey, createNegativeCache("top10_chart_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
    return [];
}

function getTmdbSearchCandidatesFromResult(result) {
    return uniqueNonEmptyStrings([
        result && result.title,
        result && result.name,
        result && result.original_title,
        result && result.original_name
    ]);
}

function scoreTmdbSearchResult(result, query, expectedYear) {
    if (!result) return 0;

    const normalizedQuery = normalizeMatchTitle(query);
    if (!normalizedQuery) return 0;

    const titleCandidates = getTmdbSearchCandidatesFromResult(result).map(normalizeMatchTitle);
    if (titleCandidates.length === 0) return 0;

    let score = 0;
    if (titleCandidates.includes(normalizedQuery)) {
        score += 100;
    } else if (titleCandidates.some(title => title.startsWith(normalizedQuery) || normalizedQuery.startsWith(title))) {
        score += 75;
    } else {
        const queryTokens = normalizedQuery.split(" ").filter(Boolean);
        const bestOverlap = titleCandidates.reduce((bestScore, candidate) => {
            const candidateTokens = candidate.split(" ").filter(Boolean);
            const sharedCount = queryTokens.filter(token => candidateTokens.includes(token)).length;
            const overlapScore = queryTokens.length > 0 && candidateTokens.length > 0
                ? (sharedCount / Math.max(queryTokens.length, candidateTokens.length)) * 50
                : 0;
            return Math.max(bestScore, overlapScore);
        }, 0);
        score += bestOverlap;
    }

    const resultYear = getYearFromValue(result.release_date || result.first_air_date);
    if (Number.isFinite(expectedYear) && resultYear) {
        if (resultYear === expectedYear) {
            score += 30;
        } else if (Math.abs(resultYear - expectedYear) === 1) {
            score += 10;
        }
    }

    score += Math.min(Number(result.popularity || 0) / 100, 5);
    score += Math.min(Number(result.vote_count || 0) / 200, 5);
    return score;
}

async function searchTmdbByTitleCandidate(typePath, query, expectedYear = null, config = null) {
    const normalizedQuery = String(query || "").trim();
    if (!normalizedQuery) return null;

    const yearQuery = Number.isFinite(expectedYear)
        ? `&${typePath === "movie" ? "year" : "first_air_date_year"}=${expectedYear}`
        : "";

    try {
        const response = await fetch(
            `${BASE_URL}/search/${typePath}?api_key=${getTmdbApiKey(config)}&query=${encodeURIComponent(normalizedQuery)}&language=it-IT${yearQuery}`
        );
        const payload = await response.json();
        const results = Array.isArray(payload && payload.results) ? payload.results : [];
        if (results.length === 0) return null;

        let bestMatch = null;
        let bestScore = 0;
        for (const result of results.slice(0, 10)) {
            const currentScore = scoreTmdbSearchResult(result, normalizedQuery, expectedYear);
            if (currentScore > bestScore) {
                bestScore = currentScore;
                bestMatch = result;
            }
        }

        return bestScore >= 60 ? bestMatch : null;
    } catch (error) {
        return null;
    }
}

async function resolveTmdbIdFromTop10Entry(entry, requestedType, config = null) {
    const expectedYear = getYearFromValue(entry && entry.year);
    const slugTitle = getTitleFromSlugPath(entry && entry.fullPath);
    const titleCandidates = uniqueNonEmptyStrings([
        entry && entry.title,
        slugTitle
    ]);
    if (titleCandidates.length === 0) return null;

    const cacheKey = `top10:tmdb-lookup:v1:${requestedType}:${normalizeMatchTitle(titleCandidates[0])}:${expectedYear || ""}:${normalizeMatchTitle(slugTitle)}`;
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return null;
    if (cached && cached.tmdbId) return cached.tmdbId;

    const typePath = requestedType === "series" ? "tv" : "movie";

    for (const candidate of titleCandidates) {
        let bestMatch = await searchTmdbByTitleCandidate(typePath, candidate, expectedYear, config);
        if (!bestMatch && Number.isFinite(expectedYear)) {
            bestMatch = await searchTmdbByTitleCandidate(typePath, candidate, null, config);
        }

        const tmdbId = bestMatch && bestMatch.id ? String(bestMatch.id) : null;
        if (tmdbId) {
            await cache.set(cacheKey, { tmdbId }, withTtlJitter(CACHE_TTL_SECONDS.top10));
            return tmdbId;
        }
    }

    await cache.set(cacheKey, createNegativeCache("top10_tmdb_lookup_failed"), NEGATIVE_CACHE_TTL_SECONDS);
    return null;
}

function buildFallbackTop10CatalogMeta(entry, requestedType, tmdbId, config = null) {
    const resolvedConfig = getRequestConfig(config);
    const metaId = `tmdb:${tmdbId}`;
    const configuredPosterUrl = getConfiguredAssetUrl(resolvedConfig, "poster", null, tmdbId, metaId, requestedType);
    const configuredBackdropUrl = getConfiguredAssetUrl(resolvedConfig, "backdrop", null, tmdbId, metaId, requestedType);

    return {
        id: metaId,
        type: requestedType,
        name: entry.title,
        poster: configuredPosterUrl || entry.poster || undefined,
        background: configuredBackdropUrl || entry.background || undefined,
        description: entry.rank ? `Top 10 Italia #${entry.rank}` : undefined,
        releaseInfo: entry.year ? String(entry.year) : undefined,
        year: entry.year || undefined,
        imdbRating: entry.imdbRating ? String(entry.imdbRating) : null,
        behaviorHints: {
            defaultVideoId: requestedType === "movie" ? metaId : null,
            hasScheduledVideos: requestedType === "series"
        }
    };
}

async function mapTop10EntryToMeta(entry, requestedType, config = null) {
    const tmdbId = await resolveTmdbIdFromTop10Entry(entry, requestedType, config);
    if (!tmdbId) return null;

    const details = await fetchTmdbDetails(requestedType === "series" ? "tv" : "movie", tmdbId, config);
    if (details) {
        const meta = await transformToMeta(details, requestedType, config, { includeVideos: false });
        if (meta) {
            meta.textBackdrop = getTextBackdropFromDetails(details) || undefined;
        }
        return meta;
    }

    return buildFallbackTop10CatalogMeta(entry, requestedType, tmdbId, config);
}

async function fetchTop10CatalogMetas(catalogId, requestedType, extra = {}, config = null) {
    const normalizedCatalogId = normalizeTop10ManifestId(catalogId);
    if (!normalizedCatalogId) return [];

    const skipValue = Number.parseInt(String(extra && extra.skip || "0"), 10);
    if (Number.isFinite(skipValue) && skipValue > 0) return [];

    const configHash = config && typeof config === "object" && Object.keys(config).length > 0
        ? JSON.stringify(config)
        : "default";
    const cacheKey = `top10:catalog:v2:${normalizedCatalogId}:${requestedType}:${configHash}`;
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return [];
    if (cached) return cached;

    const objectType = requestedType === "series" ? "SHOW" : "MOVIE";
    let pageUrl = `${TOP10_SOURCE_BASE_URL}/it/streaming-charts`;
    let requiresPackages = false;

    const providerMatch = normalizedCatalogId.match(/^t10\.(movie|series)\.([a-z0-9]+)_top10$/i);
    if (providerMatch) {
        const providerSlug = providerMatch[2].toLowerCase();
        const providerPageSlug = TOP10_PROVIDER_PAGE_SLUGS[providerSlug];
        if (!providerPageSlug) {
            await cache.set(cacheKey, createNegativeCache("top10_provider_not_supported"), NEGATIVE_CACHE_TTL_SECONDS);
            return [];
        }

        pageUrl = `${TOP10_SOURCE_BASE_URL}/it/provider/${providerPageSlug}`;
        requiresPackages = true;
    }

    const entries = providerMatch
        ? await fetchTop10ChartEntriesFromPage(pageUrl, { objectType, requiresPackages })
        : await fetchTop10ChartEntriesFromGraphql({ objectType });
    if (entries.length === 0) {
        await cache.set(cacheKey, createNegativeCache("top10_catalog_empty"), NEGATIVE_CACHE_TTL_SECONDS);
        return [];
    }

    const metas = (await mapWithConcurrency(
        entries,
        2,
        entry => mapTop10EntryToMeta(entry, requestedType, config)
    )).filter(Boolean);

    if (metas.length > 0) {
        await cache.set(cacheKey, metas, withTtlJitter(CACHE_TTL_SECONDS.top10));
        return metas;
    }

    await cache.set(cacheKey, createNegativeCache("top10_catalog_map_failed"), NEGATIVE_CACHE_TTL_SECONDS);
    return [];
}

function inferStremioTypeFromKitsuSubtype(subtype, fallback = "series") {
    return String(subtype || "").trim().toLowerCase() === "movie" ? "movie" : fallback;
}

function getKitsuTitleCandidates(attributes = {}) {
    const titlesObject = attributes.titles && typeof attributes.titles === "object"
        ? attributes.titles
        : {};
    const abbreviatedTitles = Array.isArray(attributes.abbreviatedTitles)
        ? attributes.abbreviatedTitles
        : [];

    return uniqueNonEmptyStrings([
        attributes.canonicalTitle,
        attributes.slug ? String(attributes.slug).replace(/-/g, " ") : null,
        ...Object.values(titlesObject),
        ...abbreviatedTitles
    ]);
}

function getKitsuPreferredTitle(attributes = {}, config = null, options = {}) {
    const titlesObject = attributes.titles && typeof attributes.titles === "object"
        ? attributes.titles
        : {};
    const includeSlug = options.includeSlug !== false;

    return uniqueNonEmptyStrings([
        titlesObject.en,
        titlesObject.en_us,
        attributes.canonicalTitle,
        attributes.title,
        titlesObject.en_jp,
        titlesObject.ja_jp,
        titlesObject.ja,
        ...(includeSlug && attributes.slug ? [String(attributes.slug).replace(/-/g, " ")] : [])
    ])[0] || null;
}

function isDeletedKitsuAttributes(attributes = {}) {
    if (!attributes || typeof attributes !== "object") return false;

    const slug = String(attributes.slug || "").trim().toLowerCase();
    const titleCandidates = uniqueNonEmptyStrings([
        attributes.canonicalTitle,
        attributes.title,
        ...(attributes.titles && typeof attributes.titles === "object" ? Object.values(attributes.titles) : [])
    ])
        .map(normalizeMatchTitle)
        .filter(Boolean);
    const synopsisCandidates = uniqueNonEmptyStrings([
        attributes.synopsis,
        attributes.description
    ])
        .map(normalizeMatchTitle)
        .filter(Boolean);

    if (slug.startsWith("deleted-")) {
        return true;
    }

    return titleCandidates.length > 0 &&
        titleCandidates.every(title => title === "deleted") &&
        synopsisCandidates.length > 0 &&
        synopsisCandidates.every(text => text === "deleted page");
}

function isDeletedKitsuPayload(payload) {
    const attributes = payload && payload.data && payload.data.attributes
        ? payload.data.attributes
        : null;
    return isDeletedKitsuAttributes(attributes);
}

function getKitsuPoster(attributes = {}) {
    const poster = attributes.posterImage || {};
    return poster.large || poster.original || poster.medium || poster.small || null;
}

function getKitsuBackground(attributes = {}) {
    const cover = attributes.coverImage || {};
    return cover.original || cover.large || cover.medium || null;
}

function getKitsuCategories(payload) {
    const included = Array.isArray(payload && payload.included) ? payload.included : [];

    return included
        .filter(entry => {
            const type = String(entry && entry.type || "").trim().toLowerCase();
            return type === "categories" && entry.attributes && entry.attributes.title;
        })
        .map(entry => entry.attributes.title)
        .sort();
}

function getKitsuFranchiseLinks(payload, config = null) {
    const mainItem = payload && payload.data ? payload.data : null;
    const included = Array.isArray(payload && payload.included) ? payload.included : [];
    if (!mainItem || !mainItem.relationships || !mainItem.relationships.mediaRelationships || included.length === 0) {
        return [];
    }

    const roleMap = {
        sequel: "Sequel",
        prequel: "Prequel",
        parent_story: "Parent Story",
        side_story: "Side Story",
        spinoff: "Spinoff",
        alternative_setting: "Alt. Setting",
        alternative_version: "Alt. Version"
    };
    const order = ["Parent Story", "Prequel", "Sequel", "Side Story", "Spinoff", "Alt. Setting", "Alt. Version"];

    const franchiseItems = included
        .filter(entry => {
            const type = String(entry && entry.type || "").trim().toLowerCase();
            return type === "mediarelationships" && entry.attributes && entry.relationships;
        })
        .map(entry => {
            const roleKey = String(entry.attributes.role || "").trim().toLowerCase();
            const role = roleMap[roleKey];
            if (!role) return null;

            const destination = entry.relationships &&
                entry.relationships.destination &&
                entry.relationships.destination.data
                ? entry.relationships.destination.data
                : null;
            if (!destination) return null;

            const destinationItem = included.find(candidate =>
                String(candidate && candidate.type || "").trim().toLowerCase() === String(destination.type || "").trim().toLowerCase() &&
                String(candidate && candidate.id || "") === String(destination.id || "")
            );
            if (!destinationItem || !destinationItem.attributes) return null;

            const attributes = destinationItem.attributes || {};
            const relatedType = inferStremioTypeFromKitsuSubtype(attributes.subtype, "series");
            const relatedId = `kitsu:${destinationItem.id}`;
            const relatedTitle = getKitsuPreferredTitle(attributes, config) || `Kitsu ${destinationItem.id}`;
            const relatedYear = attributes.startDate ? String(attributes.startDate).split("-")[0] : null;

            return {
                role,
                title: relatedTitle,
                year: relatedYear,
                type: relatedType,
                id: relatedId
            };
        })
        .filter(Boolean)
        .sort((a, b) => order.indexOf(a.role) - order.indexOf(b.role));

    const seen = new Set();
    return franchiseItems
        .filter(item => {
            const key = `${item.role}:${item.id}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        })
        .map(item => ({
            name: `${item.role}: ${item.title}${item.year ? ` (${item.year})` : ""}`,
            category: "Franchise",
            url: `stremio:///detail/${item.type}/${encodeURIComponent(item.id)}`
        }));
}

async function fetchAnimeMappingPayload(kitsuId, episodeNumber = null) {
    const normalizedKitsuId = normalizeKitsuId(kitsuId);
    if (!normalizedKitsuId) return null;

    const cleanEpisodeNumber = Number.isFinite(Number(episodeNumber))
        ? Number.parseInt(String(episodeNumber), 10)
        : null;
    const suffix = cleanEpisodeNumber && cleanEpisodeNumber > 0 ? `?ep=${cleanEpisodeNumber}` : "";
    const cacheKey = `animemapping:kitsu:${normalizedKitsuId}${suffix}`;
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return null;
    if (cached) return cached;

    try {
        const response = await fetch(`${ANIME_MAPPING_BASE_URL}/kitsu/${encodeURIComponent(normalizedKitsuId)}${suffix}`, {
            headers: {
                Accept: "application/json"
            }
        });
        const payload = await response.json();

        if (response.ok && payload && !payload.error) {
            await cache.set(cacheKey, payload, withTtlJitter(KITSU_TTL_SECONDS.animeMapping));
            return payload;
        }
    } catch (error) {
        // Fall through to negative cache below.
    }

    await cache.set(cacheKey, createNegativeCache("anime_mapping_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
    return null;
}

function getAnimeMappingIds(payload) {
    const ids = payload &&
        payload.mappings &&
        payload.mappings.ids &&
        typeof payload.mappings.ids === "object"
        ? payload.mappings.ids
        : {};

    return {
        malId: ids.mal ? String(ids.mal).trim() : null,
        anilistId: ids.anilist ? String(ids.anilist).trim() : null,
        imdbId: normalizeImdbId(ids.imdb),
        tmdbId: ids.tmdb ? String(ids.tmdb).trim() : null,
        tvdbId: ids.tvdb ? String(ids.tvdb).trim() : null,
        anidbId: ids.anidb ? String(ids.anidb).trim() : null
    };
}

function getAnimeMappingTmdbEpisode(payload) {
    if (!payload || typeof payload !== "object") return null;

    const nestedTmdbEpisode = payload.mappings && typeof payload.mappings === "object"
        ? payload.mappings.tmdb_episode
        : null;
    const directTmdbEpisode = payload.tmdb_episode;
    const tmdbEpisode = nestedTmdbEpisode || directTmdbEpisode;

    return tmdbEpisode && typeof tmdbEpisode === "object"
        ? tmdbEpisode
        : null;
}

async function fetchTmdbSeasonDetails(tmdbSeriesId, seasonNumber, config = null) {
    const cleanSeriesId = String(tmdbSeriesId || "").trim();
    const cleanSeasonNumber = Number.parseInt(String(seasonNumber || ""), 10);
    if (!cleanSeriesId || !Number.isFinite(cleanSeasonNumber)) return null;

    const cacheKey = `tmdb:season:tv:${cleanSeriesId}:${cleanSeasonNumber}`;
    let cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return null;
    if (cached) return cached;

    try {
        const response = await fetch(
            `${BASE_URL}/tv/${cleanSeriesId}/season/${cleanSeasonNumber}?api_key=${getTmdbApiKey(config)}&language=it-IT&append_to_response=images&include_image_language=it,en,null`
        );
        const payload = await response.json();

        if (response.ok && payload && !payload.status_message) {
            await cache.set(cacheKey, payload, withTtlJitter(CACHE_TTL_SECONDS.detailsSeries));
            return payload;
        }
    } catch (error) {
        // Fall through to negative cache below.
    }

    await cache.set(cacheKey, createNegativeCache("tmdb_season_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
    return null;
}

async function fetchTmdbSeasonImages(tmdbSeriesId, seasonNumber, config = null) {
    const cleanSeriesId = String(tmdbSeriesId || "").trim();
    const cleanSeasonNumber = Number.parseInt(String(seasonNumber || ""), 10);
    if (!cleanSeriesId || !Number.isFinite(cleanSeasonNumber)) return null;

    const cacheKey = `tmdb:season-images:tv:${cleanSeriesId}:${cleanSeasonNumber}`;
    let cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return null;
    if (cached) return cached;

    try {
        const response = await fetch(
            `${BASE_URL}/tv/${cleanSeriesId}/season/${cleanSeasonNumber}/images?api_key=${getTmdbApiKey(config)}&include_image_language=it,en,null`
        );
        const payload = await response.json();

        if (response.ok && payload && !payload.status_message) {
            await cache.set(cacheKey, payload, withTtlJitter(CACHE_TTL_SECONDS.detailsSeries));
            return payload;
        }
    } catch (error) {
        // Fall through to negative cache below.
    }

    await cache.set(cacheKey, createNegativeCache("tmdb_season_images_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
    return null;
}

function getPreferredTmdbPosterUrl(payload) {
    if (!payload || typeof payload !== "object") return null;

    if (payload.poster_path) {
        return `https://image.tmdb.org/t/p/w500${payload.poster_path}`;
    }

    const posters = Array.isArray(payload.posters)
        ? payload.posters
        : (payload.images && Array.isArray(payload.images.posters)
            ? payload.images.posters
            : []);
    if (posters.length === 0) return null;

    const preferredPoster =
        posters.find(entry => entry && entry.iso_639_1 === "it") ||
        posters.find(entry => entry && entry.iso_639_1 === "en") ||
        posters.find(entry => entry && entry.iso_639_1 === null) ||
        posters[0];

    return preferredPoster && preferredPoster.file_path
        ? `https://image.tmdb.org/t/p/w500${preferredPoster.file_path}`
        : null;
}

function findTmdbEpisodeInSeason(seasonPayload, mapping) {
    const episodes = seasonPayload && Array.isArray(seasonPayload.episodes)
        ? seasonPayload.episodes
        : [];
    if (episodes.length === 0 || !mapping || typeof mapping !== "object") return null;

    const candidateNumbers = [
        mapping.episode,
        mapping.rawEpisodeNumber,
        mapping.absoluteEpisode
    ]
        .map(value => Number.parseInt(String(value || ""), 10))
        .filter(value => Number.isFinite(value));
    const uniqueCandidateNumbers = [...new Set(candidateNumbers)];

    for (const candidateNumber of uniqueCandidateNumbers) {
        const matchedEpisode = episodes.find(entry =>
            Number.parseInt(String(entry && entry.episode_number || ""), 10) === candidateNumber
        );
        if (matchedEpisode) return matchedEpisode;
    }

    if (mapping.airDate) {
        const matchedByAirDate = episodes.find(entry => String(entry && entry.air_date || "") === mapping.airDate);
        if (matchedByAirDate) return matchedByAirDate;
    }

    return null;
}

async function mapWithConcurrency(items, limit, mapper) {
    const source = Array.isArray(items) ? items : [];
    const concurrency = Math.max(1, Number.parseInt(String(limit || ""), 10) || 1);
    const results = new Array(source.length);
    let currentIndex = 0;

    const workers = Array.from({ length: Math.min(concurrency, source.length) }, async () => {
        while (true) {
            const index = currentIndex;
            currentIndex += 1;
            if (index >= source.length) break;

            try {
                results[index] = await mapper(source[index], index);
            } catch (error) {
                results[index] = null;
            }
        }
    });

    await Promise.all(workers);
    return results;
}

function extractKitsuMappings(payload) {
    const out = {
        imdbId: null,
        tmdbId: null,
        tmdbType: null,
        tvdbId: null,
        malId: null,
        anilistId: null
    };

    const included = Array.isArray(payload && payload.included) ? payload.included : [];
    for (const entry of included) {
        const type = String(entry && entry.type || "").trim().toLowerCase();
        if (type !== "mapping" && type !== "mappings") continue;

        const attributes = entry.attributes || {};
        const site = String(attributes.externalSite || "").trim().toLowerCase();
        const externalId = String(attributes.externalId || "").trim();
        if (!externalId) continue;

        if (!out.malId && (site === "myanimelist/anime" || site === "mal")) {
            out.malId = externalId;
            continue;
        }

        if (!out.anilistId && (site === "anilist/anime" || site === "anilist")) {
            out.anilistId = externalId;
            continue;
        }

        if (!out.imdbId && (site === "imdb/anime" || site === "imdb/movie")) {
            out.imdbId = normalizeImdbId(externalId) || externalId;
            continue;
        }

        if (!out.tmdbId && (site === "themoviedb/anime" || site === "themoviedb/movie" || site === "themoviedb/tv")) {
            out.tmdbId = externalId;
            if (site.endsWith("/movie")) out.tmdbType = "movie";
            if (site.endsWith("/tv")) out.tmdbType = "tv";
            continue;
        }

        if (!out.tvdbId && (site === "thetvdb/anime" || site === "thetvdb/series")) {
            out.tvdbId = externalId;
        }
    }

    return out;
}

async function fetchTmdbDetails(typePath, tmdbId, config = null) {
    if (!tmdbId) return null;

    const resolvedConfig = getRequestConfig(config);
    const cacheKey = `tmdb:details:${typePath}:${tmdbId}`;
    let details = await cache.get(cacheKey);
    if (isNegativeCache(details)) return null;

    if (!details) {
        const detailsUrl = `${BASE_URL}/${typePath}/${tmdbId}?api_key=${getTmdbApiKey(resolvedConfig)}&language=it-IT&append_to_response=external_ids,credits,similar,videos,images,release_dates&include_image_language=it,en,null&include_video_language=it,en,null`;

        try {
            const detailsRes = await fetch(detailsUrl);
            details = await detailsRes.json();
        } catch (error) {
            details = null;
        }

        if (details && !details.status_message) {
            const detailsTtl = typePath === "movie" ? CACHE_TTL_SECONDS.detailsMovie : CACHE_TTL_SECONDS.detailsSeries;
            await cache.set(cacheKey, details, withTtlJitter(detailsTtl));
        } else {
            await cache.set(cacheKey, createNegativeCache("details_fetch_failed"), NEGATIVE_CACHE_TTL_SECONDS);
            return null;
        }
    }

    return details;
}

async function resolveTmdbIdFromImdb(imdbId, requestedType, config = null) {
    const normalizedImdbId = normalizeImdbId(imdbId);
    if (!normalizedImdbId) return null;

    const cacheKey = `tmdb:find:imdb:${requestedType}:${normalizedImdbId}`;
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return null;
    if (cached && cached.tmdbId) return cached.tmdbId;

    try {
        const response = await fetch(`${BASE_URL}/find/${normalizedImdbId}?api_key=${getTmdbApiKey(config)}&external_source=imdb_id`);
        const payload = await response.json();
        const results = requestedType === "series" ? payload.tv_results : payload.movie_results;
        const tmdbId = Array.isArray(results) && results[0] && results[0].id
            ? String(results[0].id)
            : null;

        if (tmdbId) {
            await cache.set(cacheKey, { tmdbId }, withTtlJitter(CACHE_TTL_SECONDS.detailsSeries));
            return tmdbId;
        }
    } catch (error) {
        // Fall through to negative cache below.
    }

    await cache.set(cacheKey, createNegativeCache("tmdb_find_imdb_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
    return null;
}

async function fetchValidatedAnimeTmdbDetails(typePath, tmdbId, config = null) {
    const details = await fetchTmdbDetails(typePath, tmdbId, config);
    if (!details) {
        return { details: null, rejectedNonAnime: false };
    }

    if (isTmdbAnimeDetails(details)) {
        return { details, rejectedNonAnime: false };
    }

    return { details: null, rejectedNonAnime: true };
}

async function fetchKitsuAnimeById(kitsuId) {
    const normalizedKitsuId = normalizeKitsuId(kitsuId);
    if (!normalizedKitsuId) return null;

    const cacheKey = `kitsu:anime:v2:${normalizedKitsuId}`;
    let cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return null;
    if (cached) {
        if (isDeletedKitsuPayload(cached)) {
            await cache.set(cacheKey, createNegativeCache("kitsu_anime_deleted"), NEGATIVE_CACHE_TTL_SECONDS);
            return null;
        }
        return cached;
    }

    try {
        const response = await fetch(`${KITSU_BASE_URL}/anime/${encodeURIComponent(normalizedKitsuId)}?include=mediaRelationships.destination,categories,mappings`, {
            headers: {
                Accept: "application/vnd.api+json, application/json"
            }
        });
        const payload = await response.json();

        if (response.ok && payload && payload.data && !payload.errors && !isDeletedKitsuPayload(payload)) {
            await cache.set(cacheKey, payload, withTtlJitter(KITSU_TTL_SECONDS.anime));
            return payload;
        }
    } catch (error) {
        // Fall through to negative cache below.
    }

    await cache.set(cacheKey, createNegativeCache("kitsu_anime_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
    return null;
}

async function fetchKitsuSearchResults(query) {
    const cleanQuery = String(query || "").trim();
    if (!cleanQuery) return null;

    const cacheKey = `kitsu:search:${normalizeMatchTitle(cleanQuery)}`;
    let cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return null;
    if (cached) return cached;

    try {
        const response = await fetch(`${KITSU_BASE_URL}/anime?filter[text]=${encodeURIComponent(cleanQuery)}&page[limit]=6`, {
            headers: {
                Accept: "application/vnd.api+json, application/json"
            }
        });
        const payload = await response.json();

        if (response.ok && payload && !payload.errors) {
            await cache.set(cacheKey, payload, withTtlJitter(KITSU_TTL_SECONDS.search));
            return payload;
        }
    } catch (error) {
        // Fall through to negative cache below.
    }

    await cache.set(cacheKey, createNegativeCache("kitsu_search_failed"), NEGATIVE_CACHE_TTL_SECONDS);
    return null;
}

async function fetchKitsuEpisodes(kitsuId) {
    const normalizedKitsuId = normalizeKitsuId(kitsuId);
    if (!normalizedKitsuId) return [];

    const cacheKey = `kitsu:episodes:${normalizedKitsuId}`;
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return [];
    if (cached) return cached;

    const episodes = [];
    let offset = 0;
    let hasNext = true;
    let pageSafetyCounter = 0;

    try {
        while (hasNext && pageSafetyCounter < 50) {
            const response = await fetch(`${KITSU_BASE_URL}/anime/${encodeURIComponent(normalizedKitsuId)}/episodes?page[limit]=20&page[offset]=${offset}&sort=number`, {
                headers: {
                    Accept: "application/vnd.api+json, application/json"
                }
            });
            const payload = await response.json();
            const data = Array.isArray(payload && payload.data) ? payload.data : [];

            if (!response.ok || payload.errors) {
                throw new Error(`Kitsu episodes request failed for ${normalizedKitsuId}`);
            }

            if (data.length === 0) {
                hasNext = false;
                break;
            }

            episodes.push(...data);
            hasNext = !!(payload.links && payload.links.next);
            offset += data.length;
            pageSafetyCounter += 1;
        }

        await cache.set(cacheKey, episodes, withTtlJitter(KITSU_TTL_SECONDS.episodes));
        return episodes;
    } catch (error) {
        await cache.set(cacheKey, createNegativeCache("kitsu_episodes_fetch_failed"), NEGATIVE_CACHE_TTL_SECONDS);
        return [];
    }
}

async function resolveKitsuFromExternalSite(externalSite, externalId) {
    const site = String(externalSite || "").trim().toLowerCase();
    const cleanExternalId = String(externalId || "").trim();
    if (!site || !cleanExternalId) return null;

    const cacheKey = `kitsu:external:${site}:${cleanExternalId}`;
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return null;
    if (cached && cached.kitsuId) return cached.kitsuId;

    try {
        const response = await fetch(
            `${KITSU_BASE_URL}/mappings?filter[externalSite]=${encodeURIComponent(site)}&filter[externalId]=${encodeURIComponent(cleanExternalId)}&include=item&page[limit]=1`,
            {
                headers: {
                    Accept: "application/vnd.api+json, application/json"
                }
            }
        );
        const payload = await response.json();

        const relatedId = payload && payload.data && payload.data[0] && payload.data[0].relationships &&
            payload.data[0].relationships.item && payload.data[0].relationships.item.data
            ? payload.data[0].relationships.item.data.id
            : null;
        const includedId = Array.isArray(payload && payload.included)
            ? (payload.included.find(entry => String(entry && entry.type || "").trim().toLowerCase() === "anime") || {}).id
            : null;
        const kitsuId = relatedId || includedId || null;

        if (response.ok && kitsuId) {
            await cache.set(cacheKey, { kitsuId: String(kitsuId) }, withTtlJitter(KITSU_TTL_SECONDS.externalLookup));
            return String(kitsuId);
        }
    } catch (error) {
        // Fall through to negative cache below.
    }

    await cache.set(cacheKey, createNegativeCache("kitsu_external_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
    return null;
}

function scoreKitsuSearchCandidate(entry, info = {}) {
    const attributes = entry && entry.attributes ? entry.attributes : {};
    const expectedTitles = uniqueNonEmptyStrings([
        info.originalTitle,
        info.title
    ]).map(normalizeMatchTitle).filter(Boolean);
    const candidateTitles = getKitsuTitleCandidates(attributes).map(normalizeMatchTitle).filter(Boolean);

    if (expectedTitles.length === 0 || candidateTitles.length === 0) return -1;

    let score = 0;
    if (candidateTitles.some(title => expectedTitles.includes(title))) {
        score += 120;
    } else if (candidateTitles.some(title => expectedTitles.some(expected => title.startsWith(expected) || expected.startsWith(title)))) {
        score += 90;
    } else if (candidateTitles.some(title => expectedTitles.some(expected => title.includes(expected) || expected.includes(title)))) {
        score += 60;
    } else {
        score += 10;
    }

    const expectedYear = getYearFromValue(info.year);
    const candidateYear = getYearFromValue(attributes.startDate);
    if (expectedYear && candidateYear) {
        if (candidateYear === expectedYear) score += 20;
        else if (Math.abs(candidateYear - expectedYear) === 1) score += 10;
        else score -= 10;
    }

    if (info.requestedType) {
        const candidateType = inferStremioTypeFromKitsuSubtype(attributes.subtype, "series");
        if (candidateType === info.requestedType) score += 15;
        else score -= 15;
    }

    const expectedEpisodes = Number.parseInt(String(info.episodeCount || ""), 10);
    const candidateEpisodes = Number.parseInt(String(attributes.episodeCount || ""), 10);
    if (Number.isFinite(expectedEpisodes) && expectedEpisodes > 0 && Number.isFinite(candidateEpisodes) && candidateEpisodes > 0) {
        if (candidateEpisodes === expectedEpisodes) score += 15;
        else if (Math.abs(candidateEpisodes - expectedEpisodes) <= 2) score += 8;
        else score -= 8;
    }

    return score;
}

async function resolveKitsuByTitle(info = {}) {
    const queries = uniqueNonEmptyStrings([
        info.originalTitle,
        info.title
    ]);

    let bestId = null;
    let bestScore = -1;

    for (const query of queries) {
        const payload = await fetchKitsuSearchResults(query);
        const candidates = Array.isArray(payload && payload.data) ? payload.data : [];

        for (const candidate of candidates) {
            const score = scoreKitsuSearchCandidate(candidate, info);
            if (score > bestScore) {
                bestScore = score;
                bestId = candidate && candidate.id ? String(candidate.id) : null;
            }
        }

        if (bestScore >= 120) break;
    }

    return bestScore >= 60 ? bestId : null;
}

async function resolveKitsuIdForAnimeMedia(item, details, stremioType) {
    const tmdbType = stremioType === "series" ? "tv" : "movie";
    const tmdbId = details && details.id ? String(details.id) : (item && item.id ? String(item.id) : null);
    const externalIds = details && details.external_ids ? details.external_ids : {};
    const lookupCandidates = [];

    if (tmdbId) {
        if (tmdbType === "movie") {
            lookupCandidates.push(["themoviedb/movie", tmdbId], ["themoviedb/anime", tmdbId]);
        } else {
            lookupCandidates.push(["themoviedb/tv", tmdbId], ["themoviedb/anime", tmdbId]);
        }
    }

    const imdbId = normalizeImdbId(externalIds.imdb_id);
    if (imdbId) {
        lookupCandidates.push(["imdb/movie", imdbId], ["imdb/anime", imdbId]);
        if (/^tt\d+$/i.test(imdbId)) {
            const numericImdbId = imdbId.slice(2);
            lookupCandidates.push(["imdb/movie", numericImdbId], ["imdb/anime", numericImdbId]);
        }
    }

    const tvdbId = externalIds.tvdb_id ? String(externalIds.tvdb_id).trim() : "";
    if (tvdbId) {
        lookupCandidates.push(["thetvdb/series", tvdbId], ["thetvdb/anime", tvdbId]);
    }

    for (const [site, externalId] of lookupCandidates) {
        const kitsuId = await resolveKitsuFromExternalSite(site, externalId);
        if (kitsuId) return kitsuId;
    }

    return resolveKitsuByTitle({
        title: details && (details.title || details.name) ? (details.title || details.name) : (item ? (item.title || item.name) : null),
        originalTitle: details && (details.original_title || details.original_name) ? (details.original_title || details.original_name) : (item ? (item.original_title || item.original_name) : null),
        year: details && (details.release_date || details.first_air_date) ? (details.release_date || details.first_air_date) : (item ? (item.release_date || item.first_air_date) : null),
        episodeCount: details && details.number_of_episodes ? details.number_of_episodes : null,
        requestedType: stremioType
    });
}

async function resolveTmdbDetailsFromKitsuPayload(payload, requestedType, config = null) {
    const mappings = extractKitsuMappings(payload);
    const animeMappingPayload = await fetchAnimeMappingPayload(payload && payload.data ? payload.data.id : null);
    const animeMappingIds = getAnimeMappingIds(animeMappingPayload);
    const requestedTypePath = requestedType === "series" ? "tv" : "movie";
    let rejectedNonAnime = false;

    const tryTmdbCandidate = async (typePath, tmdbId) => {
        const result = await fetchValidatedAnimeTmdbDetails(typePath, tmdbId, config);
        if (result.rejectedNonAnime) {
            rejectedNonAnime = true;
        }
        return result.details;
    };

    if (animeMappingIds.tmdbId) {
        const details = await tryTmdbCandidate(requestedTypePath, animeMappingIds.tmdbId);
        if (details) return { details, rejectedNonAnime };
    }

    if (mappings.tmdbId) {
        const preferredTypePath = mappings.tmdbType || requestedTypePath;
        let details = await tryTmdbCandidate(preferredTypePath, mappings.tmdbId);
        if (!details && preferredTypePath !== requestedTypePath) {
            details = await tryTmdbCandidate(requestedTypePath, mappings.tmdbId);
        }
        if (details) return { details, rejectedNonAnime };
    }

    if (animeMappingIds.imdbId) {
        const tmdbId = await resolveTmdbIdFromImdb(animeMappingIds.imdbId, requestedType, config);
        if (tmdbId) {
            const details = await tryTmdbCandidate(requestedTypePath, tmdbId);
            if (details) return { details, rejectedNonAnime };
        }
    }

    if (mappings.imdbId) {
        const tmdbId = await resolveTmdbIdFromImdb(mappings.imdbId, requestedType, config);
        if (tmdbId) {
            const details = await tryTmdbCandidate(requestedTypePath, tmdbId);
            if (details) return { details, rejectedNonAnime };
        }
    }

    return { details: null, rejectedNonAnime };
}

function buildKitsuEpisodeVideos(kitsuId, episodes, fallbackEpisodeCount = 0, config = null) {
    const normalizedKitsuId = normalizeKitsuId(kitsuId);
    if (!normalizedKitsuId) return [];

    const seenEpisodes = new Set();
    let normalizedEpisodes = Array.isArray(episodes)
        ? episodes
            .map(entry => {
                const attributes = entry && entry.attributes ? entry.attributes : entry || {};
                const episodeNumber = Number.parseInt(String(attributes.number || attributes.episode || ""), 10);
                if (!Number.isFinite(episodeNumber) || episodeNumber <= 0 || seenEpisodes.has(episodeNumber)) return null;
                seenEpisodes.add(episodeNumber);

                const thumbnail = attributes.thumbnail && typeof attributes.thumbnail === "object"
                    ? (attributes.thumbnail.original || attributes.thumbnail.large || attributes.thumbnail.medium || null)
                    : (attributes.thumbnail || null);

                return {
                    number: episodeNumber,
                    title: getKitsuPreferredTitle(attributes, config, { includeSlug: false }) || `Episodio ${episodeNumber}`,
                    released: safeToIsoString(attributes.airdate || attributes.released),
                    thumbnail: thumbnail,
                    overview: attributes.synopsis || attributes.overview || null
                };
            })
            .filter(Boolean)
        : [];

    if (normalizedEpisodes.length === 0) {
        const safeCount = Math.min(
            Math.max(Number.parseInt(String(fallbackEpisodeCount || ""), 10) || 0, 0),
            300
        );
        normalizedEpisodes = Array.from({ length: safeCount }, (_, index) => {
            const episodeNumber = index + 1;
            return {
                number: episodeNumber,
                title: `Episodio ${episodeNumber}`,
                released: null,
                thumbnail: null,
                overview: null
            };
        });
    }

    normalizedEpisodes.sort((a, b) => a.number - b.number);

    return normalizedEpisodes.map(episode => ({
        id: `kitsu:${normalizedKitsuId}:${episode.number}`,
        title: episode.title,
        season: 1,
        episode: episode.number,
        released: episode.released,
        thumbnail: episode.thumbnail || undefined,
        overview: episode.overview || undefined
    }));
}

async function enrichKitsuEpisodeVideosWithAnimeMapping(kitsuId, videos, tmdbDetails, config = null) {
    const normalizedKitsuId = normalizeKitsuId(kitsuId);
    const baseVideos = Array.isArray(videos) ? videos : [];
    if (!normalizedKitsuId || baseVideos.length === 0) return baseVideos;

    const validatedTmdbDetails = isTmdbAnimeDetails(tmdbDetails) ? tmdbDetails : null;
    const tmdbSeriesId = validatedTmdbDetails && validatedTmdbDetails.id
        ? String(validatedTmdbDetails.id)
        : null;
    const imdbId = normalizeImdbId(
        validatedTmdbDetails && validatedTmdbDetails.external_ids ? validatedTmdbDetails.external_ids.imdb_id : null
    );
    if (!tmdbSeriesId) return baseVideos;

    const episodeMappings = await mapWithConcurrency(baseVideos, 8, async (video) => {
        const absoluteEpisode = Number.parseInt(String(video && video.episode || ""), 10);
        if (!Number.isFinite(absoluteEpisode) || absoluteEpisode <= 0) return null;

        const payload = await fetchAnimeMappingPayload(normalizedKitsuId, absoluteEpisode);
        const tmdbEpisode = getAnimeMappingTmdbEpisode(payload);
        if (!tmdbEpisode || !tmdbEpisode.id || String(tmdbEpisode.id) !== tmdbSeriesId) return null;

        const seasonNumber = Number.parseInt(String(tmdbEpisode.season || ""), 10);
        const episodeNumber = Number.parseInt(String(tmdbEpisode.episode || ""), 10);
        if (!Number.isFinite(seasonNumber) || !Number.isFinite(episodeNumber)) return null;

        return {
            season: seasonNumber,
            episode: episodeNumber,
            rawEpisodeNumber: Number.parseInt(String(tmdbEpisode.rawEpisodeNumber || ""), 10),
            absoluteEpisode: Number.parseInt(String(tmdbEpisode.absoluteEpisode || ""), 10),
            airDate: typeof tmdbEpisode.airdate === "string" ? tmdbEpisode.airdate : null
        };
    });

    const requiredSeasons = [...new Set(
        episodeMappings
            .filter(Boolean)
            .map(item => item.season)
            .filter(season => Number.isFinite(season))
    )];
    const seasonPayloads = new Map();

    if (tmdbSeriesId && requiredSeasons.length > 0) {
        const seasons = await Promise.all(requiredSeasons.map(async seasonNumber => {
            const payload = await fetchTmdbSeasonDetails(tmdbSeriesId, seasonNumber, config);
            return [seasonNumber, payload];
        }));

        seasons.forEach(([seasonNumber, payload]) => {
            if (payload) seasonPayloads.set(seasonNumber, payload);
        });
    }

    return baseVideos.map((video, index) => {
        const mapping = episodeMappings[index];
        if (!mapping) return video;

        const seasonPayload = seasonPayloads.get(mapping.season);
        const tmdbEpisode = findTmdbEpisodeInSeason(seasonPayload, mapping);

        const episodeMediaId = tmdbSeriesId
            ? `${getPrimaryMediaId(imdbId, tmdbSeriesId)}:${mapping.season}:${mapping.episode}`
            : null;
        const configuredKitsuThumbnailId = video && typeof video.id === "string" && video.id.startsWith("kitsu:")
            ? video.id
            : null;
        const configuredThumbnailUrl = configuredKitsuThumbnailId
            ? getConfiguredAssetUrl(getRequestConfig(config), "thumbnail", imdbId, tmdbSeriesId, configuredKitsuThumbnailId)
            : (episodeMediaId
                ? getConfiguredAssetUrl(getRequestConfig(config), "thumbnail", imdbId, tmdbSeriesId, episodeMediaId)
                : null);
        const tmdbThumbnail = tmdbEpisode && tmdbEpisode.still_path
            ? `https://image.tmdb.org/t/p/w500${tmdbEpisode.still_path}`
            : null;
        const tmdbReleased = tmdbEpisode && tmdbEpisode.air_date ? safeToIsoString(tmdbEpisode.air_date) : null;

        return {
            ...video,
            title: /^episodio\s+\d+$/i.test(String(video.title || "")) && tmdbEpisode && tmdbEpisode.name
                ? tmdbEpisode.name
                : video.title,
            released: video.released || tmdbReleased || undefined,
            thumbnail: configuredThumbnailUrl || tmdbThumbnail || video.thumbnail,
            overview: (tmdbEpisode && tmdbEpisode.overview) || video.overview || undefined
        };
    });
}

async function resolvePreferredKitsuPoster(kitsuId, requestedType, config = null, tmdbDetails = null) {
    const normalizedKitsuId = normalizeKitsuId(kitsuId);
    if (!normalizedKitsuId) {
        return { poster: null, tmdbId: null, imdbId: null };
    }

    const requestedTypePath = requestedType === "series" ? "tv" : "movie";
    let validatedTmdbDetails = isTmdbAnimeDetails(tmdbDetails) ? tmdbDetails : null;

    if (!validatedTmdbDetails) {
        const animeMappingPayload = await fetchAnimeMappingPayload(normalizedKitsuId);
        const animeMappingIds = getAnimeMappingIds(animeMappingPayload);

        if (animeMappingIds.tmdbId) {
            const validatedDetailsResult = await fetchValidatedAnimeTmdbDetails(requestedTypePath, animeMappingIds.tmdbId, config);
            validatedTmdbDetails = validatedDetailsResult.details;
        }

        if (!validatedTmdbDetails && animeMappingIds.imdbId) {
            const resolvedTmdbId = await resolveTmdbIdFromImdb(animeMappingIds.imdbId, requestedType, config);
            if (resolvedTmdbId) {
                const validatedDetailsResult = await fetchValidatedAnimeTmdbDetails(requestedTypePath, resolvedTmdbId, config);
                validatedTmdbDetails = validatedDetailsResult.details;
            }
        }
    }

    const tmdbId = validatedTmdbDetails && validatedTmdbDetails.id ? String(validatedTmdbDetails.id) : null;
    const imdbId = normalizeImdbId(
        validatedTmdbDetails && validatedTmdbDetails.external_ids ? validatedTmdbDetails.external_ids.imdb_id : null
    );

    let poster = null;

    if (requestedType === "series" && tmdbId) {
        const firstEpisodeMapping = await fetchAnimeMappingPayload(normalizedKitsuId, 1);
        const tmdbEpisode = getAnimeMappingTmdbEpisode(firstEpisodeMapping);
        const seasonTmdbId = tmdbEpisode && tmdbEpisode.id ? String(tmdbEpisode.id) : null;
        const seasonNumber = Number.parseInt(String(tmdbEpisode && tmdbEpisode.season || ""), 10);

        if (seasonTmdbId && seasonTmdbId === tmdbId && Number.isFinite(seasonNumber) && seasonNumber > 0) {
            const seasonImagesPayload = await fetchTmdbSeasonImages(seasonTmdbId, seasonNumber, config);
            poster = getPreferredTmdbPosterUrl(seasonImagesPayload);

            const seasonPayload = await fetchTmdbSeasonDetails(seasonTmdbId, seasonNumber, config);
            if (!poster) {
                poster = getPreferredTmdbPosterUrl(seasonPayload);
            }
        }
    }

    if (!poster && validatedTmdbDetails) {
        poster = getPreferredTmdbPosterUrl(validatedTmdbDetails);
    }

    return { poster, tmdbId, imdbId };
}

async function fetchPreferredKitsuSeasonDetails(kitsuId, config = null, tmdbDetails = null) {
    const normalizedKitsuId = normalizeKitsuId(kitsuId);
    if (!normalizedKitsuId) return null;
    if (!isTmdbAnimeDetails(tmdbDetails) || !tmdbDetails.id) return null;

    const firstEpisodeMapping = await fetchAnimeMappingPayload(normalizedKitsuId, 1);
    const tmdbEpisode = getAnimeMappingTmdbEpisode(firstEpisodeMapping);
    const seasonTmdbId = tmdbEpisode && tmdbEpisode.id ? String(tmdbEpisode.id) : null;
    const seasonNumber = Number.parseInt(String(tmdbEpisode && tmdbEpisode.season || ""), 10);
    if (!seasonTmdbId || seasonTmdbId !== String(tmdbDetails.id) || !Number.isFinite(seasonNumber) || seasonNumber <= 0) return null;

    return fetchTmdbSeasonDetails(seasonTmdbId, seasonNumber, config);
}

function buildKitsuMetaFromPayload(kitsuId, payload, requestedType, config = null, options = {}) {
    const normalizedKitsuId = normalizeKitsuId(kitsuId);
    const data = payload && payload.data ? payload.data : {};
    const attributes = data.attributes || {};
    const mappings = extractKitsuMappings(payload);
    const resolvedConfig = getRequestConfig(config);
    const metaId = `kitsu:${normalizedKitsuId}`;
    const allowMappedAssets = options.allowMappedAssets !== false;
    const assetImdbId = allowMappedAssets ? mappings.imdbId : null;
    const assetTmdbId = allowMappedAssets ? mappings.tmdbId : null;
    const type = inferStremioTypeFromKitsuSubtype(attributes.subtype, requestedType || "series");

    let poster = getKitsuPoster(attributes);
    const configuredPosterUrl = getConfiguredAssetUrl(resolvedConfig, "poster", assetImdbId, assetTmdbId, metaId, type);
    if (configuredPosterUrl) poster = configuredPosterUrl;

    let background = getKitsuBackground(attributes);
    const configuredBackdropUrl = getConfiguredAssetUrl(resolvedConfig, "backdrop", assetImdbId, assetTmdbId, metaId, type);
    if (configuredBackdropUrl) background = configuredBackdropUrl;

    let logo = "";
    const configuredLogoUrl = getConfiguredAssetUrl(resolvedConfig, "logo", assetImdbId, assetTmdbId, metaId, type);
    if (configuredLogoUrl) logo = configuredLogoUrl;

    const releaseInfo = attributes.startDate ? String(attributes.startDate).split("-")[0] : null;
    const franchiseLinks = getKitsuFranchiseLinks(payload, config);

    const meta = {
        id: metaId,
        type,
        name: getKitsuPreferredTitle(attributes, config) || `Kitsu ${normalizedKitsuId}`,
        poster: poster || undefined,
        background: background || undefined,
        logo: logo || undefined,
        description: attributes.synopsis || attributes.description || undefined,
        releaseInfo: releaseInfo || undefined,
        released: safeToIsoString(attributes.startDate),
        year: releaseInfo || undefined,
        imdbRating: attributes.averageRating ? (parseFloat(attributes.averageRating) / 10).toFixed(1) : null,
        runtime: attributes.episodeLength
            ? `${attributes.episodeLength} min`
            : (attributes.totalLength ? `${attributes.totalLength} min` : null),
        genres: getKitsuCategories(payload),
        links: franchiseLinks,
        behaviorHints: {}
    };

    if (type === "movie") {
        meta.behaviorHints.defaultVideoId = metaId;
    } else {
        meta.behaviorHints.hasScheduledVideos = true;
    }

    return meta;
}

async function buildKitsuMetaForPayload(kitsuId, payload, requestedType, config = null, options = {}) {
    const normalizedKitsuId = normalizeKitsuId(kitsuId);
    if (!normalizedKitsuId || !payload || !payload.data) return null;

    const attributes = payload.data.attributes || {};
    const kitsuPreferredTitle = getKitsuPreferredTitle(attributes, config) || `Kitsu ${normalizedKitsuId}`;
    const includeEpisodeVideos = options.includeEpisodeVideos === true;
    const tmdbResolution = await resolveTmdbDetailsFromKitsuPayload(payload, requestedType, config);
    const tmdbDetails = tmdbResolution && tmdbResolution.details ? tmdbResolution.details : null;
    const rejectedNonAnime = !!(tmdbResolution && tmdbResolution.rejectedNonAnime);
    let meta = null;

    if (tmdbDetails) {
        meta = await transformToMeta(tmdbDetails, requestedType, config, { includeVideos: false });
    }

    if (!meta) {
        meta = buildKitsuMetaFromPayload(normalizedKitsuId, payload, requestedType, config, {
            allowMappedAssets: !rejectedNonAnime
        });
    }

    if (!meta) return null;

    meta.id = `kitsu:${normalizedKitsuId}`;
    meta.type = requestedType;
    meta.name = kitsuPreferredTitle;
    meta.behaviorHints = meta.behaviorHints || {};

    const preferredPoster = await resolvePreferredKitsuPoster(normalizedKitsuId, requestedType, config, tmdbDetails);
    const preferredSeasonDetails = requestedType === "series"
        ? await fetchPreferredKitsuSeasonDetails(normalizedKitsuId, config, tmdbDetails)
        : null;
    const configuredPosterUrl = getConfiguredAssetUrl(
        getRequestConfig(config),
        "poster",
        preferredPoster.imdbId,
        preferredPoster.tmdbId || (tmdbDetails && tmdbDetails.id ? String(tmdbDetails.id) : null),
        meta.id,
        requestedType
    );
    const configuredBackdropUrl = getConfiguredAssetUrl(
        getRequestConfig(config),
        "backdrop",
        preferredPoster.imdbId,
        preferredPoster.tmdbId || (tmdbDetails && tmdbDetails.id ? String(tmdbDetails.id) : null),
        meta.id,
        requestedType
    );
    const configuredLogoUrl = getConfiguredAssetUrl(
        getRequestConfig(config),
        "logo",
        preferredPoster.imdbId,
        preferredPoster.tmdbId || (tmdbDetails && tmdbDetails.id ? String(tmdbDetails.id) : null),
        meta.id,
        requestedType
    );
    meta.poster = configuredPosterUrl || preferredPoster.poster || meta.poster;
    if (configuredBackdropUrl) meta.background = configuredBackdropUrl;
    if (configuredLogoUrl) meta.logo = configuredLogoUrl;
    const kitsuSynopsis = attributes.synopsis || attributes.description || "";
    if (!meta.description && kitsuSynopsis) {
        meta.description = kitsuSynopsis;
    }
    if (!meta.description && preferredSeasonDetails && preferredSeasonDetails.overview) {
        meta.description = preferredSeasonDetails.overview;
    }
    meta.links = Array.isArray(meta.links) ? [...meta.links] : [];

    const franchiseLinks = getKitsuFranchiseLinks(payload, config);
    if (franchiseLinks.length > 0) {
        const seenLinks = new Set(meta.links.map(link => `${link.category || ""}:${link.url || ""}`));
        franchiseLinks.forEach(link => {
            const key = `${link.category || ""}:${link.url || ""}`;
            if (seenLinks.has(key)) return;
            seenLinks.add(key);
            meta.links.push(link);
        });
    }

    delete meta.videos;

    if (requestedType === "series") {
        if (includeEpisodeVideos) {
            const fallbackEpisodeCount = tmdbDetails && tmdbDetails.number_of_episodes
                ? tmdbDetails.number_of_episodes
                : Number.parseInt(String(payload.data.attributes && payload.data.attributes.episodeCount || ""), 10);
            const episodes = await fetchKitsuEpisodes(normalizedKitsuId);
            let videos = buildKitsuEpisodeVideos(normalizedKitsuId, episodes, fallbackEpisodeCount, config);
            videos = await enrichKitsuEpisodeVideosWithAnimeMapping(normalizedKitsuId, videos, tmdbDetails, config);

            if (videos.length > 0) {
                meta.videos = videos;
            }
        }

        delete meta.behaviorHints.defaultVideoId;
        meta.behaviorHints.hasScheduledVideos = true;
    } else {
        meta.behaviorHints.defaultVideoId = `kitsu:${normalizedKitsuId}`;
        delete meta.behaviorHints.hasScheduledVideos;
    }

    return meta;
}

async function buildMetaForKitsuId(requestedType, id, config = null) {
    const kitsuId = normalizeKitsuId(id);
    if (!kitsuId) return null;

    const payload = await fetchKitsuAnimeById(kitsuId);
    return buildKitsuMetaForPayload(kitsuId, payload, requestedType, config, { includeEpisodeVideos: requestedType === "series" });
}

function isKitsuCatalogId(catalogId) {
    return String(catalogId || "").startsWith("kitsu.");
}

function isTop10CatalogId(catalogId) {
    const normalizedCatalogId = String(catalogId || "").trim();
    return normalizedCatalogId.startsWith(TOP10_MANIFEST_PREFIX) || normalizedCatalogId.startsWith(LEGACY_TOP10_MANIFEST_PREFIX);
}

async function mapKitsuCatalogItemToMeta(item, forcedType = null, config = null, options = {}) {
    if (!item || !item.attributes) return null;

    const attributes = item.attributes || {};
    if (isDeletedKitsuAttributes(attributes)) return null;
    const type = forcedType || inferStremioTypeFromKitsuSubtype(attributes.subtype, "series");
    const enrichCatalogMeta = options.enrichCatalogMeta !== false;

    if (enrichCatalogMeta) {
        const payload = await fetchKitsuAnimeById(item.id);
        if (!payload || !payload.data) return null;
        const richMeta = await buildKitsuMetaForPayload(item.id, payload, type, config, { includeEpisodeVideos: false });
        if (richMeta) return richMeta;
    }

    const releaseInfo = attributes.startDate ? String(attributes.startDate).split("-")[0] : null;
    const metaId = `kitsu:${item.id}`;
    const preferredPoster = await resolvePreferredKitsuPoster(item.id, type, config);
    const configuredPosterUrl = getConfiguredAssetUrl(getRequestConfig(config), "poster", preferredPoster.imdbId, preferredPoster.tmdbId, metaId, type);
    const configuredBackdropUrl = getConfiguredAssetUrl(getRequestConfig(config), "backdrop", preferredPoster.imdbId, preferredPoster.tmdbId, metaId, type);

    return {
        id: metaId,
        type,
        name: getKitsuPreferredTitle(attributes, config) || `Kitsu ${item.id}`,
        poster: configuredPosterUrl || preferredPoster.poster || getKitsuPoster(attributes) || undefined,
        background: configuredBackdropUrl || getKitsuBackground(attributes) || undefined,
        description: attributes.synopsis || attributes.description || undefined,
        releaseInfo: releaseInfo || undefined,
        released: safeToIsoString(attributes.startDate),
        imdbRating: attributes.averageRating ? (parseFloat(attributes.averageRating) / 10).toFixed(1) : null,
        behaviorHints: {
            defaultVideoId: type === "movie" ? metaId : null,
            hasScheduledVideos: type === "series"
        }
    };
}

function filterKitsuCatalogItemsBySubtype(items, allowedSubtypes = []) {
    if (!Array.isArray(items) || allowedSubtypes.length === 0) return Array.isArray(items) ? items : [];

    const allowed = new Set(
        allowedSubtypes
            .map(subtype => String(subtype || "").trim().toLowerCase())
            .filter(Boolean)
    );

    return items.filter(item => {
        const itemSubtype = item && item.attributes && item.attributes.subtype
            ? String(item.attributes.subtype).trim().toLowerCase()
            : "";
        return allowed.has(itemSubtype);
    });
}

async function fetchKitsuCatalogMetas(catalogId, requestedType, extra = {}, config = null) {
    const normalizedCatalogId = String(catalogId || "").trim();
    const skip = Math.max(0, Number.parseInt(String(extra && extra.skip || "0"), 10) || 0);
    const search = typeof extra.search === "string" ? extra.search.trim() : "";
    const discover = typeof extra.discover === "string" ? extra.discover.trim() : "";
    const isDiscoverRequest = discover.toLowerCase() === "only";
    const pageLimit = isDiscoverRequest ? 100 : 20;
    const isSearchCatalog = normalizedCatalogId === "kitsu.series.search" ||
        normalizedCatalogId === "kitsu.movie.search" ||
        normalizedCatalogId === "kitsu.series.ova_search" ||
        normalizedCatalogId === "kitsu.series.ona_search" ||
        normalizedCatalogId === "kitsu.series.special_search";
    const isLatestCatalog = normalizedCatalogId === "kitsu.series.latest" ||
        normalizedCatalogId === "kitsu.movie.latest" ||
        normalizedCatalogId === "kitsu.series.ova_latest" ||
        normalizedCatalogId === "kitsu.series.ona_latest" ||
        normalizedCatalogId === "kitsu.series.special_latest";
    const resolvedConfig = getRequestConfig(config);
    const params = new URLSearchParams();
    params.set("page[limit]", String(pageLimit));
    params.set("page[offset]", String(skip));

    let forcedType = requestedType;
    let allowedSubtypes = [];
    let excludeFutureStartDates = false;

    switch (normalizedCatalogId) {
        case "kitsu.series.popular":
            params.set("sort", "-userCount");
            params.set("filter[subtype]", "TV");
            forcedType = "series";
            allowedSubtypes = ["tv"];
            break;
        case "kitsu.series.latest":
            params.set("sort", "-startDate");
            params.set("filter[subtype]", "TV");
            forcedType = "series";
            allowedSubtypes = ["tv"];
            excludeFutureStartDates = true;
            break;
        case "kitsu.movie.popular":
            params.set("sort", "-userCount");
            params.set("filter[subtype]", "movie");
            forcedType = "movie";
            allowedSubtypes = ["movie"];
            break;
        case "kitsu.movie.latest":
            params.set("sort", "-startDate");
            params.set("filter[subtype]", "movie");
            forcedType = "movie";
            allowedSubtypes = ["movie"];
            excludeFutureStartDates = true;
            break;
        case "kitsu.series.ova":
            params.set("sort", "-userCount");
            params.set("filter[subtype]", "OVA");
            forcedType = "series";
            allowedSubtypes = ["ova"];
            break;
        case "kitsu.series.ova_latest":
            params.set("sort", "-startDate");
            params.set("filter[subtype]", "OVA");
            forcedType = "series";
            allowedSubtypes = ["ova"];
            excludeFutureStartDates = true;
            break;
        case "kitsu.series.ona":
            params.set("sort", "-userCount");
            params.set("filter[subtype]", "ONA");
            forcedType = "series";
            allowedSubtypes = ["ona"];
            break;
        case "kitsu.series.ona_latest":
            params.set("sort", "-startDate");
            params.set("filter[subtype]", "ONA");
            forcedType = "series";
            allowedSubtypes = ["ona"];
            excludeFutureStartDates = true;
            break;
        case "kitsu.series.special":
            params.set("sort", "-userCount");
            params.set("filter[subtype]", "special");
            forcedType = "series";
            allowedSubtypes = ["special"];
            break;
        case "kitsu.series.special_latest":
            params.set("sort", "-startDate");
            params.set("filter[subtype]", "special");
            forcedType = "series";
            allowedSubtypes = ["special"];
            excludeFutureStartDates = true;
            break;
        case "kitsu.series.search":
            if (!search) return [];
            params.set("filter[text]", search);
            params.set("filter[subtype]", "TV");
            forcedType = "series";
            allowedSubtypes = ["tv"];
            break;
        case "kitsu.movie.search":
            if (!search) return [];
            params.set("filter[text]", search);
            params.set("filter[subtype]", "movie");
            forcedType = "movie";
            allowedSubtypes = ["movie"];
            break;
        case "kitsu.series.ova_search":
            if (!search) return [];
            params.set("filter[text]", search);
            params.set("filter[subtype]", "OVA");
            forcedType = "series";
            allowedSubtypes = ["ova"];
            break;
        case "kitsu.series.ona_search":
            if (!search) return [];
            params.set("filter[text]", search);
            params.set("filter[subtype]", "ONA");
            forcedType = "series";
            allowedSubtypes = ["ona"];
            break;
        case "kitsu.series.special_search":
            if (!search) return [];
            params.set("filter[text]", search);
            params.set("filter[subtype]", "special");
            forcedType = "series";
            allowedSubtypes = ["special"];
            break;
        default:
            return [];
    }

    const cacheKey = `kitsu:catalog:v18:${normalizedCatalogId}:${JSON.stringify({
        skip,
        search,
        discover,
        allowedSubtypes: allowedSubtypes.join(","),
        excludeFutureStartDates,
        tmdbApiKey: resolvedConfig.tmdbApiKey || "",
        topStreamingKey: resolvedConfig.topStreamingKey || ""
    })}`;
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return [];
    if (cached) return cached;

    try {
        async function fetchKitsuCatalogPage(offset) {
            params.set("page[offset]", String(offset));

            const response = await fetch(`${KITSU_BASE_URL}/anime?${params.toString()}`, {
                headers: {
                    Accept: "application/vnd.api+json, application/json"
                }
            });
            const payload = await response.json();

            if (!response.ok || payload.errors) {
                throw new Error(`Kitsu catalog fetch failed for ${normalizedCatalogId}`);
            }

            return {
                data: Array.isArray(payload && payload.data) ? payload.data : [],
                hasNext: !!(payload && payload.links && payload.links.next)
            };
        }

        let filteredData = [];

        if (normalizedCatalogId === "kitsu.series.latest") {
            async function fetchKitsuEpisodePage(offset) {
                const episodeParams = new URLSearchParams();
                episodeParams.set("page[limit]", String(pageLimit));
                episodeParams.set("page[offset]", String(offset));
                episodeParams.set("sort", "-airdate");
                episodeParams.set("include", "media");

                const response = await fetch(`${KITSU_BASE_URL}/episodes?${episodeParams.toString()}`, {
                    headers: {
                        Accept: "application/vnd.api+json, application/json"
                    }
                });
                const payload = await response.json();

                if (!response.ok || payload.errors) {
                    throw new Error(`Kitsu latest episodes fetch failed for ${normalizedCatalogId}`);
                }

                const includedItems = Array.isArray(payload && payload.included) ? payload.included : [];
                const includedMediaMap = new Map(
                    includedItems.map(item => [`${item.type}:${item.id}`, item])
                );

                return {
                    episodes: Array.isArray(payload && payload.data) ? payload.data : [],
                    includedMediaMap,
                    hasNext: !!(payload && payload.links && payload.links.next)
                };
            }

            const targetCount = skip + pageLimit;
            const maxPagesToScan = Math.min(50, Math.max(8, Math.ceil(targetCount / pageLimit) + 8));
            const seenMediaIds = new Set();
            const latestSeriesItems = [];
            let offset = 0;
            let pageIndex = 0;
            let hasNext = true;

            while (hasNext && latestSeriesItems.length < targetCount && pageIndex < maxPagesToScan) {
                const page = await fetchKitsuEpisodePage(offset);

                for (const episode of page.episodes) {
                    const airdate = episode && episode.attributes ? episode.attributes.airdate : null;
                    if (!isReleasedOnOrBeforeToday(airdate)) continue;

                    const mediaRef = episode && episode.relationships && episode.relationships.media
                        ? episode.relationships.media.data
                        : null;
                    if (!mediaRef || mediaRef.type !== "anime" || !mediaRef.id) continue;
                    if (seenMediaIds.has(mediaRef.id)) continue;

                    const mediaItem = page.includedMediaMap.get(`${mediaRef.type}:${mediaRef.id}`);
                    const mediaSubtype = mediaItem && mediaItem.attributes && mediaItem.attributes.subtype
                        ? String(mediaItem.attributes.subtype).trim().toLowerCase()
                        : "";
                    if (mediaSubtype !== "tv") continue;

                    seenMediaIds.add(mediaRef.id);
                    latestSeriesItems.push(mediaItem);

                    if (latestSeriesItems.length >= targetCount) break;
                }

                hasNext = page.hasNext && page.episodes.length === pageLimit;
                offset += pageLimit;
                pageIndex += 1;
            }

            filteredData = latestSeriesItems.slice(skip, skip + pageLimit);
        } else if (excludeFutureStartDates) {
            const targetCount = skip + pageLimit;
            const maxPagesToScan = Math.min(30, Math.max(5, Math.ceil(targetCount / pageLimit) + 5));
            const seenIds = new Set();
            let offset = 0;
            let pageIndex = 0;
            let hasNext = true;
            const releasedItems = [];

            while (hasNext && releasedItems.length < targetCount && pageIndex < maxPagesToScan) {
                const page = await fetchKitsuCatalogPage(offset);
                const releasedPageItems = filterKitsuCatalogItemsBySubtype(page.data, allowedSubtypes)
                    .filter(item => isReleasedOnOrBeforeToday(item && item.attributes ? item.attributes.startDate : null));

                for (const item of releasedPageItems) {
                    const itemId = item && item.id ? String(item.id) : "";
                    if (itemId && seenIds.has(itemId)) continue;
                    if (itemId) seenIds.add(itemId);
                    releasedItems.push(item);
                }

                hasNext = page.hasNext && page.data.length === pageLimit;
                offset += pageLimit;
                pageIndex += 1;
            }

            filteredData = releasedItems.slice(skip, skip + pageLimit);
        } else {
            const page = await fetchKitsuCatalogPage(skip);
            filteredData = filterKitsuCatalogItemsBySubtype(page.data, allowedSubtypes);
        }

        const metas = (await mapWithConcurrency(
            filteredData,
            isDiscoverRequest ? 2 : 3,
            item => mapKitsuCatalogItemToMeta(item, forcedType, config, { enrichCatalogMeta: true })
        ))
            .filter(Boolean);

        const sortedMetas = (isSearchCatalog || (isLatestCatalog && normalizedCatalogId !== "kitsu.series.latest"))
            ? sortMetasByReleaseDesc(metas)
            : metas;

        await cache.set(cacheKey, sortedMetas, withTtlJitter(KITSU_TTL_SECONDS.catalog));
        return sortedMetas;
    } catch (error) {
        console.error(`[Easy Catalogs] Kitsu Catalog Error (${normalizedCatalogId}): ${error.message}`);
        await cache.set(cacheKey, createNegativeCache("kitsu_catalog_fetch_failed"), NEGATIVE_CACHE_TTL_SECONDS);
        return [];
    }
}

const MOVIE_GENRES = {
    "Azione": 28, "Avventura": 12, "Animazione": 16, "Commedia": 35,
    "Crime": 80, "Documentario": 99, "Dramma": 18, "Famiglia": 10751,
    "Fantasy": 14, "Storia": 36, "Horror": 27, "Musica": 10402,
    "Mistero": 9648, "Romantico": 10749, "Fantascienza": 878,
    "Thriller": 53, "Guerra": 10752, "Western": 37
};

const TV_GENRES = {
    "Azione & Avventura": 10759, "Animazione": 16, "Commedia": 35,
    "Crime": 80, "Documentario": 99, "Dramma": 18, "Famiglia": 10751,
    "Bambini": 10762, "Mistero": 9648, "News": 10763, "Reality": 10764,
    "Fantascienza & Fantasy": 10765, "Soap": 10766, "Talk": 10767,
    "Guerra & Politica": 10768, "Western": 37
};

const YEARS = [];
const currentYear = new Date().getFullYear();
for (let i = currentYear; i >= 1900; i--) {
    YEARS.push(i.toString());
}

const GENRE_ID_TO_NAME = {};
Object.entries(MOVIE_GENRES).forEach(([name, id]) => GENRE_ID_TO_NAME[id] = name);
Object.entries(TV_GENRES).forEach(([name, id]) => GENRE_ID_TO_NAME[id] = name);

const PROVIDERS = {
    "Netflix": 8,
    "Amazon Prime Video": "9|119",
    "Disney+": "337|390",
    "HBO Max": "384|1899",
    "Apple TV+": 350,
    "Paramount+": "531|2303|2304",
    "Sky Go / NOW": "29|39",
    "Rai Play": 222,
    "Mediaset Infinity": "359|110",
    "Timvision": 109,
    "Rakuten TV": 35
};

const COMPANY_IDS = {
    "Netflix": "178464|145172", // Netflix, Netflix Animation
    "Amazon Prime Video": "20580|21", // Amazon Studios, MGM
    "Disney+": "2|3|420|1|6125", // Disney, Pixar, Marvel, Lucasfilm, Disney Animation
    "Apple TV+": 194232,
    "HBO Max": "7429|174|128064|12|158691", // HBO Films, WB, DC Films, New Line, HBO Max
    "Paramount+": 4,
    "Rai Play": 1583, "Mediaset Infinity": 1677, "Sky Go / NOW": 19079,
    "Timvision": 109, "Rakuten TV": 35
};

const NETWORK_IDS = {
    "Netflix": 213, 
    "Amazon Prime Video": 1024, 
    "Disney+": 2739,
    "Apple TV+": 2552, 
    "HBO Max": "49|3186", // HBO, HBO Max
    "Paramount+": 4330,
    "Rai Play": "3463|533|236|1583", 
    "Mediaset Infinity": "537|402|1677",
    "Sky Go / NOW": 2667,
    "Timvision": 109 // Fallback ID if exists
};

const SLUG_TO_PROVIDER = {
    "netflix": "Netflix", "amazon": "Amazon Prime Video",
    "disney": "Disney+", "apple": "Apple TV+", "hbo": "HBO Max",
    "paramount": "Paramount+", "now": "Sky Go / NOW", "sky": "Sky Go / NOW",
    "rai": "Rai Play", "mediaset": "Mediaset Infinity",
    "timvision": "Timvision", "rakuten": "Rakuten TV"
};

const PROVIDER_SLUGS = {};
Object.entries(SLUG_TO_PROVIDER).forEach(([slug, name]) => {
    if (!PROVIDER_SLUGS[name]) {
        PROVIDER_SLUGS[name] = slug;
    }
});

const manifest = {
    id: "org.bestia.easycatalogs",
    version: "1.0.52",
    name: "Easy Catalogs",
    description: "Easy Catalogs per Stremio",
    behaviorHints: {
        configurable: true
    },
    resources: [
        "catalog",
        { name: "meta", types: ["movie", "series"], idPrefixes: ["tmdb", "tt", "kitsu"] },
        { name: "stream", types: ["movie", "series"], idPrefixes: ["tmdb", "tt", "kitsu"] }
    ],
    types: ["movie", "series"],
    catalogs: [], // Start empty, populate later to bypass 8KB limit check
    idPrefixes: ["tmdb", "tt", "kitsu"]
};

// Define Full Catalogs List
const fullCatalogs = [
        {
            type: "movie",
            id: "tmdb.movie.upcoming",
            name: "In Arrivo",
            extra: [{ name: "skip", isRequired: false }]
        },
        {
            type: "movie",
            id: "tmdb.movie.now_playing",
            name: "Al Cinema",
            extra: [{ name: "skip", isRequired: false }]
        },
        {
            type: "series",
            id: "tmdb.series.upcoming",
            name: "In Onda",
            extra: [{ name: "skip", isRequired: false }]
        },
        {
            type: "movie",
            id: "tmdb.movie.popular",
            name: "Popolari",
            extra: [{ 
                name: "genre", 
                isRequired: false,
                options: Object.keys(MOVIE_GENRES) 
            }, { name: "skip", isRequired: false }]
        },
        {
            type: "series",
            id: "tmdb.series.popular",
            name: "Popolari",
            extra: [{ 
                name: "genre", 
                isRequired: false,
                options: Object.keys(TV_GENRES) 
            }, { name: "skip", isRequired: false }]
        },
        {
            type: "movie",
            id: "tmdb.movie.trending",
            name: "Di Tendenza",
            extra: [{ 
                name: "genre", 
                isRequired: false,
                options: ["Day", "Week"] 
            }, { name: "skip", isRequired: false }]
        },
        {
            type: "series",
            id: "tmdb.series.trending",
            name: "Di Tendenza",
            extra: [{ 
                name: "genre", 
                isRequired: false,
                options: ["Day", "Week"] 
            }, { name: "skip", isRequired: false }]
        },
        {
            type: "movie",
            id: "tmdb.movie.top_rated",
            name: "Più Votati",
            extra: [{ 
                name: "genre", 
                isRequired: false,
                options: Object.keys(MOVIE_GENRES) 
            }, { name: "skip", isRequired: false }]
        },
        {
            type: "series",
            id: "tmdb.series.top_rated",
            name: "Più Votati",
            extra: [{ 
                name: "genre", 
                isRequired: false,
                options: Object.keys(TV_GENRES) 
            }, { name: "skip", isRequired: false }]
        },
        {
            type: "movie",
            id: TOP10_MOVIE_MANIFEST_ID,
            name: "Top 10 Italia",
            extra: [{ name: "skip", isRequired: false }]
        },
        {
            type: "series",
            id: TOP10_SERIES_MANIFEST_ID,
            name: "Top 10 Italia",
            extra: [{ name: "skip", isRequired: false }]
        },
        {
            type: "movie",
            id: "tmdb.movie.year",
            name: "Per Anno",
            extra: [{ 
                name: "genre", 
                isRequired: false, 
                options: YEARS 
            }, { name: "skip", isRequired: false }]
        },
        {
            type: "series",
            id: "tmdb.series.year",
            name: "Per Anno",
            extra: [{ 
                name: "genre", 
                isRequired: false, 
                options: YEARS 
            }, { name: "skip", isRequired: false }]
        },
        {
            type: "movie",
            id: "tmdb.movie.kids",
            name: "Bambini",
            extra: [{ name: "skip", isRequired: false }]
        },
        {
            type: "series",
            id: "tmdb.series.kids",
            name: "Bambini",
            extra: [{ name: "skip", isRequired: false }]
        },

        {
            type: "movie",
            id: "tmdb.movie.search",
            name: "Ricerca TMDB",
            extra: [{ name: "search", isRequired: true }]
        },
        {
            type: "series",
            id: "tmdb.series.search",
            name: "Ricerca TMDB",
            extra: [{ name: "search", isRequired: true }]
        }
];

// Add Provider Catalogs dynamically to fullCatalogs
Object.keys(PROVIDERS).forEach(providerName => {
    const slug = PROVIDER_SLUGS[providerName] || providerName.toLowerCase().replace(/[^a-z0-9]/g, '');
    
    // 1. "Originals" Catalog (Production Company/Network)
    fullCatalogs.push({
        type: "movie",
        id: `tmdb.movie.${slug}`,
        name: `${providerName} Original`,
        extra: [{ 
            name: "genre", 
            isRequired: false,
            options: Object.keys(MOVIE_GENRES) 
        }, { name: "skip", isRequired: false }]
    });
    fullCatalogs.push({
        type: "series",
        id: `tmdb.series.${slug}`,
        name: `${providerName} Original`,
        extra: [{ 
            name: "genre", 
            isRequired: false,
            options: Object.keys(TV_GENRES) 
        }, { name: "skip", isRequired: false }]
    });

    // 2. "Catalog" Catalog (Watch Availability)
    fullCatalogs.push({
        type: "movie",
        id: `tmdb.movie.${slug}_catalog`,
        name: `${providerName}`,
        extra: [{ 
            name: "genre", 
            isRequired: false,
            options: Object.keys(MOVIE_GENRES) 
        }, { name: "skip", isRequired: false }]
    });
    fullCatalogs.push({
        type: "series",
        id: `tmdb.series.${slug}_catalog`,
        name: `${providerName}`,
        extra: [{ 
            name: "genre", 
            isRequired: false,
            options: Object.keys(TV_GENRES) 
        }, { name: "skip", isRequired: false }]
    });

    if (TOP10_PROVIDER_PAGE_SLUGS[slug]) {
        fullCatalogs.push({
            type: "movie",
            id: buildTop10ManifestId("movie", slug),
            name: `${providerName} Top 10 Italia`,
            extra: [{ name: "skip", isRequired: false }]
        });
        fullCatalogs.push({
            type: "series",
            id: buildTop10ManifestId("series", slug),
            name: `${providerName} Top 10 Italia`,
            extra: [{ name: "skip", isRequired: false }]
        });
    }
});

// Add Anime Catalogs at the end
fullCatalogs.push({
    type: "movie",
    id: "tmdb.movie.anime",
    name: "TMDB Popolari - Film Anime",
    extra: [{
        name: "genre",
        isRequired: false,
        options: Object.keys(MOVIE_GENRES)
    }, { name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "series",
    id: "tmdb.series.anime",
    name: "TMDB Popolari - Serie Anime",
    extra: [{
        name: "genre",
        isRequired: false,
        options: Object.keys(TV_GENRES)
    }, { name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "movie",
    id: "tmdb.movie.anime_search",
    name: "TMDB Cerca - Film Anime",
    extra: [{ name: "search", isRequired: true }]
});
fullCatalogs.push({
    type: "series",
    id: "tmdb.series.anime_search",
    name: "TMDB Cerca - Serie Anime",
    extra: [{ name: "search", isRequired: true }]
});

fullCatalogs.push({
    type: "series",
    id: "kitsu.series.latest",
    name: "Kitsu Ultimi Episodi - Anime",
    extra: [{ name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.popular",
    name: "Kitsu Popolari - Serie Anime",
    extra: [{ name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "movie",
    id: "kitsu.movie.latest",
    name: "Kitsu Ultimi Film - Anime",
    extra: [{ name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "movie",
    id: "kitsu.movie.popular",
    name: "Kitsu Popolari - Film Anime",
    extra: [{ name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.ova_latest",
    name: "Kitsu Ultimi OVA - Anime",
    extra: [{ name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.ova",
    name: "Kitsu OVA - Anime",
    extra: [{ name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.ona_latest",
    name: "Kitsu Ultimi ONA - Anime",
    extra: [{ name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.ona",
    name: "Kitsu ONA - Anime",
    extra: [{ name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.special_latest",
    name: "Kitsu Ultimi Special - Anime",
    extra: [{ name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.special",
    name: "Kitsu Special - Anime",
    extra: [{ name: "skip", isRequired: false }]
});

// Anime Search Catalogs
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.search",
    name: "Kitsu Cerca - Serie Anime",
    extra: [{ name: "search", isRequired: true }]
});
fullCatalogs.push({
    type: "movie",
    id: "kitsu.movie.search",
    name: "Kitsu Cerca - Film Anime",
    extra: [{ name: "search", isRequired: true }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.ova_search",
    name: "Kitsu Cerca - OVA Anime",
    extra: [{ name: "search", isRequired: true }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.ona_search",
    name: "Kitsu Cerca - ONA Anime",
    extra: [{ name: "search", isRequired: true }]
});
fullCatalogs.push({
    type: "series",
    id: "kitsu.series.special_search",
    name: "Kitsu Cerca - Special Anime",
    extra: [{ name: "search", isRequired: true }]
});

// Use a minimal catalog list for initial builder creation (to bypass 8KB limit check)
// We need at least one catalog so that validation passes for 'catalog' handler
manifest.catalogs = [ fullCatalogs[0] ];

const builder = new addonBuilder(manifest);

builder.defineStreamHandler(async ({ type, id }) => {
    if (!shouldReturnStreams()) {
        return { streams: [] };
    }

    const normalizedId = String(id || "").trim();
    const isSupportedId = normalizedId.startsWith("kitsu:") ||
        normalizedId.startsWith("tmdb:") ||
        normalizedId.startsWith("tt");
    if (!isSupportedId) {
        return { streams: [] };
    }

    try {
        const response = await fetch(`${EASY_STREAMS_BASE_URL}/stream/${encodeURIComponent(type)}/${encodeURIComponent(normalizedId)}.json`, {
            headers: {
                Accept: "application/json"
            }
        });

        if (!response.ok) {
            return { streams: [] };
        }

        const payload = await response.json();
        const streams = Array.isArray(payload && payload.streams) ? payload.streams : [];
        return { streams };
    } catch (error) {
        console.error(`[Easy Catalogs] Stream Bridge Error: ${error.message}`);
        return { streams: [] };
    }
});

// Metadata Handler
builder.defineMetaHandler(async ({ type, id }) => {
    console.log(`[Easy Catalogs] Meta Request: type=${type} id=${id}`);
    
    const config = getRequestConfig();
    const configHash = Object.keys(config).length > 0 ? JSON.stringify(config) : "default";
    const cacheKey = `meta_v14:${type}:${id}:${configHash}`;
    const metaTtl = type === "movie" ? CACHE_TTL_SECONDS.metaMovie : CACHE_TTL_SECONDS.metaSeries;
    
    const cached = await cache.get(cacheKey);
    if (isNegativeCache(cached)) return { meta: {} };
    if (cached) return { meta: cached };

    if (id.startsWith("kitsu:")) {
        try {
            const meta = await buildMetaForKitsuId(type, id, config);
            if (meta) {
                await cache.set(cacheKey, meta, withTtlJitter(metaTtl));
                return { meta };
            }

            await cache.set(cacheKey, createNegativeCache("kitsu_meta_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
            return { meta: {} };
        } catch (e) {
            console.error(`[Easy Catalogs] Kitsu Meta Error: ${e.message}`);
            await cache.set(cacheKey, createNegativeCache("kitsu_meta_fetch_failed"), NEGATIVE_CACHE_TTL_SECONDS);
            return { meta: {} };
        }
    }
    
    let tmdbId = id;
    // If it's an IMDB ID (tt...), we need to find the TMDB ID first, or use find logic if supported.
    // TMDB API supports find by external ID.
    
    let url = "";
    if (id.startsWith("tt")) {
        url = `${BASE_URL}/find/${id}?api_key=${getTmdbApiKey(config)}&external_source=imdb_id`;
    } else if (id.startsWith("tmdb:")) {
        tmdbId = id.split(":")[1];
        url = `${BASE_URL}/${type === "series" ? "tv" : "movie"}/${tmdbId}?api_key=${getTmdbApiKey(config)}&language=it-IT&append_to_response=credits,similar,videos,images,external_ids,release_dates&include_image_language=it,en,null&include_video_language=it,en,null`;
    } else {
        // Assume it is a raw TMDB ID if it's just numbers, though Stremio usually prefixes.
        // But if it comes from our catalog, it might be prefixed.
        // Let's assume standard TMDB ID for safety if numeric.
         url = `${BASE_URL}/${type === "series" ? "tv" : "movie"}/${id}?api_key=${getTmdbApiKey(config)}&language=it-IT&append_to_response=credits,similar,videos,images,external_ids,release_dates&include_image_language=it,en,null&include_video_language=it,en,null`;
    }

    try {
        const response = await fetch(url);
        const data = await response.json();
        
        let meta = null;

        if (id.startsWith("tt")) {
             // Handle Find Result
             const results = type === "series" ? data.tv_results : data.movie_results;
             if (results && results.length > 0) {
                 const item = results[0];
                const details = await fetchTmdbDetails(type === "series" ? "tv" : "movie", item.id, config);
                if (details) {
                    meta = await transformToMeta(details, type, config);
                }
             }
        } else {
            meta = await transformToMeta(data, type, config);
        }

        if (meta) {
            await cache.set(cacheKey, meta, withTtlJitter(metaTtl));
            return { meta };
        } else {
            await cache.set(cacheKey, createNegativeCache("meta_not_found"), NEGATIVE_CACHE_TTL_SECONDS);
            return { meta: {} };
        }

    } catch (e) {
        console.error(`[Easy Catalogs] Meta Error: ${e.message}`);
        await cache.set(cacheKey, createNegativeCache("meta_fetch_failed"), NEGATIVE_CACHE_TTL_SECONDS);
        return { meta: {} };
    }
});

async function transformToMeta(item, type, config = null, options = {}) {
    const isMovie = type === "movie";
    const year = item.release_date ? item.release_date.split("-")[0] : (item.first_air_date ? item.first_air_date.split("-")[0] : "");
    const resolvedConfig = getRequestConfig(config);
    const manifestUrl = getManifestUrl(resolvedConfig);
    const includeVideos = options.includeVideos !== false;
    
    // Improved logic for exact release date (Italy)
    let exactReleaseDate = item.release_date || item.first_air_date;
    
    if (isMovie && item.release_dates && item.release_dates.results) {
         const itRelease = item.release_dates.results.find(r => r.iso_3166_1 === "IT");
         if (itRelease && itRelease.release_dates && itRelease.release_dates.length > 0) {
              const theatrical = itRelease.release_dates.find(d => d.type === 3);
              if (theatrical) {
                  exactReleaseDate = theatrical.release_date.split('T')[0];
              } else {
                  exactReleaseDate = itRelease.release_dates[0].release_date.split('T')[0];
              }
         }
    } else if (!isMovie) {
         if (item.next_episode_to_air) {
             exactReleaseDate = item.next_episode_to_air.air_date;
         } else if (item.last_air_date) {
             exactReleaseDate = item.last_air_date;
         }
    }

    const formattedDate = exactReleaseDate ? exactReleaseDate.split('-').reverse().join('/') : null;

    // Improved Series Release Info (e.g. 2010-2014 or 2022-)
    let releaseInfo = year;
    if (isMovie && formattedDate) {
        releaseInfo = formattedDate;
    }
    if (!isMovie && year) {
        if (item.in_production) {
            releaseInfo = `${year}-`;
        } else if (item.last_air_date) {
            const endYear = item.last_air_date.split("-")[0];
            if (endYear && endYear !== year) {
                releaseInfo = `${year}-${endYear}`;
            }
        }
    }
    
    let trailers = [];
    let trailerStreams = [];
    
    if (item.videos && item.videos.results && item.videos.results.length > 0) {
        const tmdbTrailers = item.videos.results.filter(v => v.site === "YouTube" && v.type === "Trailer");
        
        // Sort: Italian > English, then Quality (Size)
        tmdbTrailers.sort((a, b) => {
             const langA = a.iso_639_1 === "it" ? 2 : (a.iso_639_1 === "en" ? 1 : 0);
             const langB = b.iso_639_1 === "it" ? 2 : (b.iso_639_1 === "en" ? 1 : 0);
             
             if (langA !== langB) {
                 return langB - langA;
             }
             
             // If language is same, prioritize higher resolution (size)
             return (b.size || 0) - (a.size || 0);
        });

        if (tmdbTrailers.length > 0) {
             trailers = tmdbTrailers.map(v => ({ source: v.key, type: "Trailer" }));
             trailerStreams = tmdbTrailers.map(v => ({ title: v.name, ytId: v.key }));
        }
    }
    
    // Extract Logo (Clear Art)
    let logo = "";
    if (item.images && item.images.logos && item.images.logos.length > 0) {
        // Find the best logo, preferably in Italian or English, or just the first one
        const logoItem = item.images.logos.find(l => l.iso_639_1 === "it") || 
                         item.images.logos.find(l => l.iso_639_1 === "en") || 
                         item.images.logos[0];
        if (logoItem) {
            logo = `https://image.tmdb.org/t/p/w500${logoItem.file_path}`;
        }
    }

    // Get IMDb Rating and Metadata from Cinemeta (for rating and backup thumbnails)
    const imdbId = item.imdb_id || (item.external_ids && item.external_ids.imdb_id);
    let cinemetaMeta = null;
    let fetchedImdbRating = null;
    
    if (imdbId) {
        try {
            const response = await fetch(`https://v3-cinemeta.strem.io/meta/${type}/${imdbId}.json`);
            const data = await response.json();
            if (data && data.meta) {
                cinemetaMeta = data.meta;
                fetchedImdbRating = cinemetaMeta.imdbRating;
            }
        } catch (e) {
            console.warn(`Error fetching Cinemeta for ${imdbId}:`, e.message);
        }
    }

    let poster = item.poster_path ? `https://image.tmdb.org/t/p/w500${item.poster_path}` : "";
    const configuredPosterUrl = getConfiguredAssetUrl(resolvedConfig, "poster", imdbId, item.id, null, type);
    if (configuredPosterUrl) poster = configuredPosterUrl;

    const configuredLogoUrl = getConfiguredAssetUrl(resolvedConfig, "logo", imdbId, item.id, null, type);
    if (configuredLogoUrl) logo = configuredLogoUrl;

    let background = item.backdrop_path ? `https://image.tmdb.org/t/p/original${item.backdrop_path}` : "";
    const configuredBackdropUrl = getConfiguredAssetUrl(resolvedConfig, "backdrop", imdbId, item.id, null, type);
    if (configuredBackdropUrl) background = configuredBackdropUrl;

    // Fetch Seasons and Episodes for Series
    let videos = [];
    if (includeVideos && !isMovie && item.seasons) {
        try {
            const seasonPromises = item.seasons.map(season => {
                 // Skip seasons with 0 episodes if likely placeholder, but keep specials (season 0) if they have content
                 if (season.episode_count === 0) return null; 
                 const seasonUrl = `${BASE_URL}/tv/${item.id}/season/${season.season_number}?api_key=${getTmdbApiKey(resolvedConfig)}&language=it-IT`;
                 return fetch(seasonUrl).then(res => res.json()).catch(e => null);
            }).filter(Boolean);

            const seasonsDetails = await Promise.all(seasonPromises);

            // Create a map for Cinemeta episodes for faster lookup (thumbnail fallback)
            const cinemetaEpisodes = {};
            if (cinemetaMeta && cinemetaMeta.videos) {
                cinemetaMeta.videos.forEach(v => {
                    if (v.season && v.episode) {
                        cinemetaEpisodes[`${v.season}:${v.episode}`] = v;
                    }
                });
            }

            seasonsDetails.forEach(seasonData => {
                if (seasonData && seasonData.episodes) {
                    // Check if we need to renumber episodes (e.g. for Anime with absolute numbering)
                    // If the first episode of a non-zero season starts with a number > 1, assume absolute numbering
                    // and renumber visually to start from 1.
                    const firstEp = seasonData.episodes[0];
                    const shouldRenumber = firstEp && firstEp.episode_number > 1 && firstEp.season_number > 0;

                    seasonData.episodes.forEach((ep, index) => {
                        // ID format: imdbId:season:episode or tmdb:id:season:episode
                        const idPrefix = imdbId || `tmdb:${item.id}`;
                        
                        // Handle release date
                        let released = null;
                        if (ep.air_date) {
                            try {
                                released = new Date(ep.air_date).toISOString();
                            } catch (e) {}
                        }

                        // Check for Cinemeta thumbnail
                        const cinemetaThumb = cinemetaEpisodes[`${ep.season_number}:${ep.episode_number}`]?.thumbnail;
                        const episodeMediaId = `${getPrimaryMediaId(imdbId, item.id)}:${ep.season_number}:${ep.episode_number}`;
                        const configuredThumbnailUrl = getConfiguredAssetUrl(resolvedConfig, "thumbnail", imdbId, item.id, episodeMediaId);

                        const episodeNumber = shouldRenumber ? (index + 1) : ep.episode_number;
                        const fallbackThumbnail = ep.still_path 
                            ? `https://image.tmdb.org/t/p/w500${ep.still_path}` 
                            : (cinemetaThumb || (item.backdrop_path ? `https://image.tmdb.org/t/p/w500${item.backdrop_path}` : null));

                        videos.push({
                            id: `${idPrefix}:${ep.season_number}:${episodeNumber}`,
                            title: ep.name,
                            released: released,
                            thumbnail: configuredThumbnailUrl || fallbackThumbnail,
                            overview: ep.overview,
                            season: ep.season_number,
                            episode: episodeNumber,
                        });
                    });
                }
            });
            
            // Sort videos by season and episode
            videos.sort((a, b) => {
                if (a.season !== b.season) return a.season - b.season;
                return a.episode - b.episode;
            });
        } catch (e) {
            console.error(`[Easy Catalogs] Error fetching episodes for ${item.id}:`, e);
        }
    }

    return {
        id: imdbId || `tmdb:${item.id}`,
        type: type,
        name: item.title || item.name,
        poster: poster,
        background: background,
        logo: logo,
        description: item.overview,
        releaseInfo: releaseInfo,
        released: exactReleaseDate ? new Date(exactReleaseDate).toISOString() : null,
        year: year,
        imdbRating: fetchedImdbRating || (item.vote_average !== undefined ? item.vote_average.toFixed(1) : null),
        genres: item.genres ? item.genres.map(g => g.name) : [],
        cast: item.credits && item.credits.cast ? item.credits.cast.slice(0, 10).map(c => c.name) : [],
        director: [
            ...(item.created_by || []).map(c => c.name),
            ...(item.credits && item.credits.crew ? item.credits.crew.filter(c => c.job === "Director").map(c => c.name) : [])
        ],
        writer: item.credits && item.credits.crew ? item.credits.crew.filter(c => c.job === "Writer" || c.job === "Screenplay").map(c => c.name) : [],
        
        // Stremio Links for Director, Cast, etc.
        links: [
            // IMDb Rating Link (Critical for Stremio badge)
            {
                name: fetchedImdbRating || (item.vote_average !== undefined ? item.vote_average.toFixed(1) : "N/A"),
                category: "imdb",
                url: `https://imdb.com/title/${imdbId}`
            },
            // Collection (Saga) Link
            ...(item.belongs_to_collection ? [{
                name: item.belongs_to_collection.name,
                category: "Collection",
                url: `stremio:///search?search=${encodeURIComponent(item.belongs_to_collection.name)}`
            }] : []),
            ...(item.genres ? item.genres.map(g => ({
                name: g.name,
                category: "Genres",
                url: `stremio:///discover/${encodeURIComponent(manifestUrl)}/${type}/tmdb.${type === "movie" ? "movie" : "series"}.popular?genre=${encodeURIComponent(g.name)}`
            })) : []),
            // Production Companies
            ...(item.production_companies ? item.production_companies.slice(0, 3).map(c => ({
                name: c.name,
                category: "Production",
                url: `stremio:///search?search=${encodeURIComponent(c.name)}`
            })) : []),
            // Networks (Series)
            ...(item.networks ? item.networks.slice(0, 3).map(n => ({
                name: n.name,
                category: "Networks",
                url: `stremio:///search?search=${encodeURIComponent(n.name)}`
            })) : []),
            // Creators (Series)
            ...(item.created_by ? item.created_by.map(c => ({
                name: c.name,
                category: "Creators",
                url: `stremio:///search?search=${encodeURIComponent(c.name)}`
            })) : []),
            ...(item.credits && item.credits.crew ? item.credits.crew.filter(c => c.job === "Director").map(c => ({
                name: c.name,
                category: "Directors",
                url: `stremio:///search?search=${encodeURIComponent(c.name)}`
            })) : []),
            ...(item.credits && item.credits.cast ? item.credits.cast.slice(0, 10).map(c => ({
                name: c.name,
                category: "Cast",
                url: `stremio:///search?search=${encodeURIComponent(c.name)}`
            })) : []),
            ...(item.credits && item.credits.crew ? item.credits.crew.filter(c => c.job === "Writer" || c.job === "Screenplay").map(c => ({
                name: c.name,
                category: "Writers",
                url: `stremio:///search?search=${encodeURIComponent(c.name)}`
            })) : [])
        ],
        runtime: isMovie ? (item.runtime ? `${item.runtime} min` : null) : (item.episode_run_time && item.episode_run_time[0] ? `${item.episode_run_time[0]} min` : null),
        // Match tmdb-addon implementation for trailers
        trailers: trailers,
        trailerStreams: trailerStreams,
        behaviorHints: {
            defaultVideoId: isMovie ? (item.imdb_id || `tmdb:${item.id}`) : null,
            hasScheduledVideos: !isMovie
        },
        videos: videos 
    };
}

// Now restore full catalogs to the builder's manifest (if accessible) or just define the handler
// The builder freezes the manifest, but let's check if we can modify the array content later via interface

builder.defineCatalogHandler(async ({ type, id, extra }) => {
    console.log(`[Easy Catalogs] Request: type=${type} id=${id} extra=${JSON.stringify(extra)}`);
    
    // Convert Stremio type to TMDB type
    const tmdbType = type === "series" ? "tv" : "movie";
    const allowFuture = id.includes("upcoming");
    const config = getRequestConfig();
    const resolvedExtra = extra || {};
    const catalogShapes = getConfiguredCatalogShapes(config);
    const landscapeForCatalog = shouldLandscapeCatalog(id, catalogShapes);
    
    try {
        if (isKitsuCatalogId(id)) {
            const metas = await fetchKitsuCatalogMetas(id, type, resolvedExtra, config);
            return { metas: applyLandscapeToMetas(metas, landscapeForCatalog, config) };
        }

        if (isTop10CatalogId(id)) {
            const metas = await fetchTop10CatalogMetas(id, type, resolvedExtra, config);
            return { metas: applyLandscapeToMetas(metas, landscapeForCatalog, config) };
        }

        let endpoint = null;
        let queryParams = `api_key=${getTmdbApiKey(config)}&language=it-IT`;

        // Handle Search
        if (resolvedExtra.search) {
            const query = resolvedExtra.search;
            const searchResults = new Map(); // Use Map to deduplicate by ID
            const today = new Date().toISOString().split('T')[0];
            const isAnimeSearch = id.includes("anime_search");
            const preferKitsuId = usesKitsuAnimeIds(id);

            try {
                // 1. Search Content (Movie/TV)
                const contentRes = await fetch(`${BASE_URL}/search/${tmdbType}?api_key=${getTmdbApiKey(config)}&query=${encodeURIComponent(query)}&language=it-IT`);
                const contentData = await contentRes.json();
                if (contentData.results) {
                    contentData.results.forEach(item => {
                        const date = item.release_date || item.first_air_date;
                        if (date && date <= today && !searchResults.has(item.id)) {
                             // Apply Anime Filter if needed
                             if (isAnimeSearch) {
                                 // Check for Genre 16 (Animation) AND Original Language 'ja'
                                 if (item.genre_ids && item.genre_ids.includes(16) && item.original_language === 'ja') {
                                     searchResults.set(item.id, item);
                                 }
                             } else {
                                 // Standard Search: EXCLUDE Anime (Animation + JA)
                                 if (!(item.genre_ids && item.genre_ids.includes(16) && item.original_language === 'ja')) {
                                     searchResults.set(item.id, item);
                                 }
                             }
                        }
                    });
                }

                // 2. Search Person (to get their credits) - Skip for Anime Search to be strict
                if (!isAnimeSearch) {
                    const peopleRes = await fetch(`${BASE_URL}/search/person?api_key=${getTmdbApiKey(config)}&query=${encodeURIComponent(query)}`);
                    const peopleData = await peopleRes.json();
                    if (peopleData.results && peopleData.results.length > 0) {
                        // Take top person
                        const person = peopleData.results[0];
                        // Fetch credits
                        const creditsUrl = `${BASE_URL}/person/${person.id}/${tmdbType === "movie" ? "movie_credits" : "tv_credits"}?api_key=${getTmdbApiKey(config)}&language=it-IT`;
                        const creditsRes = await fetch(creditsUrl);
                        const creditsData = await creditsRes.json();
                        const castCredits = creditsData.cast || [];
                        const crewCredits = creditsData.crew || [];
                        
                        // Sort by popularity and add to results
                        const allCredits = [...castCredits, ...crewCredits].sort((a, b) => b.popularity - a.popularity);
                        
                        allCredits.forEach(item => {
                            const date = item.release_date || item.first_air_date;
                            if (date && date <= today && !searchResults.has(item.id)) {
                                // Standard Search: EXCLUDE Anime (Animation + JA)
                                if (!(item.genre_ids && item.genre_ids.includes(16) && item.original_language === 'ja')) {
                                    searchResults.set(item.id, item);
                                }
                            }
                        });
                    }
                }

                const results = Array.from(searchResults.values());
                const metas = await enrichAndMapItems(results, type, tmdbType, config, false, true, null, preferKitsuId);
                const orderedMetas = isAnimeSearch
                    ? sortMetasByReleaseDesc(metas)
                    : metas;
                console.log(`[Easy Catalogs] Search debug: query="${query}" type=${type} catalog=${id} results=${results.length} metas=${metas.length}`);
                return { metas: applyLandscapeToMetas(orderedMetas, landscapeForCatalog, config) };

            } catch (e) {
                console.error(`[Easy Catalogs] Search Error: ${e.message}`);
                return { metas: [] };
            }
        }

        // Handle Pagination
        // TMDB uses pages (1, 2, 3...), Stremio uses skip (0, 20, 40...)
        // We assume 20 items per page.
        const skip = resolvedExtra && resolvedExtra.skip ? Number(resolvedExtra.skip) || 0 : 0;
        const page = skip ? Math.floor(skip / 20) + 1 : 1;
        queryParams += `&page=${page}`;

        // Filter by Region IT for movies to exclude unreleased content in Italy
        if (tmdbType === "movie") {
            queryParams += "&region=IT";
        }
        
        // Handle Provider Catalogs (e.g. tmdb.movie.netflix or tmdb.movie.netflix_catalog)
        let providerFromId = null;
        let isCatalogOnly = false;

        // Check if ID matches a provider pattern
        const parts = id.split('.');
        if (parts.length >= 3) {
            let potentialSlug = parts[2];
            
            // Check for _catalog suffix (Availability Catalog)
            if (potentialSlug.endsWith('_catalog')) {
                isCatalogOnly = true;
                potentialSlug = potentialSlug.replace('_catalog', '');
            }

            // Find provider by slug
            let providerName = SLUG_TO_PROVIDER[potentialSlug];
            
            // Fallback: try matching sanitized name if not in map
            if (!providerName) {
                providerName = Object.keys(PROVIDERS).find(p => 
                    p.toLowerCase().replace(/[^a-z0-9]/g, '') === potentialSlug
                );
            }

            if (providerName) {
                providerFromId = normalizeProviderName(providerName);
            }
        }

        const providerRegion = providerFromId ? getProviderRegion(providerFromId) : null;
        const providerOriginalSourceId = providerFromId
            ? (tmdbType === "movie" ? COMPANY_IDS[providerFromId] : NETWORK_IDS[providerFromId])
            : null;
        const shouldMergeProviderExclusives = !!(providerFromId && !isCatalogOnly && providerOriginalSourceId);

        // Handle Year Catalog
        if (id === "tmdb.movie.year" || id === "tmdb.series.year") {
             if (tmdbType === "movie") {
                 endpoint = "discover/movie";
             } else {
                 endpoint = "discover/tv";
             }
             
             if (extra && extra.genre) {
                 const year = extra.genre;
                 if (/^\d{4}$/.test(year)) {
                     if (tmdbType === "movie") {
                         queryParams += `&primary_release_year=${year}&sort_by=popularity.desc`;
                     } else {
                         queryParams += `&first_air_date_year=${year}&sort_by=popularity.desc`;
                     }
                 }
             } else {
                  if (tmdbType === "movie") {
                         queryParams += `&primary_release_year=${currentYear}&sort_by=popularity.desc`;
                     } else {
                         queryParams += `&first_air_date_year=${currentYear}&sort_by=popularity.desc`;
                     }
             }
        } else if (id === "tmdb.movie.now_playing") {
            endpoint = "movie/now_playing";
            queryParams += "&region=IT";
        } else if (id === "tmdb.movie.kids" || id === "tmdb.series.kids") {
            if (tmdbType === "movie") {
                endpoint = "discover/movie";
                // Animation (16) OR Family (10751)
                queryParams += "&with_genres=16|10751&sort_by=popularity.desc&certification_country=IT&certification.lte=T";
            } else {
                endpoint = "discover/tv";
                // Kids (10762) OR Animation (16) OR Family (10751)
                queryParams += "&with_genres=10762|16|10751&sort_by=popularity.desc";
            }
        } else if (id === "tmdb.movie.anime" || id === "tmdb.series.anime") {
            let genres = "16"; // Animation
            
            if (tmdbType === "movie") {
                endpoint = "discover/movie";
                if (extra && extra.genre) {
                    const genreId = MOVIE_GENRES[extra.genre];
                    if (genreId) genres += `,${genreId}`;
                }
                // Animation (16) AND Language Japanese (ja)
                queryParams += `&with_genres=${genres}&with_original_language=ja&sort_by=popularity.desc`;
            } else {
                endpoint = "discover/tv";
                if (extra && extra.genre) {
                    const genreId = TV_GENRES[extra.genre];
                    if (genreId) genres += `,${genreId}`;
                }
                // Animation (16) AND Language Japanese (ja)
                queryParams += `&with_genres=${genres}&with_original_language=ja&sort_by=popularity.desc`;
            }
        } else if (id === "tmdb.movie.trending" || id === "tmdb.series.trending") {
            const timeWindow = (extra && extra.genre && extra.genre.toLowerCase() === "day") ? "day" : "week";
            endpoint = `trending/${tmdbType}/${timeWindow}`;
        } else if (providerFromId) {
            // Logic for Provider Catalog
            const providerId = PROVIDERS[providerFromId];
            const region = providerRegion;

            if (tmdbType === "movie") {
                endpoint = "discover/movie";
                const companyId = COMPANY_IDS[providerFromId];
                
                // If it's explicitly a Catalog request OR no company ID exists (fallback), use watch_providers
                if (isCatalogOnly || !companyId) {
                     queryParams += `&with_watch_providers=${providerId}&watch_region=${region}&sort_by=primary_release_date.desc&primary_release_date.lte=${new Date().toISOString().split('T')[0]}`;
                } else {
                    // It's an "Originals" request and Company ID exists
                    queryParams += `&with_companies=${companyId}&with_watch_providers=${providerId}&watch_region=${region}&sort_by=primary_release_date.desc&primary_release_date.lte=${new Date().toISOString().split('T')[0]}`;
                }
            } else {
                endpoint = "discover/tv";
                const networkId = NETWORK_IDS[providerFromId];
                
                if (isCatalogOnly || !networkId) {
                     queryParams += `&with_watch_providers=${providerId}&watch_region=${region}&sort_by=first_air_date.desc&first_air_date.lte=${new Date().toISOString().split('T')[0]}`;
                } else {
                     queryParams += `&with_networks=${networkId}&with_watch_providers=${providerId}&watch_region=${region}&sort_by=first_air_date.desc&first_air_date.lte=${new Date().toISOString().split('T')[0]}`;
                }
            }
            
            // Handle Genre Filter inside Provider Catalog
            if (extra && extra.genre) {
                const genre = extra.genre;
                let genreId = null;
                if (tmdbType === "movie") {
                    genreId = MOVIE_GENRES[genre];
                } else {
                    genreId = TV_GENRES[genre];
                }
                
                if (genreId) {
                    queryParams += `&with_genres=${genreId}`;
                }
            }
        }
        // Handle Genres and Filters (if not provider catalog, or if extra filters applied)
        else if (extra && extra.genre) {
            const genre = extra.genre;
            
            // Check if it's a provider slug first (e.g. from a legacy context, though standard Stremio passes genre string)
            // Or check if it matches our provider list
            const providerName = normalizeProviderName(genre);
            let providerId = PROVIDERS[providerName];
            
            if (providerId) {
                // Provider Logic (via Filter)
                const region = getProviderRegion(providerName);
                
                if (tmdbType === "movie") {
                    endpoint = "discover/movie";
                    const companyId = COMPANY_IDS[providerName];
                    if (companyId) {
                        queryParams += `&with_companies=${companyId}&sort_by=primary_release_date.desc&primary_release_date.lte=${new Date().toISOString().split('T')[0]}`;
                    } else {
                        queryParams += `&with_watch_providers=${providerId}&watch_region=${region}&sort_by=primary_release_date.desc&primary_release_date.lte=${new Date().toISOString().split('T')[0]}`;
                    }
                } else {
                    endpoint = "discover/tv";
                    const networkId = NETWORK_IDS[providerName];
                    if (networkId) {
                        queryParams += `&with_networks=${networkId}&sort_by=first_air_date.desc&first_air_date.lte=${new Date().toISOString().split('T')[0]}`;
                    } else {
                        queryParams += `&with_watch_providers=${providerId}&watch_region=${region}&sort_by=first_air_date.desc&first_air_date.lte=${new Date().toISOString().split('T')[0]}`;
                    }
                }
            } else {
                // Regular Genre Logic
                let genreId = null;
                if (tmdbType === "movie") {
                    genreId = MOVIE_GENRES[genre];
                    if (!endpoint) endpoint = "discover/movie";
                } else {
                    genreId = TV_GENRES[genre];
                    if (!endpoint) endpoint = "discover/tv";
                }

                if (genreId) {
                    queryParams += `&with_genres=${genreId}`;
                    if (!queryParams.includes("sort_by")) queryParams += `&sort_by=popularity.desc`;
                } else {
                    // Default fallback if genre not found
                    if (!endpoint) endpoint = tmdbType === "movie" ? "movie/popular" : "tv/popular";
                }
            }
        } else {
            // No filters, default lists
            if (id.includes("popular")) {
                if (tmdbType === "movie") {
                    endpoint = "discover/movie";
                    queryParams += "&sort_by=popularity.desc";
                } else {
                    endpoint = "tv/popular";
                }
            } else if (id.includes("trending")) {
                const timeWindow = (extra && extra.genre && extra.genre.toLowerCase() === "day") ? "day" : "week";
                endpoint = `trending/${tmdbType}/${timeWindow}`;
            } else if (id.includes("top_rated")) {
                if (tmdbType === "movie") {
                    endpoint = "discover/movie";
                    queryParams += "&sort_by=vote_average.desc&vote_count.gte=200";
                } else {
                    endpoint = "tv/top_rated";
                }
            } else if (id.includes("upcoming")) {
                if (tmdbType === "movie") {
                    endpoint = "movie/upcoming";
                    queryParams += "&region=IT";
                } else {
                    endpoint = "tv/on_the_air";
                }
            } else if (!endpoint) {
                // Fallback for unknown IDs
                 if (tmdbType === "movie") {
                    endpoint = "discover/movie";
                    queryParams += "&sort_by=popularity.desc";
                 } else {
                    endpoint = "tv/popular";
                 }
            }
        }

        // Add Date Filter for Discover endpoints to optimize
        if (endpoint && endpoint.includes("discover/movie")) {
            const today = new Date().toISOString().split('T')[0];
            if (!allowFuture && !queryParams.includes("primary_release_date.lte")) {
                queryParams += `&primary_release_date.lte=${today}`;
            }
            if (!queryParams.includes("with_release_type")) {
                // Optional: Force theatrical or digital to avoid events/premieres
                // queryParams += "&with_release_type=3|4"; 
            }
        }

        let metas = [];
        let fetchedPage = (extra && extra.skip ? Math.floor(extra.skip / 20) + 1 : 1);
        // Remove existing page param if present to handle it in loop
        queryParams = queryParams.replace(/&page=\d+/g, '');

        if (shouldMergeProviderExclusives) {
            const skip = resolvedExtra && resolvedExtra.skip ? Number(resolvedExtra.skip) || 0 : 0;
            let providerOffset = skip;
            let mergedAttempted = false;

            while (metas.length < 20) {
                const mergedResults = await fetchProviderOriginalMergedResults({
                    id,
                    tmdbType,
                    providerName: providerFromId,
                    extra: resolvedExtra,
                    config,
                    allowFuture,
                    skip: providerOffset
                });

                if (!mergedResults || mergedResults.items.length === 0) {
                    break;
                }

                mergedAttempted = true;
                const skipRegionCheck = false;
                const preferKitsuId = usesKitsuAnimeIds(id);
                const seriesAvailabilityRegion = tmdbType === "tv" ? mergedResults.region : null;
                const mapped = await enrichAndMapItems(
                    mergedResults.items,
                    type,
                    tmdbType,
                    config,
                    allowFuture,
                    skipRegionCheck,
                    seriesAvailabilityRegion,
                    preferKitsuId
                );

                if (mapped.length > 0) {
                    metas.push(...mapped);
                }

                if (mergedResults.items.length < 20) {
                    break;
                }

                providerOffset += mergedResults.items.length;
            }

            if (mergedAttempted) {
                return { metas: applyLandscapeToMetas(metas.slice(0, 20), landscapeForCatalog, config) };
            }
        }

        const pageOffset = skip && skip > 0 ? skip % 20 : 0;
        metas = await fetchCatalogMetasForQuery({
            endpoint,
            queryParams,
            id,
            type,
            tmdbType,
            config,
            allowFuture,
            providerRegion,
            startPage: fetchedPage,
            pageOffset
        });

        if (metas.length === 0 && providerFromId) {
            const fallbackRegions = getProviderRegions(providerFromId).slice(1);
            for (const fallbackRegion of fallbackRegions) {
                metas = await fetchCatalogMetasForQuery({
                    endpoint,
                    queryParams: replaceProviderQueryRegion(queryParams, fallbackRegion),
                    id,
                    type,
                    tmdbType,
                    config,
                    allowFuture,
                    providerRegion: fallbackRegion,
                    startPage: fetchedPage,
                    pageOffset
                });

                if (metas.length > 0) {
                    break;
                }
            }
        }

        return { metas: applyLandscapeToMetas(metas, landscapeForCatalog, config) };

    } catch (error) {
        console.error("[Easy Catalogs] Error:", error);
        return { metas: [] };
    }
});

const PORT = process.env.PORT || 7000;
const addonInterface = builder.getInterface();
// Update manifest catalogs AFTER interface creation but BEFORE router usage
addonInterface.manifest.catalogs = fullCatalogs;
const addonRouter = getRouter(addonInterface);

const app = express();

app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Access-Control-Allow-Methods', '*');
    next();
});

app.get('/configure', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'configure.html'));
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'configure.html'));
});

app.use(express.static('public'));

app.use((req, res, next) => {
    const segments = req.path.split('/').filter(Boolean);
    let config = {};
    
    if (segments.length > 0) {
        const first = segments[0];
        if (first !== 'api' && first !== 'manifest.json' && first !== 'configure' && first !== 'catalog' && first !== 'meta' && first !== 'stream' && first !== 'subtitles') {
            try {
                config = decodeConfigSegment(first);
                req.url = req.url.replace(`/${first}`, '');
                if (req.url === '') req.url = '/';
            } catch (e) {
                // Not a valid config
            }
        }
    }
    
    storage.run({ config }, () => {
        next();
    });
});

app.get('/configure', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'configure.html'));
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'configure.html'));
});

app.get('/manifest.json', (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Access-Control-Allow-Methods', '*');
    
    const config = getRequestConfig();
    const customCatalogNames = getConfiguredCatalogNames(config);
    const customCatalogShapes = getConfiguredCatalogShapes(config);
    
    let filteredCatalogs = [];
    
    if (config.catalogs) {
        const allowedKeys = [...new Set(
            config.catalogs
                .split(',')
                .map(item => item.trim())
                .filter(Boolean)
                .map(item => {
                    const isDiscoverOnly = item.endsWith('_d');
                    const rawLookupKey = isDiscoverOnly ? item.slice(0, -2) : item;
                    const normalizedLookupKey = normalizeConfiguredCatalogEntryKey(rawLookupKey);
                    return normalizedLookupKey
                        ? `${normalizedLookupKey}${isDiscoverOnly ? '_d' : ''}`
                        : "";
                })
                .filter(Boolean)
        )];
        
        allowedKeys.forEach(key => {
            // Check for Discover Only suffix
            let isDiscoverOnly = false;
            let lookupKey = key;
            if (key.endsWith('_d')) {
                isDiscoverOnly = true;
                lookupKey = key.substring(0, key.length - 2);
            }

            // Check for Standard Catalogs first
            const standardMap = {
                'upcoming_movie': 'tmdb.movie.upcoming',
                'upcoming_series': 'tmdb.series.upcoming',
                'now_playing_movie': 'tmdb.movie.now_playing',
                'popular_movie': 'tmdb.movie.popular',
                'popular_series': 'tmdb.series.popular',
                'trending_movie': 'tmdb.movie.trending',
                'trending_series': 'tmdb.series.trending',
                'top_rated_movie': 'tmdb.movie.top_rated',
                'top_rated_series': 'tmdb.series.top_rated',
                [TOP10_MOVIE_CONFIG_ID]: TOP10_MOVIE_MANIFEST_ID,
                [TOP10_SERIES_CONFIG_ID]: TOP10_SERIES_MANIFEST_ID,
                [LEGACY_TOP10_MOVIE_CONFIG_ID]: TOP10_MOVIE_MANIFEST_ID,
                [LEGACY_TOP10_SERIES_CONFIG_ID]: TOP10_SERIES_MANIFEST_ID,
                'kids_movie': 'tmdb.movie.kids',
                'kids_series': 'tmdb.series.kids',
                'anime_tmdb_series': 'tmdb.series.anime',
                'anime_tmdb_movie': 'tmdb.movie.anime',
                'anime_tmdb_search_series': 'tmdb.series.anime_search',
                'anime_tmdb_search_movie': 'tmdb.movie.anime_search',
                'anime_kitsu_popular_series': 'kitsu.series.popular',
                'anime_kitsu_latest_series': 'kitsu.series.latest',
                'anime_kitsu_popular_movie': 'kitsu.movie.popular',
                'anime_kitsu_latest_movie': 'kitsu.movie.latest',
                'anime_kitsu_ova': 'kitsu.series.ova',
                'anime_kitsu_latest_ova': 'kitsu.series.ova_latest',
                'anime_kitsu_ona': 'kitsu.series.ona',
                'anime_kitsu_latest_ona': 'kitsu.series.ona_latest',
                'anime_kitsu_special': 'kitsu.series.special',
                'anime_kitsu_latest_special': 'kitsu.series.special_latest',
                'year_movie': 'tmdb.movie.year',
                'year_series': 'tmdb.series.year',
                'search_movie': 'tmdb.movie.search',
                'search_series': 'tmdb.series.search',
                'anime_kitsu_search_movie': 'kitsu.movie.search',
                'anime_kitsu_search_series': 'kitsu.series.search',
                'anime_kitsu_search_ova': 'kitsu.series.ova_search',
                'anime_kitsu_search_ona': 'kitsu.series.ona_search',
                'anime_kitsu_search_special': 'kitsu.series.special_search',
                // Backward compatibility
                'anime_movie': 'tmdb.movie.anime',
                'anime_series': 'tmdb.series.anime',
                'anime_search_movie': 'tmdb.movie.anime_search',
                'anime_search_series': 'tmdb.series.anime_search',
                'anime_popular_series': 'kitsu.series.popular',
                'anime_popular_movie': 'kitsu.movie.popular',
                'anime_ova': 'kitsu.series.ova',
                'anime_ona': 'kitsu.series.ona',
                'anime_special': 'kitsu.series.special'
            };
            
            if (lookupKey === TOP10_GLOBAL_CATALOG_ID) {
                [TOP10_MOVIE_MANIFEST_ID, TOP10_SERIES_MANIFEST_ID].forEach(catalogId => {
                    const cat = fullCatalogs.find(c => c.id === catalogId);
                    if (!cat) return;

                    const resolvedCatalog = applyConfiguredCatalogShape(
                        applyConfiguredCatalogName(cat, lookupKey, customCatalogNames),
                        lookupKey,
                        customCatalogShapes
                    );
                    if (isDiscoverOnly) {
                        const catClone = { ...resolvedCatalog, extra: [...(resolvedCatalog.extra || [])] };
                        catClone.extra.push({ name: "discover", isRequired: true, options: ["Only"] });
                        filteredCatalogs.push(catClone);
                    } else {
                        filteredCatalogs.push(resolvedCatalog);
                    }
                });
            } else if (standardMap[lookupKey]) {
                const cat = fullCatalogs.find(c => c.id === standardMap[lookupKey]);
                if (cat) {
                    const resolvedCatalog = applyConfiguredCatalogShape(
                        applyConfiguredCatalogName(cat, lookupKey, customCatalogNames),
                        lookupKey,
                        customCatalogShapes
                    );
                    if (isDiscoverOnly) {
                        // Clone catalog to avoid mutating global state and add required filter
                        const catClone = { ...resolvedCatalog, extra: [...(resolvedCatalog.extra || [])] };
                        catClone.extra.push({ name: "discover", isRequired: true, options: ["Only"] });
                        filteredCatalogs.push(catClone);
                    } else {
                        filteredCatalogs.push(resolvedCatalog);
                    }
                }
            } else {
                // Check for Streaming Catalogs
                const matching = fullCatalogs.filter(c => {
                    // Skip standard catalogs here to avoid duplicates or mis-matches
                    if (c.id.includes('upcoming') || c.id.includes('popular') || c.id.includes('trending') || c.id.includes('top_rated') || c.id.includes('year') || c.id.includes('search')) return false;

                    const idParts = c.id.split('.');
                    const lastPart = idParts[idParts.length - 1]; // e.g. "netflix" or "netflix_catalog"
                    
                    let keyFromId = lastPart;
                    if (!lastPart.endsWith('_catalog') && !lastPart.endsWith('_top10')) {
                        keyFromId = lastPart + "_original";
                    }
                    
                    return keyFromId === lookupKey;
                });
                
                matching.forEach(m => {
                    const resolvedCatalog = applyConfiguredCatalogShape(
                        applyConfiguredCatalogName(m, lookupKey, customCatalogNames),
                        lookupKey,
                        customCatalogShapes
                    );
                    if (isDiscoverOnly) {
                        const mClone = { ...resolvedCatalog, extra: [...(resolvedCatalog.extra || [])] };
                        mClone.extra.push({ name: "discover", isRequired: true, options: ["Only"] });
                        filteredCatalogs.push(mClone);
                    } else {
                        filteredCatalogs.push(resolvedCatalog);
                    }
                });
            }
        });

        // REMOVED: Always append Search catalogs if not present
        // Now they are handled via standardMap configuration

    } else {
        // Default: Show all
        filteredCatalogs = fullCatalogs;
    }

    // Deduplicate just in case
    filteredCatalogs = [...new Set(filteredCatalogs)];
    if (!config.catalogs) {
        filteredCatalogs.sort((a, b) => {
            const top10Priority = Number(isTop10Catalog(b)) - Number(isTop10Catalog(a));
            if (top10Priority !== 0) return top10Priority;

            return Number(isSearchCatalog(a)) - Number(isSearchCatalog(b));
        });
    }

    const manifest = { ...addonInterface.manifest };
    manifest.catalogs = filteredCatalogs;
    manifest.resources = shouldReturnStreams(config)
        ? addonInterface.manifest.resources
        : addonInterface.manifest.resources.filter(resource =>
            !(resource && typeof resource === "object" && resource.name === "stream")
        );
    res.json(manifest);
});

app.use(addonRouter);

// WORKAROUND: Bypass 8KB limit by injecting full catalogs AFTER validation
// The 'catalogs' array itself might not be frozen, or we can replace the content if it is mutable.
// If the array is frozen, we might need a deeper hack.
// But let's try to mutate the array.
try {
    addonInterface.manifest.catalogs.length = 0; // Clear it
    fullCatalogs.forEach(c => addonInterface.manifest.catalogs.push(c)); // Push full list
    console.log("[Easy Catalogs] Successfully injected full catalogs list.");
} catch (e) {
    console.error("[Easy Catalogs] Failed to inject catalogs:", e);
}

app.listen(PORT, () => {
    console.log(`Addon active on http://localhost:${PORT}`);
});


