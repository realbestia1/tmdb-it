const { addonBuilder, getRouter } = require("stremio-addon-sdk");
const express = require('express');
const { AsyncLocalStorage } = require('async_hooks');
const storage = new AsyncLocalStorage();
const fetch = require("node-fetch");
const path = require('path');
const crypto = require('crypto');
const cache = require('./database');

const ADDON_URL = process.env.ADDON_URL || "http://localhost:7000";
const MAX_CONFIG_SEGMENT_LENGTH = 4096;
const MAX_CONFIG_JSON_LENGTH = 4096;
const MAX_CATALOG_SELECTIONS = 80;
const MAX_SEARCH_QUERY_LENGTH = 120;
const MAX_SEARCH_PERSON_CREDITS = 120;
const MAX_SEARCH_RESULTS = 60;
const RATE_LIMIT_WINDOW_MS = 60 * 1000;
const RATE_LIMIT_MAX_REQUESTS = 120;
const RATE_LIMIT_TRACKED_IPS = 5000;
const TOP_STREAMING_KEY_REGEX = /^[A-Za-z0-9_-]{1,128}$/;

function getClientIp(req) {
    const forwardedFor = req.headers['x-forwarded-for'];
    if (typeof forwardedFor === 'string' && forwardedFor.length > 0) {
        return forwardedFor.split(',')[0].trim();
    }
    return req.ip || req.socket.remoteAddress || 'unknown';
}

function createRateLimiter(windowMs, maxRequests, maxTrackedIps) {
    const requestsByIp = new Map();
    let requestsSinceCleanup = 0;

    return (req, res, next) => {
        const now = Date.now();
        const ip = getClientIp(req);
        const current = requestsByIp.get(ip);
        const freshWindow = !current || now - current.windowStart >= windowMs;
        const nextCount = freshWindow ? 1 : current.count + 1;
        requestsByIp.set(ip, {
            count: nextCount,
            windowStart: freshWindow ? now : current.windowStart
        });

        requestsSinceCleanup += 1;
        if (requestsSinceCleanup >= 1000 || requestsByIp.size > maxTrackedIps) {
            requestsSinceCleanup = 0;
            for (const [trackedIp, entry] of requestsByIp.entries()) {
                if (now - entry.windowStart >= windowMs) {
                    requestsByIp.delete(trackedIp);
                }
            }
            if (requestsByIp.size > maxTrackedIps) {
                const oldestIps = [...requestsByIp.entries()]
                    .sort((a, b) => a[1].windowStart - b[1].windowStart)
                    .slice(0, requestsByIp.size - maxTrackedIps);
                oldestIps.forEach(([trackedIp]) => requestsByIp.delete(trackedIp));
            }
        }

        if (nextCount > maxRequests) {
            res.status(429).json({ error: "Too many requests" });
            return;
        }

        next();
    };
}

function getConfigHash(config) {
    if (!config || Object.keys(config).length === 0) {
        return "default";
    }
    return crypto.createHash("sha256").update(JSON.stringify(config)).digest("hex").slice(0, 16);
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

// Helper function to enrich and map TMDB items to Stremio Meta objects
async function enrichAndMapItems(results, stremioType, tmdbType, allowFuture = false, skipRegionCheck = false, seriesAvailabilityRegion = null) {
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
        let tagline = null;

        // Basic genre mapping from IDs
        if (item.genre_ids) {
            genres = item.genre_ids.map(id => GENRE_ID_TO_NAME[id]).filter(Boolean);
        }

        // Fetch extended details (IMDb ID, Runtime, Cast, Director, Logo)
        try {
            const typePath = tmdbType === "movie" ? "movie" : "tv";
            const cacheKey = `tmdb:details:${typePath}:${item.id}`;
            let details = await cache.get(cacheKey);

            if (!details) {
                // Fetch external_ids, credits, and images in one go
                const detailsUrl = `${BASE_URL}/${typePath}/${item.id}?api_key=${TMDB_API_KEY}&language=it-IT&append_to_response=external_ids,credits,images,videos,release_dates&include_image_language=it,en,null&include_video_language=it,en,null`;
                
                const detailsRes = await fetch(detailsUrl);
                details = await detailsRes.json();
                
                if (details && !details.status_message) {
                    await cache.set(cacheKey, details, 86400 * 3); // 3 days
                }
            }
            
            // Improved Series Release Info
            if (tmdbType === "movie" && details.release_dates && details.release_dates.results) {
                const itRelease = details.release_dates.results.find(r => r.iso_3166_1 === "IT");
                if (itRelease && itRelease.release_dates && itRelease.release_dates.length > 0) {
                     const theatrical = itRelease.release_dates.find(d => d.type === 3);
                     if (theatrical) {
                         exactReleaseDate = theatrical.release_date.split('T')[0];
                     } else {
                         exactReleaseDate = itRelease.release_dates[0].release_date.split('T')[0];
                     }
                }
            } else if (stremioType === "series") {
                if (details.next_episode_to_air) {
                    exactReleaseDate = details.next_episode_to_air.air_date;
                } else if (details.last_air_date) {
                    exactReleaseDate = details.last_air_date;
                }
            }

            // Update releaseInfo to full date for movies (to show "2026-02-27" instead of "2026")
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
            
            // STRICT RELEASE DATE FILTER (Only for Movies)
            if (tmdbType === "movie" && !allowFuture && !skipRegionCheck) {
                let hasValidRelease = false;
                if (details.release_dates && details.release_dates.results) {
                    const itRelease = details.release_dates.results.find(r => r.iso_3166_1 === "IT");
                    if (itRelease && itRelease.release_dates) {
                        // Check if any release date in Italy is <= today
                        const today = new Date().toISOString().split('T')[0];
                        const valid = itRelease.release_dates.some(d => {
                            // Type 3 is Theatrical, 4 is Digital. 
                            // But we accept any release type as long as it happened in Italy.
                            // However, to be strict, we check the date.
                            return d.release_date && d.release_date.split('T')[0] <= today;
                        });
                        if (valid) hasValidRelease = true;
                    }
                }
                
                if (!hasValidRelease) {
                    // If no valid IT release found, return null to filter this item out
                    return null;
                }
            }

            // STRICT REGION AVAILABILITY FILTER (Only for TV series when requested)
            if (tmdbType === "tv" && seriesAvailabilityRegion) {
                const providersCacheKey = `tmdb:watchproviders:tv:${item.id}`;
                let providersData = await cache.get(providersCacheKey);
                if (!providersData) {
                    const providersUrl = `${BASE_URL}/tv/${item.id}/watch/providers?api_key=${TMDB_API_KEY}`;
                    const providersRes = await fetch(providersUrl);
                    providersData = await providersRes.json();
                    if (providersData && !providersData.status_message) {
                        await cache.set(providersCacheKey, providersData, 86400 * 3); // 3 days
                    }
                }

                const hasRegionAvailability = providersData &&
                    providersData.results &&
                    providersData.results[seriesAvailabilityRegion];
                if (!hasRegionAvailability) {
                    return null;
                }
            }
            
            if (details.external_ids && details.external_ids.imdb_id) {
                imdbId = details.external_ids.imdb_id;
                // Add IMDb Link for Rating Badge
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
            
            if (details.tagline) {
                tagline = details.tagline;
            }
            
            if (details.genres) {
                genres = details.genres.map(g => g.name);
                // Add Genre Links
                details.genres.forEach(g => {
                    links.push({
                        name: g.name,
                        category: "Genres",
                        url: `stremio:///discover/${encodeURIComponent(ADDON_URL + "/manifest.json")}/${stremioType}/tmdb.${stremioType === "movie" ? "movie" : "series"}.popular?genre=${encodeURIComponent(g.name)}`
                    });
                });
            }

            if (tmdbType === "movie") {
                if (details.runtime) runtime = `${details.runtime} min`;
            } else {
                if (details.episode_run_time && details.episode_run_time.length > 0) {
                    runtime = `${details.episode_run_time[0]} min`;
                }
            }

            if (details.credits) {
                if (details.credits.cast) {
                    cast = details.credits.cast.slice(0, 3).map(c => c.name); // Top 3 cast
                    // Add Cast Links
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
                    // Add Director Links
                    details.credits.crew.filter(c => c.job === "Director").forEach(c => {
                        links.push({
                            name: c.name,
                            category: "Directors",
                            url: `stremio:///search?search=${encodeURIComponent(c.name)}`
                        });
                    });
                }
            }
            
            // Check if we need to fetch logo
            if (details.images && details.images.logos && details.images.logos.length > 0) {
                    const logoItem = details.images.logos.find(l => l.iso_639_1 === "it") || 
                                    details.images.logos.find(l => l.iso_639_1 === "en") || 
                                    details.images.logos[0];
                    if (logoItem) {
                        logo = `https://image.tmdb.org/t/p/w500${logoItem.file_path}`;
                    }
            }

            // Extract Trailers from TMDB (only if available, no YT fallback to avoid slow catalog)
            if (details.videos && details.videos.results && details.videos.results.length > 0) {
                // Filter for Trailers on YouTube
                const tmdbTrailers = details.videos.results.filter(v => v.site === "YouTube" && v.type === "Trailer");
                
                // Sort to prioritize Italian trailers, then English, then higher quality (size)
                tmdbTrailers.sort((a, b) => {
                    const langA = a.iso_639_1 === "it" ? 2 : (a.iso_639_1 === "en" ? 1 : 0);
                    const langB = b.iso_639_1 === "it" ? 2 : (b.iso_639_1 === "en" ? 1 : 0);
                    
                    if (langA !== langB) {
                        return langB - langA;
                    }
                    
                    // If language is same, prioritize higher resolution (size)
                    // e.g. 2160 > 1080 > 720
                    return (b.size || 0) - (a.size || 0);
                });

                if (tmdbTrailers.length > 0) {
                    // Stremio expects trailers in specific format
                    tmdbTrailers.forEach(t => {
                        trailers.push({
                            source: t.key,
                            type: "Trailer"
                        });
                        trailerStreams.push({
                            title: t.name,
                            ytId: t.key
                        });
                    });
                }
            }

        } catch (e) {
            console.warn(`[TMDB Addon] Failed to fetch details for ${item.id}`, e);
        }

        const store = storage.getStore();
        const formattedDate = exactReleaseDate ? exactReleaseDate.split('-').reverse().join('/') : null;

        // Override releaseInfo for movies with DD/MM/YYYY
        if (tmdbType === "movie" && formattedDate) {
            releaseInfo = formattedDate;
        }

        const config = store ? store.config : {};
        let posterUrl = item.poster_path ? `https://image.tmdb.org/t/p/w500${item.poster_path}` : null;
        
        if (config.topStreamingKey) {
            if (imdbId) {
                const imdbStr = String(imdbId);
                const imdbForTopStreaming = imdbStr.toLowerCase().startsWith("tt") ? imdbStr : `tt${imdbStr}`;
                posterUrl = `https://api.top-streaming.stream/${config.topStreamingKey}/imdb/poster-default/${imdbForTopStreaming}.jpg`;
            } else {
                posterUrl = `https://api.top-streaming.stream/${config.topStreamingKey}/tmdb/poster-default/${item.id}.jpg`;
            }
        }

        return {
            id: imdbId ? imdbId : `tmdb:${item.id}`,
            type: stremioType, // Stremio type (movie/series)
            name: item.title || item.name,
            poster: posterUrl,
            background: item.backdrop_path ? `https://image.tmdb.org/t/p/original${item.backdrop_path}` : null,
            logo: logo,
            description: item.overview,
            releaseInfo: releaseInfo,
            released: exactReleaseDate ? new Date(exactReleaseDate).toISOString() : null,
            imdbRating: item.vote_average ? item.vote_average.toFixed(1) : null,
            runtime: runtime,
            genres: genres,
            cast: cast,
            director: director,
            links: links,
            trailers: trailers,
            trailerStreams: trailerStreams,
            behaviorHints: {
                defaultVideoId: stremioType === "movie" ? (imdbId || `tmdb:${item.id}`) : null,
                hasScheduledVideos: stremioType === "series"
            }
        };
    }));
    
    return metaObjects.filter(m => m !== null);
}

const TMDB_API_KEY = "68e094699525b18a70bab2f86b1fa706";
const BASE_URL = "https://api.themoviedb.org/3";

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
    "Netflix": 8, "Amazon Prime Video": 119, "Disney+": 337,
    "HBO Max": 1899,
    "Apple TV+": 350, "Paramount+": 531, "NOW": 39, "Sky Go": 29,
    "Rai Play": 222, "Mediaset Infinity": "359|110", "Timvision": 109,
    "Rakuten TV": 35
};

const COMPANY_IDS = {
    "Netflix": "178464|145172", // Netflix, Netflix Animation
    "Amazon Prime Video": "20580|21", // Amazon Studios, MGM
    "Disney+": "2|3|420|1|6125", // Disney, Pixar, Marvel, Lucasfilm, Disney Animation
    "Apple TV+": 194232,
    "HBO Max": "7429|174|128064|12|158691", // HBO Films, WB, DC Films, New Line, HBO Max
    "Paramount+": 4,
    "Rai Play": 1583, "Mediaset Infinity": 1677, "Sky Go": 19079, "NOW": 19079,
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
    "NOW": 2667, "Sky Go": 2667,
    "Timvision": 109 // Fallback ID if exists
};

const SLUG_TO_PROVIDER = {
    "netflix": "Netflix", "amazon": "Amazon Prime Video",
    "disney": "Disney+", "apple": "Apple TV+", "hbo": "HBO Max",
    "paramount": "Paramount+", "now": "NOW", "sky": "Sky Go",
    "rai": "Rai Play", "mediaset": "Mediaset Infinity",
    "timvision": "Timvision", "rakuten": "Rakuten TV"
};

const PROVIDER_SLUGS = {};
Object.entries(SLUG_TO_PROVIDER).forEach(([slug, name]) => {
    PROVIDER_SLUGS[name] = slug;
});

const STANDARD_CATALOG_MAP = {
    upcoming_movie: 'tmdb.movie.upcoming',
    upcoming_series: 'tmdb.series.upcoming',
    now_playing_movie: 'tmdb.movie.now_playing',
    popular_movie: 'tmdb.movie.popular',
    popular_series: 'tmdb.series.popular',
    trending_movie: 'tmdb.movie.trending',
    trending_series: 'tmdb.series.trending',
    top_rated_movie: 'tmdb.movie.top_rated',
    top_rated_series: 'tmdb.series.top_rated',
    kids_movie: 'tmdb.movie.kids',
    kids_series: 'tmdb.series.kids',
    anime_movie: 'tmdb.movie.anime',
    anime_series: 'tmdb.series.anime',
    year_movie: 'tmdb.movie.year',
    year_series: 'tmdb.series.year',
    search_movie: 'tmdb.movie.search',
    search_series: 'tmdb.series.search',
    anime_search_movie: 'tmdb.movie.anime_search',
    anime_search_series: 'tmdb.series.anime_search'
};

const ALLOWED_CATALOG_CONFIG_KEYS = new Set([
    ...Object.keys(STANDARD_CATALOG_MAP),
    ...Object.values(PROVIDER_SLUGS).flatMap(slug => [`${slug}_original`, `${slug}_catalog`])
]);

function sanitizeCatalogSelection(rawCatalogs) {
    if (typeof rawCatalogs !== "string" || rawCatalogs.length > MAX_CONFIG_JSON_LENGTH) {
        return null;
    }

    const sanitized = [];
    const seen = new Set();
    for (const rawKey of rawCatalogs.split(',')) {
        const trimmed = rawKey.trim();
        if (!trimmed) {
            continue;
        }
        const isDiscoverOnly = trimmed.endsWith('_d');
        const baseKey = isDiscoverOnly ? trimmed.slice(0, -2) : trimmed;
        if (!ALLOWED_CATALOG_CONFIG_KEYS.has(baseKey)) {
            continue;
        }
        const normalized = isDiscoverOnly ? `${baseKey}_d` : baseKey;
        if (seen.has(normalized)) {
            continue;
        }
        sanitized.push(normalized);
        seen.add(normalized);
        if (sanitized.length >= MAX_CATALOG_SELECTIONS) {
            break;
        }
    }

    return sanitized.length > 0 ? sanitized.join(',') : null;
}

function sanitizeConfig(rawConfig) {
    if (!rawConfig || typeof rawConfig !== "object" || Array.isArray(rawConfig)) {
        return {};
    }

    const safeConfig = {};
    if (typeof rawConfig.topStreamingKey === "string") {
        const key = rawConfig.topStreamingKey.trim();
        if (TOP_STREAMING_KEY_REGEX.test(key)) {
            safeConfig.topStreamingKey = key;
        }
    }

    const safeCatalogs = sanitizeCatalogSelection(rawConfig.catalogs);
    if (safeCatalogs) {
        safeConfig.catalogs = safeCatalogs;
    }

    return safeConfig;
}

const manifest = {
    id: "org.bestia.tmdb",
    version: "1.0.19",
    name: "Miglior Catalogo Italiano",
    description: "Miglior Catalogo Italiano per Stremio/Nuvio",
    resources: ["catalog", "meta"],
    types: ["movie", "series"],
    catalogs: [], // Start empty, populate later to bypass 8KB limit check
    idPrefixes: ["tmdb", "tt"]
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
            name: "TMDB",
            extra: [{ name: "search", isRequired: true }]
        },
        {
            type: "series",
            id: "tmdb.series.search",
            name: "TMDB",
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
});

// Add Anime Catalogs at the end
fullCatalogs.push({
    type: "movie",
    id: "tmdb.movie.anime",
    name: "Anime",
    extra: [{ 
        name: "genre", 
        isRequired: false,
        options: Object.keys(MOVIE_GENRES) 
    }, { name: "skip", isRequired: false }]
});
fullCatalogs.push({
    type: "series",
    id: "tmdb.series.anime",
    name: "Anime",
    extra: [{ 
        name: "genre", 
        isRequired: false,
        options: Object.keys(TV_GENRES) 
    }, { name: "skip", isRequired: false }]
});

// Anime Search Catalogs
fullCatalogs.push({
    type: "movie",
    id: "tmdb.movie.anime_search",
    name: "Anime",
    extra: [{ name: "search", isRequired: true }]
});
fullCatalogs.push({
    type: "series",
    id: "tmdb.series.anime_search",
    name: "Anime",
    extra: [{ name: "search", isRequired: true }]
});

// Use a minimal catalog list for initial builder creation (to bypass 8KB limit check)
// We need at least one catalog so that validation passes for 'catalog' handler
manifest.catalogs = [ fullCatalogs[0] ];

const builder = new addonBuilder(manifest);

// Metadata Handler
builder.defineMetaHandler(async ({ type, id }) => {
    console.log(`[TMDB Addon] Meta Request: type=${type} id=${id}`);
    
    const store = storage.getStore();
    const configHash = getConfigHash(store ? store.config : null);
    const cacheKey = `meta_v3:${type}:${id}:${configHash}`;
    
    const cached = await cache.get(cacheKey);
    if (cached) return { meta: cached };
    
    let tmdbId = id;
    // If it's an IMDB ID (tt...), we need to find the TMDB ID first, or use find logic if supported.
    // TMDB API supports find by external ID.
    
    let url = "";
    if (id.startsWith("tt")) {
        url = `${BASE_URL}/find/${id}?api_key=${TMDB_API_KEY}&external_source=imdb_id`;
    } else if (id.startsWith("tmdb:")) {
        tmdbId = id.split(":")[1];
        url = `${BASE_URL}/${type === "series" ? "tv" : "movie"}/${tmdbId}?api_key=${TMDB_API_KEY}&language=it-IT&append_to_response=credits,similar,videos,images,external_ids,release_dates&include_image_language=it,en,null&include_video_language=it,en,null`;
    } else {
        // Assume it is a raw TMDB ID if it's just numbers, though Stremio usually prefixes.
        // But if it comes from our catalog, it might be prefixed.
        // Let's assume standard TMDB ID for safety if numeric.
         url = `${BASE_URL}/${type === "series" ? "tv" : "movie"}/${id}?api_key=${TMDB_API_KEY}&language=it-IT&append_to_response=credits,similar,videos,images,external_ids,release_dates&include_image_language=it,en,null&include_video_language=it,en,null`;
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
                // Now fetch full details
                const detailsUrl = `${BASE_URL}/${type === "series" ? "tv" : "movie"}/${item.id}?api_key=${TMDB_API_KEY}&language=it-IT&append_to_response=credits,similar,videos,images,external_ids,release_dates&include_image_language=it,en,null&include_video_language=it,en,null`;
                const detailsRes = await fetch(detailsUrl);
                const details = await detailsRes.json();
                 meta = await transformToMeta(details, type);
             }
        } else {
            meta = await transformToMeta(data, type);
        }

        if (meta) {
            await cache.set(cacheKey, meta, 86400 * 3); // Cache for 3 days
            return { meta };
        } else {
            return { meta: {} };
        }

    } catch (e) {
        console.error(`[TMDB Addon] Meta Error: ${e.message}`);
        return { meta: {} };
    }
});

async function transformToMeta(item, type) {
    const isMovie = type === "movie";
    const year = item.release_date ? item.release_date.split("-")[0] : (item.first_air_date ? item.first_air_date.split("-")[0] : "");
    
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
    
    const store = storage.getStore();
    const config = store ? store.config : {};

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
    
    if (config.topStreamingKey) {
        if (imdbId) {
            const imdbStr = String(imdbId);
            const imdbForTopStreaming = imdbStr.toLowerCase().startsWith("tt") ? imdbStr : `tt${imdbStr}`;
            poster = `https://api.top-streaming.stream/${config.topStreamingKey}/imdb/poster-default/${imdbForTopStreaming}.jpg`;
        } else {
            poster = `https://api.top-streaming.stream/${config.topStreamingKey}/tmdb/poster-default/${item.id}.jpg`;
        }
    }

    // Fetch Seasons and Episodes for Series
    let videos = [];
    if (!isMovie && item.seasons) {
        try {
            const seasonPromises = item.seasons.map(season => {
                 // Skip seasons with 0 episodes if likely placeholder, but keep specials (season 0) if they have content
                 if (season.episode_count === 0) return null; 
                 const seasonUrl = `${BASE_URL}/tv/${item.id}/season/${season.season_number}?api_key=${TMDB_API_KEY}&language=it-IT`;
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

                        const episodeNumber = shouldRenumber ? (index + 1) : ep.episode_number;

                        videos.push({
                            id: `${idPrefix}:${ep.season_number}:${episodeNumber}`,
                            title: ep.name,
                            released: released,
                            thumbnail: ep.still_path 
                                ? `https://image.tmdb.org/t/p/w500${ep.still_path}` 
                                : (cinemetaThumb || (item.backdrop_path ? `https://image.tmdb.org/t/p/w500${item.backdrop_path}` : null)),
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
            console.error(`[TMDB Addon] Error fetching episodes for ${item.id}:`, e);
        }
    }

    return {
        id: imdbId || `tmdb:${item.id}`,
        type: type,
        name: item.title || item.name,
        poster: poster,
        background: item.backdrop_path ? `https://image.tmdb.org/t/p/original${item.backdrop_path}` : "",
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
                url: `stremio:///discover/${encodeURIComponent(ADDON_URL + "/manifest.json")}/${type}/tmdb.${type === "movie" ? "movie" : "series"}.popular?genre=${encodeURIComponent(g.name)}`
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
    console.log(`[TMDB Addon] Request: type=${type} id=${id} extra=${JSON.stringify(extra)}`);
    
    // Convert Stremio type to TMDB type
    const tmdbType = type === "series" ? "tv" : "movie";
    const allowFuture = id.includes("upcoming");
    
    try {
        let endpoint = null;
        let queryParams = `api_key=${TMDB_API_KEY}&language=it-IT`;

        // Handle Search
        if (extra && typeof extra.search === "string" && extra.search.trim().length > 0) {
            const query = extra.search.trim();
            if (query.length > MAX_SEARCH_QUERY_LENGTH) {
                return { metas: [] };
            }
            const searchResults = new Map(); // Use Map to deduplicate by ID
            const today = new Date().toISOString().split('T')[0];
            const isAnimeSearch = id.includes("anime_search");

            try {
                // 1. Search Content (Movie/TV)
                const contentRes = await fetch(`${BASE_URL}/search/${tmdbType}?api_key=${TMDB_API_KEY}&query=${encodeURIComponent(query)}&language=it-IT`);
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
                    const peopleRes = await fetch(`${BASE_URL}/search/person?api_key=${TMDB_API_KEY}&query=${encodeURIComponent(query)}`);
                    const peopleData = await peopleRes.json();
                    if (peopleData.results && peopleData.results.length > 0) {
                        // Take top person
                        const person = peopleData.results[0];
                        // Fetch credits
                        const creditsUrl = `${BASE_URL}/person/${person.id}/${tmdbType === "movie" ? "movie_credits" : "tv_credits"}?api_key=${TMDB_API_KEY}&language=it-IT`;
                        const creditsRes = await fetch(creditsUrl);
                        const creditsData = await creditsRes.json();
                        const castCredits = creditsData.cast || [];
                        const crewCredits = creditsData.crew || [];
                        
                        // Sort by popularity and add to results
                        const allCredits = [...castCredits, ...crewCredits].sort((a, b) => b.popularity - a.popularity);
                        
                        allCredits.slice(0, MAX_SEARCH_PERSON_CREDITS).forEach(item => {
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

                const results = Array.from(searchResults.values()).slice(0, MAX_SEARCH_RESULTS);
                const seriesAvailabilityRegionForSearch = (tmdbType === "tv" && !id.includes("anime")) ? "IT" : null;
                const metas = await enrichAndMapItems(results, type, tmdbType, false, true, seriesAvailabilityRegionForSearch);
                return { metas: metas.slice(0, 20) };

            } catch (e) {
                console.error(`[TMDB Addon] Search Error: ${e.message}`);
                return { metas: [] };
            }
        }

        // Handle Pagination
        // TMDB uses pages (1, 2, 3...), Stremio uses skip (0, 20, 40...)
        // We assume 20 items per page.
        const page = extra && extra.skip ? Math.floor(extra.skip / 20) + 1 : 1;
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
                providerFromId = providerName;
            }
        }

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
            const region = providerFromId === "HBO Max" ? "US" : "IT";

            if (tmdbType === "movie") {
                endpoint = "discover/movie";
                const companyId = COMPANY_IDS[providerFromId];
                
                // If it's explicitly a Catalog request OR no company ID exists (fallback), use watch_providers
                if (isCatalogOnly || !companyId) {
                     queryParams += `&with_watch_providers=${providerId}&watch_region=${region}&sort_by=popularity.desc&primary_release_date.lte=${new Date().toISOString().split('T')[0]}`;
                } else {
                    // It's an "Originals" request and Company ID exists
                    queryParams += `&with_companies=${companyId}&with_watch_providers=${providerId}&watch_region=${region}&sort_by=popularity.desc&primary_release_date.lte=${new Date().toISOString().split('T')[0]}`;
                }
            } else {
                endpoint = "discover/tv";
                const networkId = NETWORK_IDS[providerFromId];
                
                if (isCatalogOnly || !networkId) {
                     queryParams += `&with_watch_providers=${providerId}&watch_region=${region}&sort_by=popularity.desc&first_air_date.lte=${new Date().toISOString().split('T')[0]}`;
                } else {
                     queryParams += `&with_networks=${networkId}&with_watch_providers=${providerId}&watch_region=${region}&sort_by=popularity.desc&first_air_date.lte=${new Date().toISOString().split('T')[0]}`;
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
            
            // Check if it's a provider slug first (e.g. from Nuvio context, though standard Stremio passes genre string)
            // Or check if it matches our provider list
            let providerId = PROVIDERS[genre];
            
            if (providerId) {
                // Provider Logic (via Filter)
                const region = genre === "HBO Max" ? "US" : "IT";
                
                if (tmdbType === "movie") {
                    endpoint = "discover/movie";
                    const companyId = COMPANY_IDS[genre];
                    if (companyId) {
                        queryParams += `&with_companies=${companyId}&sort_by=popularity.desc&primary_release_date.lte=${new Date().toISOString().split('T')[0]}`;
                    } else {
                        queryParams += `&with_watch_providers=${providerId}&watch_region=${region}&sort_by=popularity.desc&primary_release_date.lte=${new Date().toISOString().split('T')[0]}`;
                    }
                } else {
                    endpoint = "discover/tv";
                    const networkId = NETWORK_IDS[genre];
                    if (networkId) {
                        queryParams += `&with_networks=${networkId}&sort_by=popularity.desc&first_air_date.lte=${new Date().toISOString().split('T')[0]}`;
                    } else {
                        queryParams += `&with_watch_providers=${providerId}&watch_region=${region}&sort_by=popularity.desc&first_air_date.lte=${new Date().toISOString().split('T')[0]}`;
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
        let maxPagesToFetch = 3; // Limit to prevent excessive API calls
        
        // Remove existing page param if present to handle it in loop
        queryParams = queryParams.replace(/&page=\d+/g, '');

        while (metas.length < 20 && maxPagesToFetch > 0) {
             const currentUrl = `${BASE_URL}/${endpoint}?${queryParams}&page=${fetchedPage}`;
             console.log(`[TMDB Addon] Fetching Page ${fetchedPage}: ${currentUrl}`);
             
             try {
                 const response = await fetch(currentUrl);
                 const data = await response.json();
                 
                 if (!data.results || data.results.length === 0) break;
                 
                 // Basic Filter: Remove future content or content without date
                 // This ensures Popular, Trending, Top Rated also respect the rule
                 const today = new Date().toISOString().split('T')[0];
                 const filteredResults = data.results.filter(item => {
                    const date = item.release_date || item.first_air_date;

                    // EXCLUDE Anime (Animation + JA) from non-Anime catalogs
                    if (!id.includes("anime")) {
                        if (item.genre_ids && item.genre_ids.includes(16) && item.original_language === 'ja') {
                            return false;
                        }
                    }

                    if (allowFuture) return !!date;
                    return date && date <= today;
               });
                
                if (filteredResults.length > 0) {
                    const skipRegionCheck = (id === "tmdb.movie.anime" || id === "tmdb.series.anime");
                    let seriesAvailabilityRegion = null;
                    if (tmdbType === "tv" && !id.includes("anime")) {
                        // Default strict region for all series catalogs
                        seriesAvailabilityRegion = "IT";
                        // Provider exception
                        if (providerFromId === "HBO Max") {
                            seriesAvailabilityRegion = "US";
                        }
                    }
                    const newMetas = await enrichAndMapItems(filteredResults, type, tmdbType, allowFuture, skipRegionCheck, seriesAvailabilityRegion);
                    metas = metas.concat(newMetas);
                }
                 
                 fetchedPage++;
                 maxPagesToFetch--;
                 
             } catch (e) {
                 console.error(`[TMDB Addon] Fetch Error on page ${fetchedPage}:`, e);
                 break;
             }
        }

        return { metas: metas.slice(0, 20) };

    } catch (error) {
        console.error("[TMDB Addon] Error:", error);
        return { metas: [] };
    }
});

const PORT = process.env.PORT || 7000;
const addonInterface = builder.getInterface();
// Update manifest catalogs AFTER interface creation but BEFORE router usage
addonInterface.manifest.catalogs = fullCatalogs;
const addonRouter = getRouter(addonInterface);

const app = express();
const apiRateLimiter = createRateLimiter(
    RATE_LIMIT_WINDOW_MS,
    RATE_LIMIT_MAX_REQUESTS,
    RATE_LIMIT_TRACKED_IPS
);

app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Access-Control-Allow-Methods', '*');
    next();
});
app.use(apiRateLimiter);

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
        if (first !== 'manifest.json' && first !== 'configure' && first !== 'catalog' && first !== 'meta' && first !== 'stream' && first !== 'subtitles') {
            try {
                if (first.length <= MAX_CONFIG_SEGMENT_LENGTH) {
                    const configStr = Buffer.from(first, 'base64').toString('utf-8');
                    if (configStr.length <= MAX_CONFIG_JSON_LENGTH) {
                        config = sanitizeConfig(JSON.parse(configStr));
                    }
                }
                if (req.url.startsWith(`/${first}`)) {
                    req.url = req.url.slice(first.length + 1) || '/';
                }
            } catch (e) {
                // Not a valid config
            }
        }
    }
    
    storage.run({ config }, () => {
        next();
    });
});

app.get('/manifest.json', (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Access-Control-Allow-Methods', '*');
    
    const store = storage.getStore();
    const config = store ? store.config : {};
    
    let filteredCatalogs = [];
    
    if (config.catalogs) {
        const allowedKeys = config.catalogs.split(',');
        
        allowedKeys.forEach(key => {
            // Check for Discover Only suffix
            let isDiscoverOnly = false;
            let lookupKey = key;
            if (key.endsWith('_d')) {
                isDiscoverOnly = true;
                lookupKey = key.substring(0, key.length - 2);
            }

            // Check for Standard Catalogs first
            if (STANDARD_CATALOG_MAP[lookupKey]) {
                const cat = fullCatalogs.find(c => c.id === STANDARD_CATALOG_MAP[lookupKey]);
                if (cat) {
                    if (isDiscoverOnly) {
                        // Clone catalog to avoid mutating global state and add required filter
                        const catClone = { ...cat, extra: [...(cat.extra || [])] };
                        catClone.extra.push({ name: "discover", isRequired: true, options: ["Only"] });
                        filteredCatalogs.push(catClone);
                    } else {
                        filteredCatalogs.push(cat);
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
                    if (!lastPart.endsWith('_catalog')) {
                        keyFromId = lastPart + "_original";
                    }
                    
                    return keyFromId === lookupKey;
                });
                
                matching.forEach(m => {
                    if (isDiscoverOnly) {
                        const mClone = { ...m, extra: [...(m.extra || [])] };
                        mClone.extra.push({ name: "discover", isRequired: true, options: ["Only"] });
                        filteredCatalogs.push(mClone);
                    } else {
                        filteredCatalogs.push(m);
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

    const manifest = { ...addonInterface.manifest };
    manifest.catalogs = filteredCatalogs;
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
    console.log("[TMDB Addon] Successfully injected full catalogs list.");
} catch (e) {
    console.error("[TMDB Addon] Failed to inject catalogs:", e);
}

app.listen(PORT, () => {
    console.log(`Addon active on http://localhost:${PORT}`);
});
