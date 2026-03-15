var __async = (__this, __arguments, generator) => {
  return new Promise((resolve, reject) => {
    var fulfilled = (value) => {
      try {
        step(generator.next(value));
      } catch (e) {
        reject(e);
      }
    };
    var rejected = (value) => {
      try {
        step(generator.throw(value));
      } catch (e) {
        reject(e);
      }
    };
    var step = (x) => x.done ? resolve(x.value) : Promise.resolve(x.value).then(fulfilled, rejected);
    step((generator = generator.apply(__this, __arguments)).next());
  });
};
const TMDB_API_KEY = "68e094699525b18a70bab2f86b1fa706";
const BASE_URL = "https://api.themoviedb.org/3";
const MOVIE_GENRES = {
  "Azione": 28,
  "Avventura": 12,
  "Animazione": 16,
  "Commedia": 35,
  "Crime": 80,
  "Documentario": 99,
  "Dramma": 18,
  "Famiglia": 10751,
  "Fantasy": 14,
  "Storia": 36,
  "Horror": 27,
  "Musica": 10402,
  "Mistero": 9648,
  "Romance": 10749,
  "Fantascienza": 878,
  "Thriller": 53,
  "Guerra": 10752,
  "Western": 37
};
const TV_GENRES = {
  "Action & Adventure": 10759,
  "Animazione": 16,
  "Commedia": 35,
  "Crime": 80,
  "Documentario": 99,
  "Dramma": 18,
  "Famiglia": 10751,
  "Kids": 10762,
  "Mistero": 9648,
  "News": 10763,
  "Reality": 10764,
  "Sci-Fi & Fantasy": 10765,
  "Soap": 10766,
  "Talk": 10767,
  "War & Politics": 10768,
  "Western": 37
};
const PROVIDERS = {
  "Netflix": 8,
  "Amazon Prime Video": 119,
  "Disney+": 337,
  "Apple TV+": 350,
  "Paramount+": 531,
  "Discovery+": 524,
  "NOW": 39,
  "Sky Go": 29,
  "Rai Play": 222,
  "Mediaset Infinity": 359,
  "Timvision": 109,
  "Rakuten TV": 35,
  "Infinity+": 110,
  "HBO Max": 1899
};
const COMPANY_IDS = {
  "Netflix": 178464,
  // Netflix Company
  "Amazon Prime Video": 20580,
  // Amazon Studios
  "Disney+": 2,
  // Walt Disney Pictures
  "Apple TV+": 194232,
  // Apple Studios
  "HBO Max": 7429,
  // HBO Films
  "Paramount+": 4,
  // Paramount Pictures (ID 4)
  "Rai Play": 1583,
  // RAI
  "Mediaset Infinity": 1677,
  // Mediaset
  "Sky Go": 19079,
  // Sky
  "NOW": 19079
  // Sky
};
const NETWORK_IDS = {
  "Netflix": 213,
  "Amazon Prime Video": 1024,
  "Disney+": 2739,
  "Apple TV+": 2552,
  "HBO Max": 49,
  // HBO
  "Paramount+": 4330,
  "Rai Play": "3463|533|236|1583",
  // RaiPlay | Rai 1 | Rai 2 | RAI (Company)
  "Mediaset Infinity": "537|402|1677",
  // Canale 5 | Italia 1 | Mediaset (Company)
  "NOW": 2667,
  // Sky Atlantic (Italia)
  "Sky Go": 2667
  // Sky Atlantic (Italia)
};
const SLUG_TO_PROVIDER = {
  "netflix": "Netflix",
  "amazon": "Amazon Prime Video",
  "disney": "Disney+",
  "apple": "Apple TV+",
  "hbo": "HBO Max",
  "paramount": "Paramount+",
  "discoverypluseu": "Discovery+",
  "now": "NOW",
  "sky": "Sky Go",
  "rai": "Rai Play",
  "mediaset": "Mediaset Infinity"
};
function getCatalog(type, id, filters) {
  return __async(this, null, function* () {
    console.log(`[TMDB] getCatalog type=${type} id=${id} filters=${JSON.stringify(filters)}`);
    try {
      let endpoint = null;
      let queryParams = `api_key=${TMDB_API_KEY}&language=it-IT`;
      let providerFromId = null;
      const parts = id.split(".");
      if (parts.length === 3 && SLUG_TO_PROVIDER[parts[1]]) {
        providerFromId = SLUG_TO_PROVIDER[parts[1]];
      }
      if (providerFromId) {
        const providerId = PROVIDERS[providerFromId];
        if (type === "movie") {
          const companyId = COMPANY_IDS[providerFromId];
          endpoint = "discover/movie";
          if (companyId) queryParams += `&with_companies=${companyId}`;
          else if (providerId) queryParams += `&with_watch_providers=${providerId}&watch_region=IT`;
        } else {
          const networkId = NETWORK_IDS[providerFromId];
          endpoint = "discover/tv";
          if (networkId) queryParams += `&with_networks=${networkId}`;
          else if (providerId) queryParams += `&with_watch_providers=${providerId}&watch_region=IT`;
        }
        queryParams += `&sort_by=popularity.desc`;
      }
      if (filters && filters.genre) {
        const genre = filters.genre;
        const isYear = /^\d{4}(-\d{4})?$/.test(genre);
        const providerId = !providerFromId ? PROVIDERS[genre] : null;
        if (isYear) {
          if (type === "movie") {
            endpoint = "discover/movie";
            if (genre.includes("-")) {
              const [start, end] = genre.split("-");
              queryParams += `&primary_release_date.gte=${start}-01-01&primary_release_date.lte=${end}-12-31&sort_by=popularity.desc`;
            } else {
              queryParams += `&primary_release_year=${genre}&sort_by=popularity.desc`;
            }
          } else {
            endpoint = "discover/tv";
            if (genre.includes("-")) {
              const [start, end] = genre.split("-");
              queryParams += `&first_air_date.gte=${start}-01-01&first_air_date.lte=${end}-12-31&sort_by=popularity.desc`;
            } else {
              queryParams += `&first_air_date_year=${genre}&sort_by=popularity.desc`;
            }
          }
        } else if (providerId) {
          if (type === "movie") {
            endpoint = "discover/movie";
            const companyId = COMPANY_IDS[genre];
            if (companyId) {
              queryParams += `&with_companies=${companyId}&sort_by=popularity.desc`;
            } else {
              console.warn(`[TMDB] No Company ID for ${genre}, using provider filter instead.`);
              queryParams += `&with_watch_providers=${providerId}&watch_region=IT&sort_by=popularity.desc`;
            }
          } else {
            endpoint = "discover/tv";
            const networkId = NETWORK_IDS[genre];
            if (networkId) {
              queryParams += `&with_networks=${networkId}&sort_by=popularity.desc`;
            } else {
              console.warn(`[TMDB] No Network ID for ${genre}, using provider filter instead.`);
              queryParams += `&with_watch_providers=${providerId}&watch_region=IT&sort_by=popularity.desc`;
            }
          }
        } else {
          let genreId = null;
          if (type === "movie") {
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
            console.warn(`[TMDB] Genre '${genre}' not found map.`);
          }
        }
      }
      if (!endpoint) {
        if (type === "movie") {
          if (id === "tmdb.movie.popular") endpoint = "movie/popular";
          else if (id === "tmdb.movie.trending") endpoint = "trending/movie/week";
          else if (id === "tmdb.movie.top_rated") endpoint = "movie/top_rated";
          else endpoint = "movie/popular";
        } else if (type === "tv") {
          if (id === "tmdb.tv.popular") endpoint = "tv/popular";
          else if (id === "tmdb.tv.trending") endpoint = "trending/tv/week";
          else if (id === "tmdb.tv.top_rated") endpoint = "tv/top_rated";
          else endpoint = "tv/popular";
        }
      }
      const url = `${BASE_URL}/${endpoint}?${queryParams}`;
      const response = yield fetch(url);
      const data = yield response.json();
      if (!data.results) return [];
      const resultsWithImdb = yield Promise.all(data.results.map((item) => __async(null, null, function* () {
        let imdbId = null;
        try {
          const typePath = type === "movie" ? "movie" : "tv";
          const detailsUrl = `${BASE_URL}/${typePath}/${item.id}/external_ids?api_key=${TMDB_API_KEY}`;
          const detailsRes = yield fetch(detailsUrl);
          const details = yield detailsRes.json();
          if (details.imdb_id) imdbId = details.imdb_id;
        } catch (e) {
          console.warn(`[TMDB] Failed to fetch IMDB ID for ${item.id}`, e);
        }
        return {
          id: imdbId ? imdbId : `tmdb:${item.id}`,
          name: item.title || item.name,
          poster: item.poster_path ? `https://image.tmdb.org/t/p/w500${item.poster_path}` : null,
          description: item.overview,
          type,
          releaseDate: item.release_date || item.first_air_date,
          rating: item.vote_average
        };
      })));
      return resultsWithImdb;
    } catch (error) {
      console.error("[TMDB] Error fetching catalog:", error);
      return [];
    }
  });
}
module.exports = { getCatalog };
