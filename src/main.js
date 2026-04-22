/**
 * Justia Lawyer Directory Scraper — Apify Actor
 *
 * Two-phase extraction:
 *   Phase 1: Scrape listing pages (div.jld-card) for bulk attorney data
 *   Phase 2: Enrich via individual profile pages (JSON-LD + HTML fallback)
 *
 * Requires US residential proxies — Justia uses Cloudflare IP-tier blocking.
 */

import { Actor, log } from 'apify';
import { CheerioCrawler } from 'crawlee';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

// ── Blocked website hosts (social media, directories) ──────────────────────
const BLOCKED_WEBSITE_HOSTS = new Set([
    'facebook.com', 'www.facebook.com',
    'twitter.com', 'www.twitter.com', 'x.com', 'www.x.com',
    'linkedin.com', 'www.linkedin.com',
    'instagram.com', 'www.instagram.com',
    'youtube.com', 'www.youtube.com',
    'tiktok.com', 'www.tiktok.com',
    'justia.com', 'www.justia.com',
    'lawyers.justia.com',
    'avvo.com', 'www.avvo.com',
    'findlaw.com', 'www.findlaw.com',
    'martindale.com', 'www.martindale.com',
    'yelp.com', 'www.yelp.com',
]);

function isBlockedWebsite(url) {
    if (!url) return true;
    try {
        const hostname = new URL(url).hostname.toLowerCase();
        return BLOCKED_WEBSITE_HOSTS.has(hostname);
    } catch {
        return true; // malformed URL
    }
}

// ── Cloudflare detection ───────────────────────────────────────────────────
function isCloudflareBlocked(html) {
    const snippet = (html || '').substring(0, 5000);
    return (
        snippet.includes('Just a moment') ||
        snippet.includes('cf-browser-verification') ||
        snippet.includes('Checking your browser') ||
        snippet.includes('Cloudflare')
    );
}

// ── JSON-LD extraction from profile pages ──────────────────────────────────
const LEGAL_JSONLD_TYPES = new Set([
    'Attorney', 'Person', 'LegalService', 'LocalBusiness',
]);

function extractJsonLd($) {
    const scripts = $('script[type="application/ld+json"]');
    for (let i = 0; i < scripts.length; i++) {
        try {
            const data = JSON.parse($(scripts[i]).html());
            const items = Array.isArray(data) ? data : [data];
            for (const item of items) {
                const types = Array.isArray(item['@type']) ? item['@type'] : [item['@type']];
                if (types.some((t) => LEGAL_JSONLD_TYPES.has(t))) {
                    return item;
                }
            }
        } catch { /* ignore malformed JSON-LD */ }
    }
    return null;
}

// ── Firm name heuristic (when schema.org attributes are absent) ────────────
const FIRM_KEYWORDS = /\b(Law|Attorney|Legal|LLC|LLP|P\.?A\.?|Office|Firm|Group)\b/i;

function extractFirmName($) {
    // Try schema.org itemprop first
    const itemprop = $('[itemprop="worksFor"]').text().trim()
        || $('[itemprop="memberOf"]').text().trim()
        || $('[itemprop="affiliation"]').text().trim();
    if (itemprop) return itemprop;

    // Heuristic: scan headings for legal business keywords
    const headings = $('h1, h2, h3, h4').toArray();
    for (const el of headings) {
        const text = $(el).text().trim();
        if (FIRM_KEYWORDS.test(text) && text.length < 120) {
            return text;
        }
    }
    return '';
}

// ── Main actor ─────────────────────────────────────────────────────────────
await Actor.init();

const input = await Actor.getInput();
const {
    startUrl,
    maxLawyers = 20,
    maxListingPages = 10,
    enrichProfiles = true,
    proxyConfiguration: proxyInput,
} = input ?? {};

if (!startUrl) {
    throw new Error('startUrl is required');
}

const proxyConfiguration = await Actor.createProxyConfiguration(proxyInput || {
    groups: ['RESIDENTIAL'],
    countryCode: 'US',
});

// ── State ──────────────────────────────────────────────────────────────────
const seenProfileUrls = new Set();
const lawyersToEnrich = [];
const stats = {
    totalLawyersScraped: 0,
    pagesProcessed: 0,
    profileEnrichments: 0,
    blockedRequests: 0,
    totalRequests: 0,
};
let debugPageCount = 0;

// ── Phase 1: Listing crawler ───────────────────────────────────────────────
const crawler = new CheerioCrawler({
    proxyConfiguration,
    maxRequestRetries: 8,
    maxRequestsPerCrawl: 500,
    sessionPoolOptions: {
        maxPoolSize: 10,
        sessionOptions: {
            maxUsageCount: 50,
        },
    },
    preNavigationHooks: [
        (crawlingContext) => {
            const { session } = crawlingContext;
            if (session?.userData?.lastUrl) {
                crawlingContext.request.headers = {
                    ...crawlingContext.request.headers,
                    Referer: session.userData.lastUrl,
                };
            }
        },
    ],
    async requestHandler({ request, $, session }) {
        stats.totalRequests++;
        const html = $.html();

        // Cloudflare check
        if (isCloudflareBlocked(html)) {
            stats.blockedRequests++;
            if (session) session.markBad();
            log.warning(`Cloudflare block on ${request.url}`);
            throw new Error('Cloudflare blocked — will retry with new session');
        }

        if (session) {
            session.userData = session.userData || {};
            session.userData.lastUrl = request.url;
        }

        // Extract attorney cards from listing page
        const cards = $('div.jld-card').toArray();

        if (cards.length === 0) {
            debugPageCount++;
            const kvStore = await Actor.openKeyValueStore();
            await kvStore.setValue(`DEBUG_NO_RESULTS_${debugPageCount}`, html, { contentType: 'text/html' });
            log.warning(`No attorney cards found on ${request.url} — saved debug HTML`);
            return;
        }

        stats.pagesProcessed++;
        const pageLawyers = [];

        for (const card of cards) {
            if (maxLawyers > 0 && stats.totalLawyersScraped >= maxLawyers) break;

            const $card = $(card);

            // Profile URL and dedup
            const profileLink = $card.find('a[href*="lawyers.justia.com/lawyer/"]').first();
            const profileUrl = profileLink.attr('href') || '';
            if (!profileUrl || seenProfileUrls.has(profileUrl)) continue;
            seenProfileUrls.add(profileUrl);

            // Name: strong.name a
            const name = $card.find('strong.name a').first().text().trim()
                || profileLink.text().trim();

            // Practice areas: span containing jicon-gavel, text is in the parent span
            let practiceAreas = [];
            const gavelParent = $card.find('.jicon-gavel').closest('.iconed-line-small');
            if (gavelParent.length) {
                // Get text content, excluding the icon span itself
                const paText = gavelParent.clone().children('.jicon').remove().end().text().trim();
                if (paText) {
                    practiceAreas = paText.split(',').map((s) => s.trim()).filter(Boolean);
                }
            }
            // Premium cards: practice area in div.outline
            if (practiceAreas.length === 0) {
                const outlineText = $card.find('div.outline').first().text().trim();
                if (outlineText) practiceAreas = [outlineText];
            }

            // Location: div.address.-hide-desktop, or extract from div.rating span
            let location = '';
            const addressEl = $card.find('div.address').first();
            if (addressEl.length) {
                // Parse "Street\nCity, ST ZIP" — take last line for city/state
                const lines = addressEl.text().trim().split('\n').map((l) => l.trim()).filter(Boolean);
                location = lines.length > 0 ? lines[lines.length - 1] : '';
            }
            if (!location) {
                // Fallback: extract "City, ST" from rating div text like "Overland Park, KS Personal Injury..."
                const ratingDiv = $card.find('div.rating > span').first().text().trim();
                const locMatch = ratingDiv.match(/^([A-Za-z\s.]+,\s*[A-Z]{2})\b/);
                if (locMatch) location = locMatch[1].trim();
            }

            // Phone: strong.phone a or a[href^="tel:"]
            const phoneEl = $card.find('strong.phone a').first();
            const phone = phoneEl.text().trim()
                || (phoneEl.attr('href') || '').replace(/^tel:\+?1?-?/, '');

            // Rating: star image filename encodes rating (stars-5-0.svg = 5.0)
            let rating = null;
            const starImg = $card.find('div.rating img[src*="stars-"]').first();
            if (starImg.length) {
                const starMatch = (starImg.attr('src') || '').match(/stars-(\d+)-(\d+)\.svg/);
                if (starMatch) rating = parseFloat(`${starMatch[1]}.${starMatch[2]}`);
            }
            // Also check for "10/10" text rating
            if (rating === null) {
                const ratingStrong = $card.find('div.rating strong').first().text().trim();
                const numMatch = ratingStrong.match(/(\d+(?:\.\d+)?)\s*\/\s*10/);
                if (numMatch) rating = parseFloat(numMatch[1]);
            }

            // Review count: "N Peer Review(s)" in rating div
            let reviewCount = 0;
            const ratingArea = $card.find('div.rating').first().text();
            const reviewMatch = ratingArea.match(/\((\d+)\s+Peer\s+Reviews?\)/i);
            if (reviewMatch) reviewCount = parseInt(reviewMatch[1], 10);

            // Years of experience from rating div text
            let yearsExperience = null;
            const yearsMatch = ratingArea.match(/(\d+)\s+years?\s+of\s+experience/i);
            if (yearsMatch) yearsExperience = parseInt(yearsMatch[1], 10);

            // Website: aria-label ending in "Website"
            const websiteEl = $card.find('a[aria-label$="Website"].rio-button').first();
            let website = websiteEl.attr('href') || '';
            // Strip Justia UTM tracking params
            if (website) {
                try { website = website.split('?utm_source=justia')[0]; } catch {}
            }
            if (isBlockedWebsite(website)) website = '';

            // Claimed profile badge
            const justiaClaimedProfile = !!$card.find('.rclaimed, .-j_claimed').length
                || $card.hasClass('-j_claimed');

            // Law school from listing card
            const lawSchool = $card.find('.jicon-education').closest('.iconed-line-small')
                .clone().children('.jicon').remove().end().text().trim();

            // Card tier (premium/gold/organic/claimed)
            const isPremium = $card.hasClass('-premium');
            const isGold = $card.hasClass('-gold');
            const cardTier = isPremium ? 'premium' : isGold ? 'gold' : 'organic';

            // Justia profile ID from data attribute
            const justiaProfileId = $card.attr('data-vars-profile') || '';

            const lawyer = {
                name,
                firmName: '', // only available from profile page
                profileUrl,
                practiceAreas,
                location,
                phone,
                rating,
                reviewCount,
                website,
                email: '', // Justia never shows email
                biography: '',
                barAdmissions: [],
                education: [],
                lawSchool,
                languages: [],
                yearsExperience,
                associations: [],
                justiaClaimedProfile,
                justiaProfileId,
                cardTier,
                latitude: null,
                longitude: null,
                source: 'justia',
                scrapedAt: new Date().toISOString(),
            };

            pageLawyers.push(lawyer);
            stats.totalLawyersScraped++;
        }

        // Push listing-only data to dataset (will be updated if enriched)
        if (!enrichProfiles) {
            await Actor.pushData(pageLawyers);
        } else {
            lawyersToEnrich.push(...pageLawyers);
        }

        log.info(`Page ${stats.pagesProcessed}: found ${pageLawyers.length} attorneys (total: ${stats.totalLawyersScraped})`);

        // Pagination
        if (stats.pagesProcessed < maxListingPages &&
            (maxLawyers === 0 || stats.totalLawyersScraped < maxLawyers)) {
            const nextLink = $('span.next a, .pagination .next a, a[rel="next"]').first().attr('href');
            if (nextLink) {
                const nextUrl = new URL(nextLink, request.url).href;
                await crawler.addRequests([{ url: nextUrl }]);
            }
        }
    },
    async failedRequestHandler({ request }) {
        stats.blockedRequests++;
        log.error(`Request permanently failed: ${request.url}`);
    },
});

log.info(`Starting Justia scraper: ${startUrl}`);
log.info(`Settings: maxLawyers=${maxLawyers}, maxListingPages=${maxListingPages}, enrichProfiles=${enrichProfiles}`);

await crawler.run([{ url: startUrl }]);

// ── Phase 2: Profile enrichment (out-of-band via gotScraping) ──────────────
if (enrichProfiles && lawyersToEnrich.length > 0) {
    const blockRate = stats.totalRequests > 0
        ? stats.blockedRequests / stats.totalRequests
        : 0;

    if (blockRate > 0.2) {
        log.warning(`Block rate ${(blockRate * 100).toFixed(1)}% exceeds 20% threshold — skipping enrichment, pushing listing-only data`);
        await Actor.pushData(lawyersToEnrich);
    } else {
        log.info(`Enriching ${lawyersToEnrich.length} profiles (block rate: ${(blockRate * 100).toFixed(1)}%)...`);

        const CONCURRENCY = 5;
        const enrichedLawyers = [];

        for (let i = 0; i < lawyersToEnrich.length; i += CONCURRENCY) {
            const batch = lawyersToEnrich.slice(i, i + CONCURRENCY);

            const results = await Promise.allSettled(
                batch.map(async (lawyer) => {
                    const proxyUrl = await proxyConfiguration.newUrl();
                    try {
                        const response = await gotScraping({
                            url: lawyer.profileUrl,
                            proxyUrl,
                            responseType: 'text',
                            timeout: { request: 30000 },
                        });

                        stats.totalRequests++;

                        if ([403, 429, 503].includes(response.statusCode)) {
                            stats.blockedRequests++;
                            log.warning(`Blocked on profile: ${lawyer.profileUrl} (${response.statusCode})`);
                            return lawyer; // return un-enriched
                        }

                        const html = response.body;
                        if (isCloudflareBlocked(html)) {
                            stats.blockedRequests++;
                            log.warning(`Cloudflare block on profile: ${lawyer.profileUrl}`);
                            return lawyer;
                        }

                        const $ = cheerio.load(html);
                        stats.profileEnrichments++;

                        // JSON-LD first
                        const jsonLd = extractJsonLd($);
                        if (jsonLd) {
                            lawyer.firmName = lawyer.firmName
                                || jsonLd.worksFor?.name
                                || jsonLd.memberOf?.name
                                || extractFirmName($);

                            if (jsonLd.geo) {
                                lawyer.latitude = jsonLd.geo.latitude || null;
                                lawyer.longitude = jsonLd.geo.longitude || null;
                            }

                            if (jsonLd.description) {
                                lawyer.biography = jsonLd.description.substring(0, 2000);
                            }
                        } else {
                            lawyer.firmName = lawyer.firmName || extractFirmName($);
                        }

                        // HTML fallback enrichment
                        if (!lawyer.biography) {
                            const bioEl = $('[itemprop="description"], .attorney-bio, .profile-bio').first();
                            lawyer.biography = bioEl.text().trim().substring(0, 2000);
                        }

                        // Bar admissions
                        const barEls = $('.bar-admission, [itemprop="hasCredential"]').toArray();
                        for (const el of barEls) {
                            const text = $(el).text().trim();
                            if (text) lawyer.barAdmissions.push(text);
                        }

                        // Education
                        const eduEls = $('.education-entry, [itemprop="alumniOf"]').toArray();
                        for (const el of eduEls) {
                            const text = $(el).text().trim();
                            if (text) lawyer.education.push(text);
                        }

                        // Languages
                        const langEls = $('.language, [itemprop="knowsLanguage"]').toArray();
                        for (const el of langEls) {
                            const text = $(el).text().trim();
                            if (text) lawyer.languages.push(text);
                        }

                        // Years experience
                        const yearsEl = $('[itemprop="yearsOfExperience"], .years-experience').first().text();
                        const yearsMatch = yearsEl.match(/(\d+)/);
                        if (yearsMatch) lawyer.yearsExperience = parseInt(yearsMatch[1], 10);

                        // Associations
                        const assocEls = $('.association, [itemprop="memberOf"]').toArray();
                        for (const el of assocEls) {
                            const text = $(el).text().trim();
                            if (text && text.length < 200) lawyer.associations.push(text);
                        }

                        // Claimed profile indicator
                        lawyer.justiaClaimedProfile = !!$('.claimed-badge, .verified-badge, [data-claimed="true"]').length;

                        // Website from profile page (if not found in listing)
                        if (!lawyer.website) {
                            const profileWebsite = $('a[data-button-tag="website"], a.website-link').first().attr('href') || '';
                            if (!isBlockedWebsite(profileWebsite)) {
                                lawyer.website = profileWebsite;
                            }
                        }

                        return lawyer;
                    } catch (err) {
                        log.warning(`Failed to enrich ${lawyer.profileUrl}: ${err.message}`);
                        return lawyer;
                    }
                }),
            );

            for (const result of results) {
                enrichedLawyers.push(result.status === 'fulfilled' ? result.value : batch[results.indexOf(result)]);
            }

            // Random delay between batches: 300-1200ms
            if (i + CONCURRENCY < lawyersToEnrich.length) {
                const delay = 300 + Math.random() * 900;
                await new Promise((r) => setTimeout(r, delay));
            }
        }

        await Actor.pushData(enrichedLawyers);
        log.info(`Enrichment complete: ${stats.profileEnrichments}/${lawyersToEnrich.length} profiles enriched`);
    }
}

// ── Save run statistics ────────────────────────────────────────────────────
const blockRate = stats.totalRequests > 0
    ? (stats.blockedRequests / stats.totalRequests * 100).toFixed(1)
    : '0.0';

log.info(`Run complete — ${stats.totalLawyersScraped} lawyers, ${stats.pagesProcessed} pages, ${stats.profileEnrichments} enrichments, ${blockRate}% blocked`);

const kvStore = await Actor.openKeyValueStore();
await kvStore.setValue('RUN_STATISTICS', {
    ...stats,
    blockRate: `${blockRate}%`,
    startUrl,
    completedAt: new Date().toISOString(),
});

await Actor.exit();
