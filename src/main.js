/**
 * Justia Lawyer Directory Scraper — Apify Actor
 *
 * Scrapes attorney listing pages from Justia.com (div.jld-card).
 * Extracts: name, phone, website, practiceAreas, location, lawSchool,
 * yearsExperience, cardTier, justiaClaimedProfile, justiaProfileId.
 *
 * Profile enrichment (firmName, bio, barAdmissions) is handled downstream
 * in the Python pipeline via firm website scraping.
 *
 * At run end (even on failure/abort) writes the RUN_STATS key-value record
 * — the complete-coverage contract the sales_engine pipeline reads:
 * {listingTotal, reachedListingEnd, blockedRatio, pagesCrawled, uniqueProfiles}.
 *
 * Requires US residential proxies — Justia uses Cloudflare IP-tier blocking.
 */

import { Actor, log } from 'apify';
import { CheerioCrawler } from 'crawlee';

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

// ── Listing total detection ────────────────────────────────────────────────
// Directory-wide total for RUN_STATS.listingTotal, parsed once from the first
// listing page. Justia states it in the meta description ("Compare 380
// personal injury attorneys in Kansas on Justia."); older layouts used visible
// "showing X - Y of N" / "N lawyers" text. Returns an int or null — never
// guesses. A null total keeps the coverage cell unverified downstream.
function parseListingTotal($) {
    const metaText = [
        $('meta[name="description"]').attr('content') || '',
        $('meta[property="og:description"]').attr('content') || '',
    ].join(' ');
    const bodyText = $('body').text().replace(/\s+/g, ' ');
    const candidates = [
        [metaText, /compare\s+([\d,]+)\+?\s+[^.]{0,80}?(?:attorneys?|lawyers?)\b/i],
        [bodyText, /showing\s+[\d,]+\s*(?:[-–—]|to)\s*[\d,]+\s+of\s+([\d,]+)/i],
        [bodyText, /\bof\s+([\d,]+)\+?\s+(?:attorneys?|lawyers?|results)\b/i],
        [bodyText, /([\d,]+)\+?\s+(?:attorneys?|lawyers?)\s+(?:found|match(?:ed)?|serving|in)\b/i],
    ];
    for (const [text, re] of candidates) {
        const match = text.match(re);
        if (match) {
            const n = parseInt(match[1].replace(/,/g, ''), 10);
            if (!Number.isNaN(n)) return n;
        }
    }
    return null;
}

// ── Main actor ─────────────────────────────────────────────────────────────
await Actor.init();

const input = await Actor.getInput();
const {
    startUrl,
    maxLawyers = 0,
    maxListingPages = 0,
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
const stats = {
    totalLawyersScraped: 0,
    pagesProcessed: 0,
    blockedRequests: 0,
    totalRequests: 0,
};
let debugPageCount = 0;
let listingTotal = null;         // RUN_STATS.listingTotal — parsed once from the first listing page
let listingTotalChecked = false;
let reachedListingEnd = false;   // RUN_STATS.reachedListingEnd — true ONLY on natural pagination end

// ── Listing crawler ────────────────────────────────────────────────────────
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

        // Parse the directory total once, from the first successfully fetched page
        if (!listingTotalChecked) {
            listingTotalChecked = true;
            listingTotal = parseListingTotal($);
            log.info(`Listing total: ${listingTotal === null ? 'not found' : listingTotal}`);
        }

        // Extract attorney cards from listing page
        stats.pagesProcessed++;
        const cards = $('div.jld-card').toArray();

        if (cards.length === 0) {
            debugPageCount++;
            const kvStore = await Actor.openKeyValueStore();
            await kvStore.setValue(`DEBUG_NO_RESULTS_${debugPageCount}`, html, { contentType: 'text/html' });
            log.warning(`No attorney cards found on ${request.url} — saved debug HTML`);
            // A page yielding zero profile URLs is a natural end of the listing
            // (verified-zero cells depend on this; a mis-render cannot falsely
            // verify because the listingTotal*0.95 gate decides downstream).
            reachedListingEnd = true;
            return;
        }

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

            // Location: div.address, or extract from div.rating span
            let location = '';
            const addressEl = $card.find('div.address').first();
            if (addressEl.length) {
                const raw = addressEl.text().replace(/[\t\n\r]+/g, ' ').replace(/\s{2,}/g, ' ').trim();
                const cityStateZip = raw.match(/\b([A-Za-z][A-Za-z\s.]*[A-Za-z]),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)/);
                if (cityStateZip) {
                    let city = cityStateZip[1];
                    const streetSuffixCut = city.match(/(?:St\.?|Street|Ave\.?|Avenue|Blvd\.?|Rd\.?|Road|Dr\.?|Drive|Ct\.?|Ln\.?|Way|Suite|Ste\.?)\s+(.+)$/i);
                    if (streetSuffixCut) city = streetSuffixCut[1];
                    location = `${city.trim()}, ${cityStateZip[2]} ${cityStateZip[3]}`;
                } else {
                    const cityState = raw.match(/\b([A-Za-z][A-Za-z\s.]*[A-Za-z]),\s*([A-Z]{2})\b/);
                    if (cityState) {
                        let city = cityState[1];
                        const streetSuffixCut = city.match(/(?:St\.?|Street|Ave\.?|Avenue|Blvd\.?|Rd\.?|Road|Dr\.?|Drive|Ct\.?|Ln\.?|Way|Suite|Ste\.?)\s+(.+)$/i);
                        if (streetSuffixCut) city = streetSuffixCut[1];
                        location = `${city.trim()}, ${cityState[2]}`;
                    } else {
                        location = raw;
                    }
                }
            }
            if (!location) {
                const ratingDiv = $card.find('div.rating > span').first().text().trim();
                const locMatch = ratingDiv.match(/^([A-Za-z\s.]+,\s*[A-Z]{2})\b/);
                if (locMatch) location = locMatch[1].trim();
            }

            // Phone: strong.phone a
            const phoneEl = $card.find('strong.phone a').first();
            const phone = phoneEl.text().trim()
                || (phoneEl.attr('href') || '').replace(/^tel:\+?1?-?/, '');

            // Years of experience from rating div text
            let yearsExperience = null;
            const ratingArea = $card.find('div.rating').first().text();
            const yearsMatch = ratingArea.match(/(\d+)\s+years?\s+of\s+experience/i);
            if (yearsMatch) yearsExperience = parseInt(yearsMatch[1], 10);

            // Website: aria-label ending in "Website"
            const websiteEl = $card.find('a[aria-label$="Website"].rio-button').first();
            let website = websiteEl.attr('href') || '';
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

            // Card tier (premium/gold/organic)
            const isPremium = $card.hasClass('-premium');
            const isGold = $card.hasClass('-gold');
            const cardTier = isPremium ? 'premium' : isGold ? 'gold' : 'organic';

            // Justia profile ID from data attribute
            const justiaProfileId = $card.attr('data-vars-profile') || '';

            pageLawyers.push({
                name,
                profileUrl,
                practiceAreas,
                location,
                phone,
                website,
                lawSchool,
                yearsExperience,
                justiaClaimedProfile,
                justiaProfileId,
                cardTier,
                source: 'justia',
                scrapedAt: new Date().toISOString(),
            });
            stats.totalLawyersScraped++;
        }

        await Actor.pushData(pageLawyers);

        log.info(`Page ${stats.pagesProcessed}: found ${pageLawyers.length} attorneys (total: ${stats.totalLawyersScraped})`);

        // Pagination — 0 means unlimited; reachedListingEnd flips true ONLY on
        // a natural end (no new profiles, or no next link), never on a cap.
        const lawyersCapReached = maxLawyers > 0 && stats.totalLawyersScraped >= maxLawyers;
        const pagesCapReached = maxListingPages > 0 && stats.pagesProcessed >= maxListingPages;

        if (lawyersCapReached || pagesCapReached) {
            log.info(`Cap reached (maxLawyers=${maxLawyers}, maxListingPages=${maxListingPages}) — stopping pagination`);
        } else if (pageLawyers.length === 0) {
            // Cards present but every profile URL already seen (Justia repeats
            // premium/gold attorneys across pages) — the listing has run out.
            reachedListingEnd = true;
            log.info(`No new profiles on ${request.url} — natural end of listing`);
        } else {
            const nextLink = $('span.next a, .pagination .next a, a[rel="next"]').first().attr('href');
            if (nextLink) {
                const nextUrl = new URL(nextLink, request.url).href;
                await crawler.addRequests([{ url: nextUrl }]);
            } else {
                reachedListingEnd = true; // last page — no next link
            }
        }
    },
    async failedRequestHandler({ request }) {
        // A permanently failed request never reached requestHandler, so count
        // it as both seen and blocked — hard 403/challenge failures must move
        // blockedRatio instead of leaving totalRequests at 0.
        stats.totalRequests++;
        stats.blockedRequests++;
        log.error(`Request permanently failed: ${request.url}`);
    },
});

// ── Run statistics ─────────────────────────────────────────────────────────
function blockedRatio() {
    return stats.totalRequests > 0 ? stats.blockedRequests / stats.totalRequests : 0;
}

async function saveRunStats() {
    const kvStore = await Actor.openKeyValueStore();
    // Complete-coverage contract (sales_engine spec §4.2.3) — the pipeline
    // reads these five exact field names; do not rename.
    await kvStore.setValue('RUN_STATS', {
        listingTotal,
        reachedListingEnd,
        blockedRatio: blockedRatio(),
        pagesCrawled: stats.pagesProcessed,
        uniqueProfiles: seenProfileUrls.size,
    });
    // Legacy human-oriented record, kept for continuity with earlier runs.
    await kvStore.setValue('RUN_STATISTICS', {
        ...stats,
        blockRate: `${(blockedRatio() * 100).toFixed(1)}%`,
        startUrl,
        completedAt: new Date().toISOString(),
    });
}

// Graceful platform aborts emit 'aborting' with a short grace period —
// report whatever was collected (reachedListingEnd stays false).
Actor.on('aborting', () => {
    saveRunStats().catch((err) => log.exception(err, 'Failed to save RUN_STATS on abort'));
});

log.info(`Starting Justia scraper: ${startUrl}`);
log.info(`Settings: maxLawyers=${maxLawyers === 0 ? 'unlimited' : maxLawyers}, maxListingPages=${maxListingPages === 0 ? 'unlimited' : maxListingPages}`);

try {
    await crawler.run([{ url: startUrl }]);
} finally {
    // finally-style: crashed/bailed runs still report RUN_STATS.
    await saveRunStats().catch((err) => log.exception(err, 'Failed to save RUN_STATS'));
}

log.info(`Run complete — ${stats.totalLawyersScraped} lawyers, ${stats.pagesProcessed} pages, `
    + `${(blockedRatio() * 100).toFixed(1)}% blocked, reachedListingEnd=${reachedListingEnd}`);

await Actor.exit();
