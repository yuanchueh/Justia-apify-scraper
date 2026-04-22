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

// ── Main actor ─────────────────────────────────────────────────────────────
await Actor.init();

const input = await Actor.getInput();
const {
    startUrl,
    maxLawyers = 20,
    maxListingPages = 10,
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
log.info(`Settings: maxLawyers=${maxLawyers}, maxListingPages=${maxListingPages}`);

await crawler.run([{ url: startUrl }]);

// ── Save run statistics ────────────────────────────────────────────────────
const blockRate = stats.totalRequests > 0
    ? (stats.blockedRequests / stats.totalRequests * 100).toFixed(1)
    : '0.0';

log.info(`Run complete — ${stats.totalLawyersScraped} lawyers, ${stats.pagesProcessed} pages, ${blockRate}% blocked`);

const kvStore = await Actor.openKeyValueStore();
await kvStore.setValue('RUN_STATISTICS', {
    ...stats,
    blockRate: `${blockRate}%`,
    startUrl,
    completedAt: new Date().toISOString(),
});

await Actor.exit();
