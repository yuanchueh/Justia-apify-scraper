/**
 * Justia Lawyer Directory Scraper — Apify Actor
 *
 * Scrapes attorney listing pages from Justia.com (div.jld-card) with a real
 * headless Chromium browser (PlaywrightCrawler).
 *
 * Why a browser: Justia sits behind Cloudflare bot management and the block
 * is NOT clearable at the HTTP layer. got-scraping (header impersonation)
 * hard-403'd every retry; impit (real Chrome TLS/JA3 + HTTP/2 replay) ALSO
 * hard-403'd (run JtHNnWoRP0c7xZRcS, blockedRatio 1.0). Passing requires
 * executing the Cloudflare JS challenge, which only a real browser can do.
 * Chromium + crawlee fingerprint injection + US residential proxy clears the
 * managed challenge; the cf_clearance cookie then rides the (single, sticky)
 * session through pagination without re-challenging.
 *
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
 * Requires US residential proxies — Cloudflare also scores at the IP tier.
 */

import { Actor, log } from 'apify';
import { PlaywrightCrawler, sleep } from 'crawlee';

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

// ── Cloudflare page classification ─────────────────────────────────────────
// 'cards'      — listing rendered (div.jld-card present): success.
// 'challenge'  — interstitial ("Just a moment…"): may auto-solve, keep waiting.
// 'hardblock'  — definite block page (1020/1015 "you have been blocked"):
//                never auto-solves, fail fast and rotate.
// 'plain'      — rendered non-challenge page without cards (empty listing or
//                past the last page).
// 'transition' — DOM unreadable mid-navigation; poll again.
async function classifyPage(page) {
    try {
        if (await page.$('div.jld-card')) return 'cards';
        const probe = await page.evaluate(() => ({
            title: document.title || '',
            text: document.body ? document.body.innerText.slice(0, 3000) : '',
        }));
        const t = `${probe.title}\n${probe.text}`.toLowerCase();
        if (
            t.includes('sorry, you have been blocked')
            || t.includes('attention required')
            || t.includes('access denied')
            || t.includes('error 1020')
            || t.includes('error 1015')
            || t.includes('error 1010')
        ) return 'hardblock';
        if (
            t.includes('just a moment')
            || t.includes('checking your browser')
            || t.includes('verify you are human')
            || t.includes('verifying you are human')
            || t.includes('needs to review the security of your connection')
            || t.includes('enable javascript and cookies to continue')
        ) return 'challenge';
        return 'plain';
    } catch {
        return 'transition';
    }
}

// Wait for the listing selector OR a definite outcome. Cloudflare's managed
// challenge auto-solves in a real browser after a few seconds, so a visible
// challenge is not yet a block — only a challenge that PERSISTS past the
// patience window (or a hard block page) counts as blocked.
const CHALLENGE_PATIENCE_MS = 55_000;
const POLL_MS = 1_500;
const QUIET_POLLS_FOR_EMPTY = 4; // ~6s of stable non-challenge DOM => genuinely cardless page

async function waitForCardsOrBlock(page) {
    const deadline = Date.now() + CHALLENGE_PATIENCE_MS;
    let sawChallenge = false;
    let quietPolls = 0;
    let state = await classifyPage(page);
    while (Date.now() < deadline) {
        if (state === 'cards' || state === 'hardblock') break;
        if (state === 'plain') {
            quietPolls++;
            if (quietPolls >= QUIET_POLLS_FOR_EMPTY) break;
        } else {
            quietPolls = 0;
            if (state === 'challenge') sawChallenge = true;
        }
        await sleep(POLL_MS);
        state = await classifyPage(page);
    }
    // Timed out mid-challenge/transition => blocked; stable 'plain' => empty page.
    const blocked = state === 'hardblock' || state === 'challenge' || state === 'transition';
    return { blocked, state, sawChallenge };
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
    totalRequests: 0,      // navigations that produced a document (+ final failures)
};
let debugPageCount = 0;
let listingTotal = null;         // RUN_STATS.listingTotal — parsed once from the first listing page
let listingTotalChecked = false;
let reachedListingEnd = false;   // RUN_STATS.reachedListingEnd — true ONLY on natural pagination end

// ── Listing crawler ────────────────────────────────────────────────────────
// Real Chromium, sequential pagination. One session at a time (maxPoolSize 1)
// so the cf_clearance cookie, the sticky residential IP, and the browser
// fingerprint stay consistent across pages — Cloudflare binds clearance to
// IP + fingerprint, so rotating sessions per page would re-challenge every
// navigation. On a block we retire BOTH the session (new IP) and the browser
// (new fingerprint) and let crawlee retry.
const crawler = new PlaywrightCrawler({
    proxyConfiguration,
    launchContext: {
        launchOptions: {
            args: [
                '--disable-blink-features=AutomationControlled',
                '--lang=en-US',
            ],
        },
    },
    browserPoolOptions: {
        useFingerprints: true, // default, made explicit — headless-signal patching
        fingerprintOptions: {
            fingerprintGeneratorOptions: {
                browsers: ['chrome'],
                operatingSystems: ['windows'],
                locales: ['en-US'],
            },
        },
    },
    maxConcurrency: 2,           // pagination discovers pages serially; stays ~1 in practice
    maxRequestRetries: 5,
    maxRequestsPerCrawl: 500,
    navigationTimeoutSecs: 90,   // residential proxies are slow; challenge adds redirects
    requestHandlerTimeoutSecs: 180,
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: 1,          // single sticky session: cookie/IP/fingerprint continuity
        sessionOptions: {
            maxUsageCount: 50,
        },
        // CRITICAL: the Cloudflare challenge itself arrives as a 403. The
        // session pool's default blockedStatusCodes ([401,403,429]) would
        // throw before the browser gets a chance to run and solve the
        // challenge — disable status-code auto-blocking; classifyPage()
        // decides what is actually blocked.
        blockedStatusCodes: [],
    },
    preNavigationHooks: [
        async ({ page, session }, gotoOptions) => {
            // The challenge interstitial holds the 'load' event hostage;
            // return on DOM ready and let waitForCardsOrBlock() do the rest.
            gotoOptions.waitUntil = 'domcontentloaded';
            if (session?.userData?.lastUrl) gotoOptions.referer = session.userData.lastUrl;
            // Residential bandwidth is the dominant cost — drop images/media/
            // fonts. Keep scripts/styles/XHR: the challenge needs them. NEVER
            // touch Cloudflare's own challenge assets (challenges.cloudflare.com,
            // /cdn-cgi/) — aborting them can stall the managed challenge and
            // waste the first attempt.
            await page.route('**/*', (route) => {
                const url = route.request().url();
                if (url.includes('challenges.cloudflare.com') || url.includes('/cdn-cgi/')) {
                    return route.continue();
                }
                const type = route.request().resourceType();
                if (type === 'image' || type === 'media' || type === 'font') return route.abort();
                return route.continue();
            });
        },
    ],
    async requestHandler({ request, page, response, session, parseWithCheerio, crawler: crawlerRef }) {
        stats.totalRequests++;

        const { blocked, state, sawChallenge } = await waitForCardsOrBlock(page);
        if (blocked) {
            stats.blockedRequests++;
            const status = response?.status?.() ?? 'n/a';
            log.warning(`Cloudflare ${state} persisted on ${request.url} (HTTP ${status}, sawChallenge=${sawChallenge})`);
            session?.retire();
            // New fingerprint next attempt, not just a new IP.
            await crawlerRef.browserPool.retireBrowserByPage(page);
            throw new Error(`Cloudflare blocked (${state}) — retrying with fresh session + browser`);
        }
        if (sawChallenge) {
            log.info(`Cloudflare challenge auto-solved on ${request.url}`);
        }

        if (session) {
            session.userData = session.userData || {};
            session.userData.lastUrl = request.url;
        }

        // Cheerio over the RENDERED DOM — parsing below is unchanged from the
        // CheerioCrawler versions of this actor.
        const $ = await parseWithCheerio();

        // Parse the directory total once, from the first successfully fetched page
        if (!listingTotalChecked) {
            listingTotalChecked = true;
            listingTotal = parseListingTotal($);
            if (listingTotal !== null) {
                log.info(`Listing total: ${listingTotal}`);
            } else {
                // Save the rendered page so the count markup can be inspected
                // when the parser misses — listingTotal=null keeps the
                // coverage cell unverified downstream. (Same pattern as the
                // Avvo actor's LISTING_DEBUG_HTML.)
                log.warning('listingTotal parse missed; saving LISTING_DEBUG_HTML.');
                try {
                    await Actor.setValue('LISTING_DEBUG_HTML', await page.content(), { contentType: 'text/html' });
                } catch (e) {
                    log.warning(`Could not save LISTING_DEBUG_HTML: ${e.message}`);
                }
            }
        }

        // Extract attorney cards from listing page
        stats.pagesProcessed++;
        const cards = $('div.jld-card').toArray();

        if (cards.length === 0) {
            debugPageCount++;
            try {
                await Actor.setValue(`DEBUG_NO_RESULTS_${debugPageCount}`, await page.content(), { contentType: 'text/html' });
            } catch (e) {
                log.warning(`Could not save DEBUG_NO_RESULTS_${debugPageCount}: ${e.message}`);
            }
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
        // A request that exhausted its retries counts as both seen and blocked
        // — persistent challenge/403 failures must move blockedRatio instead
        // of leaving totalRequests at 0.
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

log.info(`Starting Justia scraper (Playwright/Chromium): ${startUrl}`);
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
