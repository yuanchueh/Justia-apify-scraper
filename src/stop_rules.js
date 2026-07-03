/**
 * Stop-at-listing-total rule.
 *
 * If a listing publishes a parseable total N, everything past the first N
 * unique profiles is padding — stop there. As of 2026-07 Justia's dedicated
 * PA listings publish NO total (numberless meta) and hold only true matches,
 * so this rule is dormant on them and pagination ends naturally; it re-arms
 * automatically if Justia republishes "Compare N" totals. The actor-forced
 * listingTotal=0 from hub-fallback detection (servedBroaderListing) satisfies
 * at zero: collect nothing, the whole page is hub padding.
 *
 * Totals the parse missed (null) never satisfy: pagination must run to its
 * natural end. Totals above the ~1,000-item server cap can never be satisfied
 * either — the run ends at the cap and downstream coverage stays honestly
 * partial.
 */
export function listingTotalSatisfied({ stopAtListingTotal, listingTotal, uniqueProfiles }) {
    if (!stopAtListingTotal) return false;
    if (!Number.isInteger(listingTotal) || listingTotal < 0) return false;
    return uniqueProfiles >= listingTotal;
}

/**
 * Sparse-combo hub fallback detection.
 *
 * When a practice-area×place combo has too few lawyers for a dedicated
 * listing, Justia serves the broader state/city hub page instead — the
 * canonical URL drops the practice-area segment (verified live 2026-07-02:
 * requesting /lawyers/wrongful-death/wyoming served canonical
 * /lawyers/wyoming with the whole ~1,000-lawyer state directory and a
 * numberless hub meta). A broader serve means the requested listing does not
 * exist: zero true matches, everything on the page is padding.
 *
 * Broader = the canonical path's segments are a PROPER ordered subset of the
 * requested path's segments. A same-depth canonical (e.g. a slug rename)
 * never triggers — mistaking a real listing for a hub would silently zero a
 * cell that has data. No/malformed canonical gives no signal.
 */
export function servedBroaderListing({ requestedUrl, canonicalUrl }) {
    const segments = (u) => {
        try {
            return new URL(u).pathname.toLowerCase().split('/').filter(Boolean);
        } catch {
            return null;
        }
    };
    const requested = segments(requestedUrl);
    const canonical = canonicalUrl ? segments(canonicalUrl) : null;
    if (!requested || !canonical) return false;
    if (canonical.length >= requested.length) return false;
    // ordered subsequence: every canonical segment appears in the requested
    // path, in order — i.e. the canonical is the requested URL minus segments
    let i = 0;
    for (const seg of requested) {
        if (i < canonical.length && canonical[i] === seg) i++;
    }
    return i === canonical.length;
}
