/**
 * Stop-at-listing-total rule.
 *
 * Justia listing pages serve the TRUE matches first, then pad the remainder
 * with unrelated lawyers from the wider directory up to a ~1,000-item/25-page
 * server cap (verified: WY wrongful-death listingTotal=10 served 1,002 items;
 * every TN city listing ended at ~1,000). So once uniqueProfiles reaches the
 * parsed listingTotal, everything that follows — on this page and every page
 * after — is padding, not coverage.
 *
 * Totals the parse missed (null) never satisfy: pagination must run to its
 * natural end. Totals above the server cap can never be satisfied either —
 * the run ends at the cap and downstream coverage stays honestly partial.
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
