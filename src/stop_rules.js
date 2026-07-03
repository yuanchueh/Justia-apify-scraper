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
