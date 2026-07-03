import test from 'node:test';
import assert from 'node:assert/strict';

import { listingTotalSatisfied, servedBroaderListing } from '../src/stop_rules.js';

// As of 2026-07 Justia's dedicated PA listings publish no total (numberless
// meta) and hold only true matches, so this rule is dormant there and re-arms
// if totals return. Hub pages cap at ~1,000 items (WY hub served 1,002; TN
// cities ended at ~1,000 each); the actor-forced listingTotal=0 from
// hub-fallback detection satisfies at zero — collect nothing.

test('satisfied exactly at the parsed total (WY wrongful-death: 10 of 10)', () => {
    assert.equal(
        listingTotalSatisfied({ stopAtListingTotal: true, listingTotal: 10, uniqueProfiles: 10 }),
        true,
    );
});

test('satisfied past the total (mid-page overshoot)', () => {
    assert.equal(
        listingTotalSatisfied({ stopAtListingTotal: true, listingTotal: 10, uniqueProfiles: 12 }),
        true,
    );
});

test('not satisfied below the total', () => {
    assert.equal(
        listingTotalSatisfied({ stopAtListingTotal: true, listingTotal: 10, uniqueProfiles: 9 }),
        false,
    );
});

test('never satisfied when no total parsed — pagination must run to its natural end', () => {
    assert.equal(
        listingTotalSatisfied({ stopAtListingTotal: true, listingTotal: null, uniqueProfiles: 500 }),
        false,
    );
});

test('stopAtListingTotal=false disables the rule even far past the total', () => {
    assert.equal(
        listingTotalSatisfied({ stopAtListingTotal: false, listingTotal: 10, uniqueProfiles: 1002 }),
        false,
    );
});

test('zero total: a padding-only listing is satisfied before collecting anything', () => {
    assert.equal(
        listingTotalSatisfied({ stopAtListingTotal: true, listingTotal: 0, uniqueProfiles: 0 }),
        true,
    );
});

test('garbage totals never satisfy (NaN, negative, non-integer)', () => {
    for (const bad of [NaN, -5, 10.5, '10', undefined]) {
        assert.equal(
            listingTotalSatisfied({ stopAtListingTotal: true, listingTotal: bad, uniqueProfiles: 999 }),
            false,
            `listingTotal=${String(bad)} must not satisfy`,
        );
    }
});

// Sparse-combo hub fallback: when a practice-area×place combo has too few
// lawyers, Justia serves the broader state/city hub instead of a dedicated
// listing — the canonical URL drops the practice-area segment (verified live
// 2026-07-02: requesting /lawyers/wrongful-death/wyoming served canonical
// /lawyers/wyoming with the whole ~1,000-lawyer state directory). A broader
// serve means the requested listing does not exist: verified zero.

test('state hub fallback detected (WY wrongful-death, verified live)', () => {
    assert.equal(servedBroaderListing({
        requestedUrl: 'https://www.justia.com/lawyers/wrongful-death/wyoming',
        canonicalUrl: 'https://www.justia.com/lawyers/wyoming',
    }), true);
});

test('city hub fallback detected (PA segment dropped from city listing)', () => {
    assert.equal(servedBroaderListing({
        requestedUrl: 'https://www.justia.com/lawyers/wrongful-death/tennessee/nashville',
        canonicalUrl: 'https://www.justia.com/lawyers/tennessee/nashville',
    }), true);
});

test('self-canonical dedicated listing is not a fallback', () => {
    assert.equal(servedBroaderListing({
        requestedUrl: 'https://www.justia.com/lawyers/personal-injury/kansas',
        canonicalUrl: 'https://www.justia.com/lawyers/personal-injury/kansas',
    }), false);
});

test('trailing slash, case, and host differences are not fallbacks', () => {
    assert.equal(servedBroaderListing({
        requestedUrl: 'https://www.justia.com/lawyers/Personal-Injury/kansas/',
        canonicalUrl: 'https://justia.com/lawyers/personal-injury/kansas',
    }), false);
});

test('missing or malformed canonical gives no signal', () => {
    for (const canonicalUrl of ['', null, undefined, 'not a url']) {
        assert.equal(servedBroaderListing({
            requestedUrl: 'https://www.justia.com/lawyers/wrongful-death/wyoming',
            canonicalUrl,
        }), false, `canonical=${String(canonicalUrl)} must not signal`);
    }
});

test('same-depth canonical (slug rename) is not a broader listing', () => {
    // A renamed slug keeps the segment count — never mistake it for a hub
    // serve and silently zero a cell that actually has a dataset.
    assert.equal(servedBroaderListing({
        requestedUrl: 'https://www.justia.com/lawyers/products-liability/kansas',
        canonicalUrl: 'https://www.justia.com/lawyers/product-liability/kansas',
    }), false);
});

test('query strings are ignored when comparing listing paths', () => {
    assert.equal(servedBroaderListing({
        requestedUrl: 'https://www.justia.com/lawyers/wrongful-death/wyoming?page=1',
        canonicalUrl: 'https://www.justia.com/lawyers/wyoming?ref=x',
    }), true);
});
