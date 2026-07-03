import test from 'node:test';
import assert from 'node:assert/strict';

import { listingTotalSatisfied } from '../src/stop_rules.js';

// Justia listings serve true matches FIRST, then pad with unrelated lawyers up
// to a ~1,000-item/25-page server cap (verified: WY wrongful-death
// listingTotal=10 served 1,002; every TN city ended at ~1,000). Once
// uniqueProfiles reaches the parsed listingTotal, everything after is padding.

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
