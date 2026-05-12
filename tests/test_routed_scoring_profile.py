import unittest

import numpy as np

import decision_tape_compiled
import routed_scoring_profile as routed
import scoring_variant_lab as lab
import scoring_variant_lab_fast as fast


FEATURE_INDEX = {name: idx for idx, name in enumerate(fast.FEATURE_NAMES)}


def _compiled() -> dict:
    features = np.zeros((4, len(fast.FEATURE_NAMES)), dtype=np.float64)
    features[:, FEATURE_INDEX['ema']] = np.array([-1.0, 0.5, 0.0, 2.0])
    features[0, FEATURE_INDEX['open_phase']] = 1.0
    features[1, FEATURE_INDEX['open_phase']] = 1.0
    features[2, FEATURE_INDEX['midday_phase']] = 1.0
    features[3, FEATURE_INDEX['setup_btc_relative_strength']] = 1.0
    return {
        'rows': 4,
        'features': features,
        'ticker_code': np.array([0, 1, 0, 1], dtype=np.int16),
        'setup_code': np.array([0, 0, 1, 1], dtype=np.int16),
        'day_code': np.array([0, 0, 0, 0], dtype=np.int16),
        'original_side': np.array([
            decision_tape_compiled.SIDE_LONG,
            decision_tape_compiled.SIDE_SHORT,
            decision_tape_compiled.SIDE_LONG,
            decision_tape_compiled.SIDE_SHORT,
        ], dtype=np.int8),
        'ticker_map': {'CLSK': 0, 'MARA': 1},
        'setup_map': {'btc_relative_strength': 0, 'momentum_breakout': 1},
        'day_map': {'2026-05-08': 0},
    }


class RoutedScoringProfileTests(unittest.TestCase):
    def test_empty_routed_variant_matches_plain_scoring(self):
        compiled = _compiled()
        plain = lab.Variant('plain', {'ema': 1.0}, 0.0)
        routed_variant = routed.routed_variant('routed_plain', {'ema': 1.0}, 0.0, [])

        plain_sides = decision_tape_compiled.side_matrix(compiled, [plain])
        routed_sides = decision_tape_compiled.side_matrix(compiled, [routed_variant])

        np.testing.assert_array_equal(plain_sides, routed_sides)

    def test_route_match_overrides_only_matching_slice(self):
        compiled = _compiled()
        variant = routed.routed_variant(
            'route_clsk_open',
            {'ema': 1.0},
            routes=[
                routed.route(
                    'clsk_open_reverse_ema',
                    {'ticker': 'CLSK', 'session_phase': 'open'},
                    {'ema': -1.0},
                ),
            ],
        )

        sides = decision_tape_compiled.side_matrix(compiled, [variant])[0]

        self.assertEqual(decision_tape_compiled.SIDE_LONG, int(sides[0]))
        self.assertEqual(decision_tape_compiled.SIDE_LONG, int(sides[1]))
        self.assertEqual(decision_tape_compiled.SIDE_LONG, int(sides[2]))
        self.assertEqual(decision_tape_compiled.SIDE_LONG, int(sides[3]))

    def test_skip_route_returns_skip_side_for_matching_rows(self):
        compiled = _compiled()
        variant = routed.routed_variant(
            'skip_momentum_midday',
            {'ema': 1.0},
            routes=[
                routed.route(
                    'skip_momentum_midday',
                    {'setup_type': 'momentum_breakout', 'session_phase': 'midday'},
                    action='skip',
                ),
            ],
        )

        sides = decision_tape_compiled.side_matrix(compiled, [variant])[0]

        self.assertEqual(routed.SIDE_SKIP, int(sides[2]))
        self.assertNotEqual(routed.SIDE_SKIP, int(sides[0]))
        self.assertNotEqual(routed.SIDE_SKIP, int(sides[1]))
        self.assertNotEqual(routed.SIDE_SKIP, int(sides[3]))

    def test_route_mask_supports_feature_thresholds(self):
        compiled = _compiled()
        mask = routed.route_mask(
            compiled,
            routed.route(
                'mara_brs_feature',
                {
                    'ticker': 'MARA',
                    'setup_type': 'momentum_breakout',
                    'min_features': {'setup_btc_relative_strength': 1.0},
                },
            ),
        )

        self.assertEqual([False, False, False, True], mask.tolist())

    def test_decision_hash_distinguishes_skip_from_short(self):
        short_sides = np.array([[decision_tape_compiled.SIDE_SHORT]], dtype=np.int8)
        skip_sides = np.array([[routed.SIDE_SKIP]], dtype=np.int8)

        short_hash = decision_tape_compiled.decision_hashes_from_sides(short_sides)[0]
        skip_hash = decision_tape_compiled.decision_hashes_from_sides(skip_sides)[0]

        self.assertNotEqual(short_hash, skip_hash)


if __name__ == '__main__':
    unittest.main()
