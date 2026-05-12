import step2_learning_depth as depth


def _row(name, pnl, holdout, trades=120, gate_ok=True):
    return {
        "variant": name,
        "step2_pnl": pnl,
        "step2_trades": trades,
        "min_trade_gate": {"ok": gate_ok},
        "holdout_gate": {"ok": holdout > 0.0, "holdout_pnl": holdout},
        "routes": [
            {
                "action": "score",
                "match": {
                    "ticker": "CLSK",
                    "setup_type": "trend_pullback",
                    "session_phase": "open",
                    "side": "LONG",
                },
            }
        ],
    }


def test_learning_depth_report_builds_tranches_14_to_18():
    rows = [
        _row("profit_portfolio_parent", 500.0, 250.0),
        _row("profit_micro_probe", 100.0, 80.0, trades=70, gate_ok=False),
        _row("profit_mixed_loss", -200.0, -50.0),
    ]
    report = depth.learning_depth_report(
        rows,
        true_profit_report={"target_pnl": 300.0},
        lesson_half_life_report={
            "lessons": [
                {
                    "subject": "score|CLSK|trend_pullback|open|LONG",
                    "memory_type": "route_prior",
                    "half_life_state": "fresh",
                    "confidence": 0.8,
                    "refresh_action": "keep_in_memory",
                }
            ]
        },
        family_survival_report={
            "families": [
                {
                    "family_key": "profit_portfolio",
                    "survival_state": "surviving_family",
                    "holdout_survival_rate_pct": 80.0,
                }
            ]
        },
        belief_calibration_report={"learning_adjustments": {"belief_weight_multiplier": 1.0}},
        outcome_attribution_report={
            "route_attribution": [
                {
                    "route_key": "score|CLSK|trend_pullback|open|LONG",
                    "attribution_score": 40.0,
                }
            ]
        },
        uncertainty_heatmap_report={"cells": [{"cell_key": "route:score", "uncertainty_score": 65.0, "recommended_action": "test_first_next_batch"}]},
        uncertainty_model_report={"cells": [{"cell": "momentum", "uncertainty_score": 0.4, "recommended_action": "controlled_sibling_test"}]},
        deployment_risk_report={"deployment_state": "paper"},
        promotion_evidence_gap_report={
            "top_candidates": [
                {"variant": "profit_portfolio_parent", "gaps": [], "promotion_evidence_status": "review_ready"},
                {"variant": "profit_micro_probe", "gaps": [{"gap": "low_sample"}], "promotion_evidence_status": "needs_repair"},
            ]
        },
        statistical_validation_report={"ok": True},
        world_class_readiness_report={"ok": True},
        walk_forward_validation_bundle={"validations": [{"variant": "profit_portfolio_parent", "passed": True}]},
    )

    assert report["tranches"] == [14, 15, 16, 17, 18]
    assert report["enhanced_target_label_report"]["top_labels"][0]["target_label"] == "true_profit_target_candidate"
    assert report["lesson_survival_decay_report"]["retain_count"] >= 1
    assert report["portfolio_learning_report"]["core_count"] >= 1
    assert report["uncertainty_risk_pricing_report"]["risk_budget_state"] in {"tight", "moderate", "normal"}
    assert report["promotion_evidence_hardening_report"]["promotion_hardened"] is False
    assert "top_portfolio_candidate" in report["next_batch_depth_directive"]


def test_promotion_hardening_blocks_missing_walk_forward_validation():
    report = depth.promotion_evidence_hardening_report(
        promotion_evidence_gap_report={"top_candidates": [{"variant": "v1", "gaps": []}]},
        statistical_validation_report={"ok": True},
        world_class_readiness_report={"ok": True},
        walk_forward_validation_bundle={"validations": []},
    )

    assert report["promotion_hardened"] is False
    assert "walk_forward_validation_missing" in report["blockers"]
