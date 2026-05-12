import step2_belief_calibration as calibration


def test_belief_calibration_scores_route_beliefs_against_validation():
    belief_report = {
        "beliefs": [
            {
                "belief": "route_prior:CLSK|trend_pullback|late",
                "posterior_confidence": 0.8,
                "state": "promising",
                "evidence": {"observations": 4},
            },
            {
                "belief": "positive_gene:force_short",
                "posterior_confidence": 0.6,
            },
        ]
    }
    validation_rows = [
        {"route_key": "CLSK|trend_pullback|late", "passed": True},
        {"route_key": "CLSK|trend_pullback|late", "passed": True},
    ]

    report = calibration.report_from_beliefs(
        belief_report,
        run_id="run1",
        validation_rows=validation_rows,
    )

    assert report["belief_count"] == 2
    assert report["resolved_predictions"] == 1
    assert report["pending_predictions"] == 1
    assert report["brier_score"] == 0.04
    resolved = [row for row in report["predictions"] if row["status"] == "resolved"][0]
    assert resolved["actual"] == "validation_passed"
    assert resolved["resolution_source"] == "validation_results"
    assert resolved["confidence_bucket"] == "80_90"


def test_feedback_rejection_marks_confident_route_belief_overconfident():
    prediction = {
        "belief": "route_prior:RIOT|breakout|open",
        "belief_type": "route_prior",
        "subject": "RIOT|breakout|open",
        "predicted_probability": 0.9,
    }

    resolved = calibration.resolve_prediction_outcome(
        prediction,
        feedback_rows=[{"route_key": "RIOT|breakout|open", "status": "rejected"}],
    )

    assert resolved["status"] == "resolved"
    assert resolved["actual_numeric"] == 0.0
    assert resolved["brier_score"] == 0.81
    assert resolved["confidence_bias"] == "overconfident"
