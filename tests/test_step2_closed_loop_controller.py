import step2_closed_loop_controller as controller


def test_route_controller_scales_when_prior_validation_and_rewards_agree():
    report = controller.controller_report(
        route_prior_model={
            "priors": [
                {
                    "route_key": "CLSK|trend_pullback|late",
                    "prior_score": 180.0,
                    "validation_pass_rate": 1.0,
                    "reject_rate": 0.0,
                }
            ]
        },
        experiment_registry={
            "experiments": [
                {"route_key": "CLSK|trend_pullback|late", "verdict": "validated"},
            ]
        },
        experiment_debt={"queue": []},
        closed_loop_memory={
            "command_outcomes": [
                {
                    "route_key": "CLSK|trend_pullback|late",
                    "action": "scale",
                    "reward_score": 20.0,
                    "worked": True,
                }
            ],
            "action_league": [{"action": "scale", "rating": 1560.0, "sample_count": 4, "success_rate": 0.75}],
        },
        validation_rows=[{"route_key": "CLSK|trend_pullback|late", "passed": True, "metric_value": 12.0}],
        feedback_rows=[],
        batch_size=100,
    )

    assert report["route_controller"]["scale_routes"] == ["CLSK|trend_pullback|late"]
    assert report["next_run_directives"]["allocation"][0]["variant_budget"] == 100
    assert report["action_controller"]["preferred_actions"] == ["scale"]


def test_route_controller_retires_rejected_or_failed_routes():
    report = controller.controller_report(
        route_prior_model={"priors": [{"route_key": "BAD|route|x", "prior_score": 50.0}]},
        experiment_registry={
            "experiments": [
                {"route_key": "BAD|route|x", "verdict": "downweight_or_change_treatment"},
                {"route_key": "BAD|route|x", "verdict": "failed"},
            ]
        },
        experiment_debt={"queue": []},
        closed_loop_memory={"command_outcomes": [{"route_key": "BAD|route|x", "reward_score": -8.0, "worked": False}]},
        validation_rows=[
            {"route_key": "BAD|route|x", "passed": False},
            {"route_key": "BAD|route|x", "passed": False},
        ],
        feedback_rows=[{"route_key": "BAD|route|x", "status": "rejected"}],
        batch_size=100,
    )

    decision = report["route_controller"]["decisions"][0]
    assert decision["route_key"] == "BAD|route|x"
    assert decision["decision"] == "retire_or_quarantine"
    assert report["next_run_directives"]["retire_routes"] == ["BAD|route|x"]
