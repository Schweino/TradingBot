import step2_ops_hardening as ops


def test_ops_hardening_report_builds_tranches_19_to_24():
    report = ops.ops_hardening_report(
        closed_loop_controller={
            "next_run_directives": {
                "scale_routes": [{"route_key": "CLSK|trend_pullback|late"}],
                "probe_routes": [],
                "repair_routes": [],
                "retire_routes": [],
            }
        },
        deployment_risk_report={
            "deployment_queue": [{"variant": "winner"}],
            "global_kill_switches": ["disable_if_void_registry_flags_source_day"],
        },
        belief_calibration_report={"pending_predictions": 1},
        learning_control_plane={"next_batch_directive": {"top_attribution": {"route_key": "CLSK|trend_pullback|late"}}},
        learning_depth_report={
            "uncertainty_risk_pricing_report": {"risk_budget_state": "normal"},
            "next_batch_depth_directive": {"promotion_hardened": True, "risk_budget_state": "normal"},
        },
        learning_governance_report={"decision": "allow_learning_to_steer_next_batch"},
        promotion_evidence_hardening_report={"promotion_hardened": True},
        world_class_readiness_report={"ok": True},
        statistical_validation_report={"false_discovery_pressure": 0.1},
        data_quality_report={"learning_allowed": True, "quality_score": 95.0},
        artifact_paths={
            "belief_calibration_report": "belief.json",
            "learning_control_plane": "control.json",
            "learning_depth_report": "depth.json",
            "deployment_risk_report": "risk.json",
            "world_class_readiness_report": "ready.json",
        },
        artifact_trust_report={"trust_state": "trusted"},
        learning_ops_readiness_report={"ready_for_next_batch": True, "blockers": []},
        active_experiment_design_report={"experiments": [{"experiment_id": "exp1", "subject": "route"}]},
    )

    assert report["tranches"] == [19, 20, 21, 22, 23, 24]
    assert report["live_feedback_loop_report"]["feedback_item_count"] >= 3
    assert report["drift_monitoring_report"]["monitor_count"] >= 1
    assert report["rollback_kill_switch_report"]["kill_switch_count"] >= 1
    assert report["auditability_lineage_report"]["audit_ready"] is True
    assert report["end_to_end_readiness_gate"]["readiness_state"] in {"ready", "blocked", "caution"}
    assert report["elite_runbook_report"]["steps"][0]["step"] == "verify_void_registry_and_data_quality"


def test_readiness_gate_blocks_critical_monitor_and_kill_switch():
    gate = ops.end_to_end_readiness_gate(
        live_feedback_loop_report={"live_feedback_ready": True},
        drift_monitoring_report={"critical_monitor_count": 1},
        rollback_kill_switch_report={"deployment_may_scale": False},
        auditability_lineage_report={"audit_ready": True},
        learning_ops_readiness_report={"ready_for_next_batch": True},
    )

    assert gate["ready"] is False
    assert "critical_drift_or_quality_monitor" in gate["blockers"]
    assert "kill_switch_blocks_scale" in gate["blockers"]
