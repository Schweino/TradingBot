import step2_score_cache


def test_decision_hash_identity_rewrite_strips_variant_scoped_route_audit():
    result = {
        "variant": "old",
        "routes": [
            {"name": "old_route", "match": {"ticker": "CLSK"}, "action": "force_short"}
        ],
        "route_audit": {"routes": [{"route": "old_route"}]},
        "routed_profile_safety": {"ok": True, "status": "ok"},
    }
    variant = type(
        "Variant",
        (),
        {
            "name": "new",
            "weights": {},
            "bias": 0.0,
            "routes": [
                {"name": "new_route", "match": {"ticker": "RIOT"}, "action": "force_long"}
            ],
        },
    )()

    rewritten = step2_score_cache._result_with_variant_identity(result, variant)

    assert rewritten["variant"] == "new"
    assert rewritten["routes"][0]["name"] == "new_route"
    assert "route_audit" not in rewritten
    assert "routed_profile_safety" not in rewritten
    assert rewritten["route_audit_identity_status"] == "sanitized_for_variant_identity_rewrite"
