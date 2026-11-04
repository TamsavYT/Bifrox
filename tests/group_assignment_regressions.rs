use bifrox::protocol::wire::MemberAssignment;
use bifrox::server::coordinator::GroupCoordinator;
use std::time::Duration;

#[test]
fn replacements_clear_old_topics_but_stable_reconnects_preserve_assignment() {
    let c = GroupCoordinator::with_rebalance_delay(Duration::ZERO);
    let (leader, generation, _, _) = c
        .join_group("g", "leader", None, vec!["range".into()], None, false)
        .unwrap();
    c.close_join_window("g");
    let assignment = MemberAssignment {
        member_id: leader.clone(),
        topic: "old-topic".into(),
        partitions: vec![0],
    };
    let first = c
        .sync_group("g", generation, &leader, vec![assignment])
        .unwrap();
    assert_eq!(first.get("old-topic"), Some(&vec![0]));
    assert_eq!(
        c.sync_group("g", generation, &leader, vec![]).unwrap(),
        first,
        "stable same-generation query preserves ownership"
    );
    let (_, next_generation, _, _) = c
        .join_group("g", "second", None, vec!["range".into()], None, false)
        .unwrap();
    assert_ne!(next_generation, generation);
    c.close_join_window("g");
    assert!(
        c.sync_group("g", next_generation, &leader, vec![])
            .unwrap()
            .is_empty(),
        "empty replacement during rebalance clears old ownership"
    );
}

#[test]
fn conflicting_ownership_is_rejected_without_changing_assignments() {
    let c = GroupCoordinator::with_rebalance_delay(Duration::ZERO);
    let (leader, _, _, _) = c
        .join_group("g", "leader", None, vec!["range".into()], None, false)
        .unwrap();
    let (second, generation, _, _) = c
        .join_group("g", "second", None, vec!["range".into()], None, false)
        .unwrap();
    c.close_join_window("g");
    let make = |member: String| MemberAssignment {
        member_id: member,
        topic: "t".into(),
        partitions: vec![0],
    };
    c.sync_group("g", generation, &leader, vec![make(leader.clone())])
        .unwrap();
    assert!(c
        .sync_group(
            "g",
            generation,
            &leader,
            vec![make(leader.clone()), make(second)]
        )
        .is_err());
    assert_eq!(
        c.sync_group("g", generation, &leader, vec![])
            .unwrap()
            .get("t"),
        Some(&vec![0])
    );
}
