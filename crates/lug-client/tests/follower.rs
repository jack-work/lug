//! The follower: converge, survive a reconnect, survive a gap.

mod support;

use lug_client::{Event, Hub, Status};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use support::sim::Sim;
use support::unix::Mock;

fn create(key: &str, value: serde_json::Value) -> serde_json::Value {
    json!({ "Create": { key: value } })
}

fn update(key: &str, value: serde_json::Value) -> serde_json::Value {
    json!({ "Update": { key: value } })
}

/// The patch sequence every test folds: creates, updates, a nested edit.
fn sequence() -> Vec<serde_json::Value> {
    let mut patches = vec![
        create("title", json!("lug")),
        create("author", json!({ "name": "Gluck" })),
        update("title", json!("lug: a little log")),
        json!({ "Update": { "author": { "Create": { "city": "Atlanta" } } } }),
    ];
    for n in 0..20 {
        patches.push(create(&format!("k{n}"), json!(n)));
    }
    patches
}

#[tokio::test]
async fn the_follower_converges_on_the_servers_view() {
    let mock = Mock::start("converge").await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");
    let mut follower = hub.follow("log").await.expect("follow");

    let patches = sequence();
    let wanted = patches.len() as u64;
    for patch in &patches {
        mock.sim
            .append(std::slice::from_ref(patch))
            .expect("append");
    }

    let state = follower.wait_for(wanted).await.expect("converge");
    assert_eq!(state.status, Status::Live);
    assert!(follower.reducible());
    let snapshot = state.snapshot.expect("a reducible log has a view");
    assert_eq!(snapshot.version(), mock.sim.version());
    assert_eq!(snapshot.root().to_json(), mock.sim.root());

    // The JSON view a renderer binds to says the same thing, cached per
    // version rather than rebuilt per call.
    let view = follower.view().expect("view");
    assert_eq!(view.version, mock.sim.version());
    assert_eq!(view.value, mock.sim.root());
}

#[tokio::test]
async fn the_view_preamble_may_be_a_bare_root() {
    let sim = Arc::new(Sim::new());
    sim.bare_views
        .store(true, std::sync::atomic::Ordering::Relaxed);
    sim.append(&sequence()).expect("seed");
    let mock = Mock::start_with("bare", sim.clone()).await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");

    let mut follower = hub.follow("log").await.expect("follow");
    let state = follower.wait_for(sim.version()).await.expect("converge");
    assert_eq!(state.snapshot.expect("view").root().to_json(), sim.root());
}

#[tokio::test]
async fn the_follower_survives_a_reconnect() {
    let mock = Mock::start("follower-reconnect").await;
    let hub = Hub::builder(mock.transport())
        .connections(1)
        .connect()
        .await
        .expect("connect");
    let mut follower = hub.follow("log").await.expect("follow");

    mock.sim.append(&sequence()).expect("seed");
    follower
        .wait_for(mock.sim.version())
        .await
        .expect("first convergence");

    mock.kill_connections();
    // Patches that land while the follower is detached must still show up.
    mock.sim
        .append(&[create("after", json!("the reconnect"))])
        .expect("append");

    let state = follower
        .wait_for(mock.sim.version())
        .await
        .expect("converged again");
    assert_eq!(state.status, Status::Live);
    assert_eq!(
        state.snapshot.expect("view").root().to_json(),
        mock.sim.root()
    );
    assert!(
        mock.sim.connections_used() >= 2,
        "the follower never reconnected"
    );
}

#[tokio::test]
async fn the_follower_recovers_from_a_gap() {
    let mock = Mock::start("gap").await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");
    let mut follower = hub.follow("log").await.expect("follow");
    let events = follower.records().expect("events, handed out once");
    assert!(follower.records().is_none(), "events are handed out once");

    mock.sim.append(&sequence()).expect("seed");
    follower
        .wait_for(mock.sim.version())
        .await
        .expect("first convergence");

    // The server reclaims everything the follower has not seen, then moves on:
    // the records are gone, so only a refetched view can be correct.
    mock.sim
        .append(&[create("hidden", json!(1))])
        .expect("append");
    mock.sim.reclaim(mock.sim.version());
    mock.sim
        .append(&[create("after_gap", json!(2))])
        .expect("append");

    let state = follower
        .wait_for(mock.sim.version())
        .await
        .expect("recovered");
    assert_eq!(
        state.snapshot.expect("view").root().to_json(),
        mock.sim.root()
    );

    let mut events = events;
    let mut gap = None;
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(50), events.recv()).await
    {
        if let Event::Gap { from, to } = event {
            gap = Some((from, to));
        }
    }
    let (from, to) = gap.expect("the gap was hidden from the consumer");
    assert!(
        to > from,
        "a gap that covers nothing is not a gap: {from}..{to}"
    );
    assert!(
        to >= mock.sim.version() - 1,
        "the gap must run to where the view resumed"
    );
}

#[tokio::test]
async fn records_reach_the_consumer_in_order() {
    let mock = Mock::start("events").await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");
    let mut follower = hub.follow("log").await.expect("follow");
    let mut events = follower.records().expect("events");

    let patches = sequence();
    // Fold the first patch before taking the stream, so what follows is
    // unambiguously pushed rather than carried by the view preamble.
    mock.sim.append(&patches[..1]).expect("seed");
    follower.wait_for(1).await.expect("preamble");
    mock.sim.append(&patches[1..]).expect("rest");
    follower
        .wait_for(patches.len() as u64)
        .await
        .expect("converge");

    let mut expected = 2;
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(50), events.recv()).await
    {
        if let Event::Record(record) = event {
            assert_eq!(record.version, expected, "records arrived out of order");
            expected += 1;
        }
    }
    assert_eq!(expected - 1, patches.len() as u64);
}

#[tokio::test]
async fn a_plain_log_still_ticks_its_versions() {
    let mock = Mock::start("plain").await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");
    hub.create("events", false).await.expect("create");

    let mut follower = hub.follow("events").await.expect("follow");
    mock.sim.append(&sequence()).expect("seed");

    let state = follower
        .wait_for(mock.sim.version())
        .await
        .expect("caught up");
    assert!(
        state.snapshot.is_none(),
        "a log that keeps no view must not pretend to have one"
    );
    assert!(!follower.reducible(), "a plain log is not reducible");
    assert!(follower.view().is_none());
    assert_eq!(state.version, mock.sim.version());
    assert_eq!(*follower.changed().borrow(), mock.sim.version());
}

#[tokio::test]
async fn following_a_missing_log_stops_with_the_reason() {
    let mock = Mock::start("nolog-follow").await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");
    let mut follower = hub.follow("absent").await.expect("follow");

    loop {
        let state = follower.next_change().await.expect("state");
        if let Status::Failed(e) = state.status {
            assert!(e.to_string().contains("no log absent"), "{e}");
            return;
        }
        assert!(
            !state.status.is_final(),
            "stopped for the wrong reason: {:?}",
            state.status
        );
    }
}

#[tokio::test]
async fn clones_see_the_same_stream() {
    let mock = Mock::start("clone").await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");
    let follower = hub.follow("log").await.expect("follow");
    let mut one = follower.clone();
    let mut two = follower.clone();

    mock.sim.append(&sequence()).expect("seed");
    let target = mock.sim.version();
    let (a, b) = tokio::join!(one.wait_for(target), two.wait_for(target));
    assert_eq!(
        a.expect("first").snapshot.expect("view").root().to_json(),
        b.expect("second").snapshot.expect("view").root().to_json()
    );
    assert_eq!(follower.version(), target);
}

#[tokio::test]
async fn the_gap_is_queued_before_the_refetched_version_wakes_a_renderer() {
    use futures::task::{ArcWake, waker};
    use std::future::Future;
    use std::sync::Mutex;
    use std::task::Context;
    use tokio::sync::mpsc;

    struct Renderer {
        events: Mutex<mpsc::Receiver<Event>>,
        observations: Mutex<Vec<Vec<Event>>>,
        follower: lug_client::Follower,
        views: Mutex<Vec<u64>>,
    }

    impl ArcWake for Renderer {
        fn wake_by_ref(this: &Arc<Self>) {
            // Observe at the notification itself, not after the producer gets
            // another turn to repair an incorrectly ordered publication.
            let mut events = this.events.lock().unwrap();
            let mut available = Vec::new();
            while let Ok(event) = events.try_recv() {
                available.push(event);
            }
            this.observations.lock().unwrap().push(available);
            this.views.lock().unwrap().push(this.follower.state().version);
        }
    }

    let mock = Mock::start("gap-order").await;
    mock.sim.append(&[create("seed", json!(1))]).expect("seed");
    let hub = Hub::builder(mock.transport()).connections(1).connect().await.expect("connect");
    let mut follower = hub.follow("log").await.expect("follow");
    follower.wait_for(1).await.expect("preamble");
    let renderer = Arc::new(Renderer {
        events: Mutex::new(follower.records().expect("events")),
        observations: Mutex::new(Vec::new()),
        follower: follower.clone(),
        views: Mutex::new(Vec::new()),
    });
    let mut versions = follower.changed();
    versions.borrow_and_update();
    let notification = versions.changed();
    futures::pin_mut!(notification);
    let waker = waker(renderer.clone());
    assert!(notification.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    // No await between these: the subscriber cannot see the hidden record
    // before retention removes it, on this current-thread runtime.
    mock.sim.append(&[create("hidden", json!(2))]).expect("hidden");
    mock.sim.reclaim(2);
    mock.sim.append(&[create("after", json!(3))]).expect("after");
    tokio::time::timeout(Duration::from_secs(5), follower.wait_for(3))
        .await.expect("recovery deadline").expect("recovered");

    let observations = renderer.observations.lock().unwrap();
    assert!(!observations.is_empty(), "the version watch did not wake");
    assert!(
        observations[0].contains(&Event::Gap { from: 1, to: 3 }),
        "renderer woke before the recovery Gap was queued: {observations:?}"
    );
    assert_eq!(*renderer.views.lock().unwrap(), vec![3], "version watch woke before the view was published");
}
