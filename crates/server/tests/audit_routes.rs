#[test]
fn page_routes_can_be_constructed_without_catchall_panics() {
    let _ = local_link_server::core::pages::router();
}
