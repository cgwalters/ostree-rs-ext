//! Module used for integration tests; should not be public.


pub(crate) fn detectenv() -> &'static str {
    match (crate::container_utils::running_in_container(), crate::container_utils::has_ostree()) {
        (true, true) => "ostree-container",
        (true, false) => "container",
        (false, true) => "ostree",
        (false, false) => "none",
    }
}
