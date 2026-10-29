//! Pure signal-admission helpers used by the runtime.

pub(crate) fn is_political_title(t: &str) -> bool {
    let t = t.to_ascii_lowercase();
    const KW: &[&str] = &[
        "president",
        "election",
        "senate",
        "primary",
        "democratic",
        "republican",
        "congress",
        "governor",
        "mayor",
        "parliament",
        "prime minister",
        "putin",
        "xi jinping",
        "kamala",
        "newsom",
        "trump",
        "biden",
        "nominee",
        "electoral",
        "referendum",
        "impeach",
        "cabinet",
        "out as ",
        "win the 20",
    ];
    KW.iter().any(|k| t.contains(k))
}
