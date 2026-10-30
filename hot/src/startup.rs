//! One-time config, operator-capability, funder lease and signing-identity bootstrap.
use super::*;

pub(crate) struct Identity {
    pub(crate) operator_access: Arc<OperatorAccess>,
    pub(crate) root: Root,
    pub(crate) lanes: Vec<copybot_hot::lanes::Lane>,
    pub(crate) shadow: bool,
    pub(crate) live: bool,
    pub(crate) boot_profile: copybot_hot::config::BootProfile,
    pub(crate) funder: String,
    pub(crate) sig_type: u8,
    pub(crate) pk: Option<[u8;32]>,
    pub(crate) signer_addr: String,
    pub(crate) _execution_lease: Option<copybot_hot::lease::ExecutionLease>,
}

pub(crate) fn load() -> Identity {
    eprintln!("Abomination81 Copybot — execution engine");
    let operator_access = match OperatorAccess::from_environment() {
        Ok(access) => Arc::new(access),
        Err(error) => {
            eprintln!("OPERATOR CONFIG ERROR: {error}");
            std::process::exit(2);
        }
    };
    let cfg_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "deploy/copybot2.toml".into());
    let root = match Root::load(&cfg_path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("CONFIG ERROR: {e}");
            std::process::exit(2);
        }
    };
    let boot_profile = match root.boot_profile() {
        Ok(profile) => profile,
        Err(error) => {
            eprintln!("CONFIG ERROR: {error}");
            std::process::exit(2);
        }
    };
    let lanes = match root.build_lanes() {
        Ok(l) => l,
        Err(e) => {
            eprintln!("CONFIG ERROR: {e}");
            std::process::exit(2);
        }
    };
    if boot_profile == copybot_hot::config::BootProfile::DryObserveOnly {
        eprintln!("[boot] DRY OBSERVE ONLY: zero enabled lanes; no copy orders");
    }
    let shadow = root.bot.mode == "shadow";
    let live = root.bot.mode == "live" || shadow;
    let funder = root.bot.funder.clone();
    let signer = root.bot.signer.clone();
    let sig_type = match root.bot.signature_type {
        Some(signature_type) => signature_type.wire(),
        None => {
            eprintln!("CONFIG ERROR: signature_type must explicitly match wallet custody");
            std::process::exit(2);
        }
    };
    if addr20(&funder).is_err() || addr20(&signer).is_err() {
        eprintln!("CONFIG ERROR: funder/signer must be 20-byte hex addresses");
        std::process::exit(2);
    }
    let _execution_lease = if live && !shadow {
        match copybot_hot::lease::ExecutionLease::acquire_for_funder(&funder) {
            Ok(lease) => {
                eprintln!("[lease] acquired {}", lease.path.display());
                Some(lease)
            }
            Err(e) => {
                eprintln!("REFUSING TO START: {e}");
                std::process::exit(2);
            }
        }
    } else {
        None
    };
    let pk_hex = std::env::var("PRIVATE_KEY").unwrap_or_default();
    let pk = match hex::decode(pk_hex.trim_start_matches("0x")) {
        Ok(b) if b.len() == 32 => {
            let mut k = [0u8; 32];
            k.copy_from_slice(&b);
            Some(k)
        }
        _ => None,
    };
    // Shadow is an order-submitting execution mode, NOT a dry run.
    // Neither it nor live mode may invent a throwaway signing identity.
    if live && pk.is_none() {
        eprintln!("REFUSING TO START: execution mode requires a valid signer key");
        std::process::exit(2);
    }
    let signer_addr: String = match pk.as_ref().map(copybot_hot::auth::address_from_key)
    {
        Some(Ok(a)) if a.eq_ignore_ascii_case(&signer) => a,
        Some(Ok(_)) => {
            eprintln!("REFUSING TO START: configured signer does not match signer key");
            std::process::exit(2);
        }
        Some(Err(_)) => {
            eprintln!("REFUSING TO START: signer key cannot derive an identity");
            std::process::exit(2);
        }
        None => signer.clone(),
    };
    Identity { operator_access, root, lanes, shadow, live, boot_profile, funder, sig_type, pk, signer_addr, _execution_lease }
}

/// Preserve startup incident visibility without altering halt-latch ownership.
pub(crate) fn report_incidents(
    incidents: &Arc<std::sync::Mutex<copybot_hot::incidents::Incidents>>,
    incidents_path: &str,
) {
        let g = incidents.lock().unwrap();
        if let Some(e) = g.unreadable() {
            eprintln!(
                "[incidents] CANNOT READ {incidents_path}: {e} — EVERY lane is \
                       latched for buys until this is fixed (exits still mirror)"
            );
        } else if !g.is_empty() {
            for i in g.open_incidents() {
                eprintln!(
                    "[incidents] OPEN: {} — {} ({}) raised at {}; buys stay halted \
                           until cleared",
                    i.lane, i.why, i.kind, i.t
                );
            }
        }
}

/// Report external-funding basis without inventing a physical equity P&L.
pub(crate) fn report_funding(
    funding: &Arc<std::sync::Mutex<copybot_hot::funding::Funding>>,
) {
    match funding.lock().unwrap().basis() {
        Some(b) => eprintln!("[funding] declared external funding: ${b:.2}"),
        None => {
            eprintln!(
                "[funding] NO external funding declared — physical P&L will \
                           read UNKNOWN until you record a deposit (see run/*.funding)"
            )
        }
    }
}
