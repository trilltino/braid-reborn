//! Offline policy administration; never accepts a network-supplied principal.
use braid_core::access::{
    administration::{AdoptionPlan, Assignment},
    PolicyChange, PolicyStore, Principal, Role,
};
use serde::de::DeserializeOwned;
use std::path::Path;
use tokio::io::AsyncReadExt;

async fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, Box<dyn std::error::Error>> {
    let file = tokio::fs::File::open(path).await?;
    let mut bytes = Vec::new();
    file.take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err("Administrative input exceeds 16 MiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn principal(kind: &str, id: &str) -> Result<Principal, Box<dyn std::error::Error>> {
    match kind {
        "user" => Ok(Principal::User(id.into())),
        "peer" => Ok(Principal::Peer(id.into())),
        _ => Err("Principal kind must be user or peer".into()),
    }
}
fn print(value: &impl serde::Serialize) -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let usage = "braid-admin COMMAND POLICY_ROOT ...\n inventory\n scan chat|wiki|local|v2|iroh DATA_ROOT\n get RESOURCE\n plan ASSIGNMENTS_JSON PLAN_JSON\n apply PLAN_JSON [--apply]\n rollback PLAN_JSON [--apply]\n grant RESOURCE ACTOR_KIND ACTOR_ID SUBJECT_KIND SUBJECT_ID ROLE\n revoke RESOURCE ACTOR_KIND ACTOR_ID SUBJECT_KIND SUBJECT_ID\n public RESOURCE ACTOR_KIND ACTOR_ID true|false\n transfer RESOURCE ACTOR_KIND ACTOR_ID SUBJECT_KIND SUBJECT_ID\nPlans and rollback are dry-run unless --apply is present. Stop application writers before rollback.";
    if args.len() < 2 {
        return Err(usage.into());
    }
    let policies = PolicyStore::new((&args[1]).into())?;
    match (args[0].as_str(), &args[2..]) {
        ("inventory", []) => print(&policies.inventory().await?)?,
        ("scan", [kind, root]) => print(&policies.scan_data(kind, Path::new(root)).await?)?,
        ("get", [resource]) => print(&policies.get(resource).await?)?,
        ("plan", [assignments, destination]) => {
            // Never replace an existing review/backup artifact.
            if tokio::fs::try_exists(destination).await? {
                return Err("Plan file already exists".into());
            }
            let assignments: Vec<Assignment> = read_json(Path::new(assignments)).await?;
            let plan = policies.plan_adoption(assignments).await?;
            let bytes = serde_json::to_vec_pretty(&plan)?;
            // create_new preserves another administrator's plan on a race.
            use tokio::io::AsyncWriteExt;
            let mut options = tokio::fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut file = options.open(destination).await?;
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            print(&plan)?;
        }
        (command @ ("apply" | "rollback"), rest)
            if rest.len() == 1 || (rest.len() == 2 && rest[1] == "--apply") =>
        {
            let plan: AdoptionPlan = read_json(Path::new(&rest[0])).await?;
            let status = if rest.len() == 1 {
                policies.inspect_adoption(&plan).await?
            } else if command == "apply" {
                policies.apply_adoption(&plan).await?
            } else {
                policies.rollback_adoption(&plan).await?
            };
            print(&status)?;
        }
        ("grant", [resource, kind, actor, target_kind, target, role]) => {
            let role = match role.as_str() {
                "reader" => Role::Reader,
                "editor" => Role::Editor,
                "owner" => Role::Owner,
                _ => return Err("Role must be reader, editor or owner".into()),
            };
            print(
                &policies
                    .change(
                        resource,
                        &principal(kind, actor)?,
                        PolicyChange::Grant(principal(target_kind, target)?, role),
                    )
                    .await?,
            )?;
        }
        (command @ ("revoke" | "transfer"), [resource, kind, actor, target_kind, target]) => {
            let target = principal(target_kind, target)?;
            let change = if command == "revoke" {
                PolicyChange::Revoke(target)
            } else {
                PolicyChange::Transfer(target)
            };
            print(
                &policies
                    .change(resource, &principal(kind, actor)?, change)
                    .await?,
            )?;
        }
        ("public", [resource, kind, actor, public]) => print(
            &policies
                .change(
                    resource,
                    &principal(kind, actor)?,
                    PolicyChange::PublicRead(public.parse()?),
                )
                .await?,
        )?,
        _ => return Err(usage.into()),
    }
    Ok(())
}
