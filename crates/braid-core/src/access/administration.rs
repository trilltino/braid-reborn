//! Local policy adoption. Plans preserve the absence of an old policy; resource
//! payloads are never rewritten. This is not an engine-format migration tool.
use super::*;
use std::collections::HashSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assignment {
    pub resource: String,
    pub owner: Principal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdoptionPlan {
    pub schema_version: u32,
    pub id: String,
    pub policy_root: PathBuf,
    /// Every entry had no policy when this backup/plan was prepared. Only the
    /// exact generated policy may be treated as an already applied entry.
    pub policies: Vec<ResourcePolicy>,
}

#[derive(Debug, Serialize)]
pub struct AdoptionStatus {
    pub missing: Vec<String>,
    pub applied: Vec<String>,
    pub changed: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ResourceInventory {
    pub file: PathBuf,
    pub bytes: u64,
    pub sha256: String,
    /// None means the old hashed checkpoint has no recoverable original name.
    /// An administrator must supply its verified mapping, never guess an ID.
    pub resource: Option<String>,
    pub policy: Option<ResourcePolicy>,
}

impl PolicyStore {
    /// Read-only legacy inventory. Stop writers for a stable backup manifest.
    /// Internal directories are excluded; scan each configured backend root.
    pub async fn scan_data(
        &self,
        kind: &str,
        root: &std::path::Path,
    ) -> Result<Vec<ResourceInventory>, AccessError> {
        if !matches!(kind, "chat" | "wiki" | "local" | "v2" | "iroh") {
            return Err(AccessError::Invalid("Unknown data namespace".into()));
        }
        let root = std::path::absolute(root)?;
        let mut directories = vec![(root.clone(), 0)];
        let mut entries_seen = 0;
        let mut result = Vec::new();
        while let Some((directory, depth)) = directories.pop() {
            reject_link(&tokio::fs::symlink_metadata(&directory).await?)?;
            let mut entries = tokio::fs::read_dir(directory).await?;
            while let Some(entry) = entries.next_entry().await? {
                entries_seen += 1;
                if entries_seen > 10000 {
                    return Err(AccessError::Invalid(
                        "Inventory exceeds 10000 entries".into(),
                    ));
                }
                if entry.file_name().to_string_lossy().starts_with('.') {
                    continue;
                }
                let path = entry.path();
                let metadata = tokio::fs::symlink_metadata(&path).await?;
                reject_link(&metadata)?;
                if metadata.is_dir() {
                    if depth >= 32 {
                        return Err(AccessError::Invalid("Inventory exceeds depth 32".into()));
                    }
                    directories.push((path, depth + 1));
                    continue;
                }
                if !metadata.is_file() {
                    return Err(AccessError::Invalid("Unsupported inventory entry".into()));
                }
                if matches!(kind, "chat" | "v2" | "iroh")
                    && path.extension().is_none_or(|ext| ext != "json")
                {
                    continue;
                }
                if matches!(kind, "wiki" | "local")
                    && path.file_name().is_some_and(|name| {
                        name.to_string_lossy().starts_with("p-")
                            && path.extension().is_some_and(|ext| ext == "json")
                    })
                {
                    continue;
                }
                let mut file = tokio::fs::File::open(&path).await?;
                let mut hasher = Sha256::new();
                let mut bytes = 0u64;
                let mut json = Vec::new();
                let mut buffer = vec![0u8; 65536];
                loop {
                    let n = file.read(&mut buffer).await?;
                    if n == 0 {
                        break;
                    }
                    bytes += n as u64;
                    hasher.update(&buffer[..n]);
                    if matches!(kind, "chat" | "v2" | "iroh") {
                        if bytes > 16 * 1024 * 1024 {
                            return Err(AccessError::Invalid(
                                "Inventory JSON exceeds 16 MiB; inspect separately".into(),
                            ));
                        }
                        json.extend_from_slice(&buffer[..n]);
                    }
                }
                let resource = if matches!(kind, "chat" | "v2" | "iroh") {
                    let value: serde_json::Value = serde_json::from_slice(&json)?;
                    if kind == "chat" {
                        let id = value
                            .get("id")
                            .and_then(|value| value.as_str())
                            .filter(|_| value.get("crdt_state").is_some())
                            .ok_or_else(|| AccessError::Invalid("Unrecognized chat data".into()))?;
                        Some(named_resource(kind, id)?)
                    } else {
                        if value.get("heads").is_none() || value.get("version_graph").is_none() {
                            return Err(AccessError::Invalid("Unrecognized replica data".into()));
                        }
                        value
                            .get("resource")
                            .and_then(|value| value.as_str())
                            .map(|id| {
                                if kind == "iroh" {
                                    validate_resource(id)?;
                                    Ok(id.to_owned())
                                } else {
                                    named_resource(kind, id)
                                }
                            })
                            .transpose()?
                    }
                } else {
                    let relative = path
                        .strip_prefix(&root)
                        .map_err(|_| AccessError::Invalid("Inventory escaped root".into()))?
                        .to_str()
                        .ok_or_else(|| AccessError::Invalid("Non-UTF-8 inventory name".into()))?
                        .replace('\\', "/");
                    Some(named_resource(kind, &relative)?)
                };
                let policy = match &resource {
                    Some(resource) => self.get(resource).await?,
                    None => None,
                };
                result.push(ResourceInventory {
                    file: path,
                    bytes,
                    sha256: format!("{:x}", hasher.finalize()),
                    resource,
                    policy,
                });
            }
        }
        result.sort_by(|a, b| a.file.cmp(&b.file));
        Ok(result)
    }
    /// Preparation is read-only. Save the returned plan before applying it.
    pub async fn plan_adoption(
        &self,
        assignments: Vec<Assignment>,
    ) -> Result<AdoptionPlan, AccessError> {
        if assignments.is_empty() || assignments.len() > 10000 {
            return Err(AccessError::Invalid(
                "Adoption needs 1 through 10000 explicit assignments".into(),
            ));
        }
        let mut seen = HashSet::new();
        let mut policies = Vec::with_capacity(assignments.len());
        for assignment in assignments {
            validate_resource(&assignment.resource)?;
            assignment.owner.validate()?;
            if !seen.insert(assignment.resource.clone()) {
                return Err(AccessError::Invalid("Duplicate adoption resource".into()));
            }
            if self.get(&assignment.resource).await?.is_some() {
                return Err(AccessError::AlreadyOwned);
            }
            policies.push(ResourcePolicy {
                schema_version: 1,
                resource: assignment.resource,
                owner: assignment.owner,
                grants: vec![],
                public_read: false,
                revision: 0,
                creation_id: uuid::Uuid::new_v4().to_string(),
                write_generation: 0,
            });
        }
        Ok(AdoptionPlan {
            schema_version: 1,
            id: uuid::Uuid::new_v4().to_string(),
            policy_root: self.root.clone(),
            policies,
        })
    }

    fn validate_plan(&self, plan: &AdoptionPlan) -> Result<(), AccessError> {
        if plan.schema_version != 1
            || plan.policy_root != self.root
            || uuid::Uuid::parse_str(&plan.id).is_err()
            || plan.policies.is_empty()
            || plan.policies.len() > 10000
        {
            return Err(AccessError::Invalid(
                "Invalid adoption schema, root, identity or size".into(),
            ));
        }
        let mut seen = HashSet::new();
        for policy in &plan.policies {
            policy.validate(&policy.resource)?;
            if policy.revision != 0
                || policy.write_generation != 0
                || policy.public_read
                || !policy.grants.is_empty()
                || !seen.insert(&policy.resource)
            {
                return Err(AccessError::Invalid(
                    "Adoption must create unique private policies without grants".into(),
                ));
            }
        }
        Ok(())
    }

    /// Also serves as dry-run and post-apply validation; never creates a file.
    pub async fn inspect_adoption(
        &self,
        plan: &AdoptionPlan,
    ) -> Result<AdoptionStatus, AccessError> {
        self.validate_plan(plan)?;
        let mut status = AdoptionStatus {
            missing: vec![],
            applied: vec![],
            changed: vec![],
        };
        for expected in &plan.policies {
            match self.get(&expected.resource).await? {
                None => status.missing.push(expected.resource.clone()),
                Some(actual) if actual == *expected => {
                    status.applied.push(expected.resource.clone())
                }
                Some(_) => status.changed.push(expected.resource.clone()),
            }
        }
        Ok(status)
    }

    /// Each entry is independently atomic. Interruption is resumed with the same
    /// saved plan. A competing owner or later policy change is never overwritten.
    pub async fn apply_adoption(&self, plan: &AdoptionPlan) -> Result<AdoptionStatus, AccessError> {
        self.validate_plan(plan)?;
        let _migration = writer_lease(&self.root.join("adoption.lock")).await?;
        if !self.inspect_adoption(plan).await?.changed.is_empty() {
            return Err(AccessError::AlreadyOwned);
        }
        for expected in &plan.policies {
            let path = self.path(&expected.resource)?;
            let _guard = document_lock(path.clone()).await;
            let _lease = writer_lease(&path.with_extension("lock")).await?;
            match self.get(&expected.resource).await? {
                None => self.save(expected).await?,
                Some(actual) if actual == *expected => {}
                Some(_) => return Err(AccessError::AlreadyOwned),
            }
        }
        self.inspect_adoption(plan).await
    }

    /// Roll back policy creation only. An owner/grant change blocks rollback;
    /// callers must stop new-format writers before a broader data rollback.
    pub async fn rollback_adoption(
        &self,
        plan: &AdoptionPlan,
    ) -> Result<AdoptionStatus, AccessError> {
        self.validate_plan(plan)?;
        let _migration = writer_lease(&self.root.join("adoption.lock")).await?;
        if !self.inspect_adoption(plan).await?.changed.is_empty() {
            return Err(AccessError::Invalid(
                "Rollback blocked by a changed policy".into(),
            ));
        }
        for expected in &plan.policies {
            let path = self.path(&expected.resource)?;
            let _guard = document_lock(path.clone()).await;
            let _lease = writer_lease(&path.with_extension("lock")).await?;
            match self.get(&expected.resource).await? {
                None => {}
                Some(actual) if actual == *expected => {
                    tokio::fs::remove_file(&path).await?;
                    #[cfg(unix)]
                    {
                        let root = self.root.clone();
                        tokio::task::spawn_blocking(move || std::fs::File::open(root)?.sync_all())
                            .await
                            .map_err(std::io::Error::other)??;
                    }
                }
                Some(_) => {
                    return Err(AccessError::Invalid(
                        "Rollback blocked by a changed policy".into(),
                    ))
                }
            }
        }
        self.inspect_adoption(plan).await
    }

    /// Inventory persisted policies, including private resources. Local use only.
    pub async fn inventory(&self) -> Result<Vec<ResourcePolicy>, AccessError> {
        let mut directory = match tokio::fs::read_dir(&self.root).await {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(error) => return Err(error.into()),
        };
        let mut policies = Vec::new();
        while let Some(entry) = directory.next_entry().await? {
            if entry
                .path()
                .extension()
                .is_none_or(|extension| extension != "json")
            {
                continue;
            }
            if !entry.file_type().await?.is_file() {
                return Err(AccessError::Invalid("Non-file policy entry".into()));
            }
            let file = tokio::fs::File::open(entry.path()).await?;
            let mut bytes = Vec::new();
            file.take(65537).read_to_end(&mut bytes).await?;
            if bytes.len() > 65536 {
                return Err(AccessError::Invalid("Policy exceeds 64 KiB".into()));
            }
            let policy: ResourcePolicy = serde_json::from_slice(&bytes)?;
            policy.validate(&policy.resource)?;
            if self.path(&policy.resource)? != entry.path() {
                return Err(AccessError::Invalid(
                    "Policy filename does not match resource".into(),
                ));
            }
            policies.push(policy);
        }
        policies.sort_by(|a, b| a.resource.cmp(&b.resource));
        Ok(policies)
    }
}

fn reject_link(metadata: &std::fs::Metadata) -> Result<(), AccessError> {
    if braid_http::durable::is_link_or_reparse_point(metadata) {
        return Err(AccessError::Invalid(
            "Inventory refuses symlinks and reparse points".into(),
        ));
    }
    Ok(())
}
