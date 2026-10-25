//! Durable resource policy. Transport authentication supplies the principal;
//! knowing a resource name, version or content hash never supplies authority.
use braid_http::durable::{atomic_write, document_lock, writer_lease};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

pub mod administration;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum Principal {
    User(String),
    Peer(String),
}
impl Principal {
    fn validate(&self) -> Result<(), AccessError> {
        let (Self::User(id) | Self::Peer(id)) = self;
        if id.is_empty() || id.len() > 512 || id.chars().any(char::is_control) {
            return Err(AccessError::Invalid("Invalid principal".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Owner,
    Editor,
    Reader,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Read,
    Write,
    Manage,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Grant {
    pub principal: Principal,
    pub role: Role,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourcePolicy {
    pub schema_version: u32,
    pub resource: String,
    pub owner: Principal,
    pub grants: Vec<Grant>,
    pub public_read: bool,
    pub revision: u64,
    /// Immutable identity distinguishes resumable adoption from another writer.
    pub creation_id: String,
    /// Advanced before a protected data write, so adoption rollback can never
    /// discard ownership after a new-format writer has started publishing.
    pub write_generation: u64,
}
impl ResourcePolicy {
    pub fn permits(&self, principal: Option<&Principal>, action: Action) -> bool {
        if action == Action::Read && self.public_read {
            return true;
        }
        let Some(principal) = principal else {
            return false;
        };
        if principal == &self.owner {
            return true;
        }
        self.grants.iter().any(|grant| {
            &grant.principal == principal
                && match grant.role {
                    Role::Owner => true,
                    Role::Editor => action != Action::Manage,
                    Role::Reader => action == Action::Read,
                }
        })
    }
    fn validate(&self, resource: &str) -> Result<(), AccessError> {
        validate_resource(resource)?;
        if self.schema_version != 1
            || self.resource != resource
            || self.grants.len() > 256
            || uuid::Uuid::parse_str(&self.creation_id).is_err()
        {
            return Err(AccessError::Invalid(
                "Invalid policy schema, resource or grant count".into(),
            ));
        }
        self.owner.validate()?;
        for (index, grant) in self.grants.iter().enumerate() {
            grant.principal.validate()?;
            if self.grants[..index]
                .iter()
                .any(|old| old.principal == grant.principal)
            {
                return Err(AccessError::Invalid("Duplicate policy principal".into()));
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AccessError {
    #[error("Access denied")]
    Denied,
    #[error("Resource already has an owner")]
    AlreadyOwned,
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Debug)]
pub enum PolicyChange {
    Grant(Principal, Role),
    Revoke(Principal),
    PublicRead(bool),
    Transfer(Principal),
}

#[derive(Clone, Debug)]
pub struct PolicyStore {
    root: PathBuf,
}

/// Keeps authorization stable through one data publication. A new private
/// policy is staged, then durably reserved immediately before the data write.
pub struct MutationPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _lease: std::fs::File,
    pending: Option<ResourcePolicy>,
}
impl MutationPermit {
    pub async fn reserve(&mut self, store: &PolicyStore) -> Result<(), AccessError> {
        if let Some(policy) = &self.pending {
            store.save(policy).await?;
        }
        self.pending = None;
        Ok(())
    }
}
impl PolicyStore {
    pub fn new(root: PathBuf) -> Result<Self, AccessError> {
        Ok(Self {
            root: std::path::absolute(root)?,
        })
    }
    fn path(&self, resource: &str) -> Result<PathBuf, AccessError> {
        validate_resource(resource)?;
        Ok(self
            .root
            .join(format!("{:x}.json", Sha256::digest(resource.as_bytes()))))
    }
    /// Reads are uncached so revocation is visible across store handles/processes.
    pub async fn get(&self, resource: &str) -> Result<Option<ResourcePolicy>, AccessError> {
        let file = match tokio::fs::File::open(self.path(resource)?).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        file.take(65537).read_to_end(&mut bytes).await?;
        if bytes.len() > 65536 {
            return Err(AccessError::Invalid("Policy exceeds 64 KiB".into()));
        }
        let policy: ResourcePolicy = serde_json::from_slice(&bytes)?;
        policy.validate(resource)?;
        Ok(Some(policy))
    }
    pub async fn authorize(
        &self,
        resource: &str,
        principal: Option<&Principal>,
        action: Action,
    ) -> Result<(), AccessError> {
        match self.get(resource).await? {
            Some(policy) if policy.permits(principal, action) => Ok(()),
            _ => Err(AccessError::Denied),
        }
    }

    /// The caller must hold its data creation lock and prove `new_resource`.
    /// Existing ownerless data must never pass true, including storage errors.
    pub async fn begin_write(
        &self,
        resource: &str,
        actor: &Principal,
        new_resource: bool,
    ) -> Result<MutationPermit, AccessError> {
        actor.validate()?;
        let path = self.path(resource)?;
        let guard = document_lock(path.clone()).await;
        let lease = writer_lease(&path.with_extension("lock")).await?;
        let pending = match self.get(resource).await? {
            Some(mut policy) if policy.permits(Some(actor), Action::Write) => {
                policy.write_generation =
                    policy.write_generation.checked_add(1).ok_or_else(|| {
                        AccessError::Invalid("Policy write generation exhausted".into())
                    })?;
                Some(policy)
            }
            Some(_) => return Err(AccessError::Denied),
            None if new_resource => Some(ResourcePolicy {
                schema_version: 1,
                resource: resource.into(),
                owner: actor.clone(),
                grants: vec![],
                public_read: false,
                revision: 0,
                creation_id: uuid::Uuid::new_v4().to_string(),
                write_generation: 1,
            }),
            None => return Err(AccessError::Denied),
        };
        Ok(MutationPermit {
            _guard: guard,
            _lease: lease,
            pending,
        })
    }
    /// Trusted creation/adoption boundary. Network callers must first prove the
    /// data resource is new; ownerless legacy data may only be adopted locally.
    pub async fn create(
        &self,
        resource: &str,
        owner: Principal,
    ) -> Result<ResourcePolicy, AccessError> {
        owner.validate()?;
        let path = self.path(resource)?;
        let _guard = document_lock(path.clone()).await;
        let _lease = writer_lease(&path.with_extension("lock")).await?;
        if self.get(resource).await?.is_some() {
            return Err(AccessError::AlreadyOwned);
        }
        let policy = ResourcePolicy {
            schema_version: 1,
            resource: resource.into(),
            owner,
            grants: vec![],
            public_read: false,
            revision: 0,
            creation_id: uuid::Uuid::new_v4().to_string(),
            write_generation: 0,
        };
        self.save(&policy).await?;
        Ok(policy)
    }
    pub async fn change(
        &self,
        resource: &str,
        actor: &Principal,
        change: PolicyChange,
    ) -> Result<ResourcePolicy, AccessError> {
        let path = self.path(resource)?;
        let _guard = document_lock(path.clone()).await;
        let _lease = writer_lease(&path.with_extension("lock")).await?;
        let mut policy = self.get(resource).await?.ok_or(AccessError::Denied)?;
        if !policy.permits(Some(actor), Action::Manage) {
            return Err(AccessError::Denied);
        }
        match change {
            PolicyChange::Grant(principal, role) => {
                principal.validate()?;
                if principal == policy.owner {
                    return Err(AccessError::Invalid(
                        "Use transfer to change the primary owner".into(),
                    ));
                }
                policy.grants.retain(|grant| grant.principal != principal);
                policy.grants.push(Grant { principal, role });
            }
            PolicyChange::Revoke(principal) => {
                if principal == policy.owner {
                    return Err(AccessError::Invalid(
                        "The primary owner must be transferred, not revoked".into(),
                    ));
                }
                policy.grants.retain(|grant| grant.principal != principal);
            }
            PolicyChange::PublicRead(public) => policy.public_read = public,
            PolicyChange::Transfer(principal) => {
                principal.validate()?;
                policy.grants.retain(|grant| {
                    grant.principal != principal && grant.principal != policy.owner
                });
                policy.owner = principal;
            }
        }
        policy.revision = policy
            .revision
            .checked_add(1)
            .ok_or_else(|| AccessError::Invalid("Policy revision exhausted".into()))?;
        self.save(&policy).await?;
        Ok(policy)
    }
    async fn save(&self, policy: &ResourcePolicy) -> Result<(), AccessError> {
        policy.validate(&policy.resource)?;
        let bytes = serde_json::to_vec_pretty(policy)?;
        if bytes.len() > 65536 {
            return Err(AccessError::Invalid("Policy exceeds 64 KiB".into()));
        }
        atomic_write(&self.path(&policy.resource)?, &bytes).await?;
        Ok(())
    }
}
fn validate_resource(resource: &str) -> Result<(), AccessError> {
    if resource.is_empty() || resource.len() > 4096 || resource.chars().any(char::is_control) {
        return Err(AccessError::Invalid("Invalid policy resource".into()));
    }
    Ok(())
}

/// Canonical key for file-backed application namespaces. Iroh uses its own
/// normalized absolute resource names within an independent policy directory.
pub fn named_resource(prefix: &str, path: &str) -> Result<String, AccessError> {
    if !matches!(prefix, "chat" | "wiki" | "local" | "v2")
        || path.is_empty()
        || path.len() > 4096
        || path
            .chars()
            .any(|c| c.is_control() || "\\:<>\"|?*".contains(c))
        || path
            .split('/')
            .any(|part| part.is_empty() || part.starts_with('.') || part.ends_with(['.', ' ']))
    {
        return Err(AccessError::Invalid(
            "Invalid resource path or namespace".into(),
        ));
    }
    #[cfg(windows)]
    let path = path.to_lowercase();
    Ok(format!("{prefix}/{path}"))
}
