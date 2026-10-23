//! Authentication Module
//!
//! Handles user signup, login, and session management.
//! All user data stored in SQLite database at braid_sync/users.sqlite

pub mod handlers;
pub mod middleware;

use crate::core::database as sqlx;
use anyhow::{Context, Result};
use bcrypt::{hash, verify, DEFAULT_COST};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

/// User record stored in database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub email: String,
    pub username: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub avatar_blob_hash: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_login: Option<DateTime<Utc>>,
    pub is_active: bool,
}

/// Public user info (no sensitive data)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub id: String,
    pub email: String,
    pub username: String,
    pub avatar_blob_hash: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<User> for UserInfo {
    fn from(user: User) -> Self {
        Self {
            id: user.id,
            email: user.email,
            username: user.username,
            avatar_blob_hash: user.avatar_blob_hash,
            created_at: user.created_at,
        }
    }
}

/// Session token for authenticated requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub token: String,
    pub user_id: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Auth manager handles all authentication
pub struct AuthManager {
    db_path: std::path::PathBuf,
    pool: sqlx::SqlitePool,
}

impl AuthManager {
    /// Create new auth manager
    pub async fn new(base_dir: &std::path::Path) -> Result<Self> {
        let db_path = base_dir.join("users.sqlite");

        let pool = sqlx::open(&db_path).await?;
        let manager = Self { db_path, pool };

        // Initialize database
        manager.init_db().await?;

        info!("[Auth] Initialized at {:?}", manager.db_path);

        Ok(manager)
    }

    /// Initialize SQLite database
    async fn init_db(&self) -> Result<()> {
        let pool = self.pool.clone();

        // Create users table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                username TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                avatar_blob_hash TEXT,
                created_at TEXT NOT NULL,
                last_login TEXT,
                is_active INTEGER DEFAULT 1
            )
            "#,
        )
        .execute(&pool)
        .await?;

        // Migration: Add avatar_blob_hash if it doesn't exist
        let _ = sqlx::query("ALTER TABLE users ADD COLUMN avatar_blob_hash TEXT")
            .execute(&pool)
            .await;

        // Create sessions table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            "#,
        )
        .execute(&pool)
        .await?;

        Ok(())
    }

    /// Get database connection
    async fn get_pool(&self) -> Result<sqlx::SqlitePool> {
        Ok(self.pool.clone())
    }

    pub(crate) fn pool(&self) -> sqlx::SqlitePool {
        self.pool.clone()
    }

    /// Register a new user
    pub async fn signup(
        &self,
        email: String,
        username: String,
        password: String,
        avatar_blob_hash: Option<String>,
    ) -> Result<User> {
        let pool = self.get_pool().await?;

        // Check if email already exists
        let existing: Option<(String,)> = sqlx::query_as("SELECT id FROM users WHERE email = ?")
            .bind(&email)
            .fetch_optional(&pool)
            .await?;

        if existing.is_some() {
            return Err(anyhow::anyhow!("Email already registered"));
        }

        // Hash password
        let password_hash = password_hash(password).await?;

        // Create user
        let user = User {
            id: Uuid::new_v4().to_string(),
            email: email.clone(),
            username: username.clone(),
            password_hash,
            avatar_blob_hash: avatar_blob_hash.clone(),
            created_at: Utc::now(),
            last_login: None,
            is_active: true,
        };

        // Insert into database
        sqlx::query(
            "INSERT INTO users (id, email, username, password_hash, avatar_blob_hash, created_at, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&user.id)
        .bind(&user.email)
        .bind(&user.username)
        .bind(&user.password_hash)
        .bind(&user.avatar_blob_hash)
        .bind(user.created_at.to_rfc3339())
        .bind(user.is_active)
        .execute(&pool)
        .await?;

        info!("[Auth] User registered: {} ({})", username, email);

        Ok(user)
    }

    /// Login user and create session
    pub async fn login(&self, email: String, password: String) -> Result<(User, Session)> {
        let pool = self.get_pool().await?;

        // Find user by email
        let row: Option<(String, String, String, String, Option<String>, String)> = sqlx::query_as(
            "SELECT id, email, username, password_hash, avatar_blob_hash, created_at FROM users WHERE email = ? AND is_active = 1"
        )
        .bind(&email)
        .fetch_optional(&pool)
        .await?;

        let (user_id, email, username, password_hash, avatar_blob_hash, created_at) =
            row.ok_or_else(|| anyhow::anyhow!("Invalid email or password"))?;

        // Verify password
        let stored_hash = password_hash.clone();
        let valid = password_work(move || verify(password, &stored_hash)).await?;

        if !valid {
            warn!("[Auth] Failed login attempt for {}", email);
            return Err(anyhow::anyhow!("Invalid email or password"));
        }

        // Update last login
        sqlx::query("UPDATE users SET last_login = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(&user_id)
            .execute(&pool)
            .await?;

        // Create session
        let session = self.create_session(&pool, &user_id).await?;

        let user = User {
            id: user_id,
            email,
            username,
            password_hash: String::new(), // Don't return hash
            avatar_blob_hash,
            created_at: created_at
                .parse()
                .context("Invalid stored user creation timestamp")?,
            last_login: Some(Utc::now()),
            is_active: true,
        };

        info!("[Auth] User logged in: {}", user.username);

        Ok((user, session))
    }

    /// Create new session
    async fn create_session(&self, pool: &sqlx::SqlitePool, user_id: &str) -> Result<Session> {
        let session = Session {
            token: Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::days(30),
        };

        sqlx::query(
            "INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
        )
        .bind(&session.token)
        .bind(&session.user_id)
        .bind(session.created_at.to_rfc3339())
        .bind(session.expires_at.to_rfc3339())
        .execute(pool)
        .await?;

        Ok(session)
    }

    /// Validate session token
    pub async fn validate_session(&self, token: &str) -> Result<UserInfo> {
        // Check database
        let pool = self.get_pool().await?;

        let row: Option<(String, String, String, Option<String>, String, String)> = sqlx::query_as(
            r#"
            SELECT u.id, u.email, u.username, u.avatar_blob_hash, u.created_at, s.expires_at 
            FROM users u 
            JOIN sessions s ON u.id = s.user_id 
            WHERE s.token = ? AND u.is_active = 1
            "#,
        )
        .bind(token)
        .fetch_optional(&pool)
        .await?;

        if let Some((id, email, username, avatar_blob_hash, created_at, expires_at)) = row {
            let expires: DateTime<Utc> = expires_at
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid date"))?;
            if expires > Utc::now() {
                return Ok(UserInfo {
                    id,
                    email,
                    username,
                    avatar_blob_hash,
                    created_at: created_at
                        .parse()
                        .context("Invalid stored user creation timestamp")?,
                });
            }
        }

        Err(anyhow::anyhow!("Invalid or expired session"))
    }

    /// Logout user (invalidate session)
    pub async fn logout(&self, token: &str) -> Result<()> {
        // Remove from database
        let pool = self.get_pool().await?;
        sqlx::query("DELETE FROM sessions WHERE token = ?")
            .bind(token)
            .execute(&pool)
            .await?;

        info!("[Auth] Session invalidated");

        Ok(())
    }

    /// Get user by ID
    pub async fn get_user(&self, user_id: &str) -> Result<UserInfo> {
        let pool = self.get_pool().await?;

        let row: Option<(String, String, String, Option<String>, String)> = sqlx::query_as(
            "SELECT id, email, username, avatar_blob_hash, created_at FROM users WHERE id = ?",
        )
        .bind(user_id)
        .fetch_optional(&pool)
        .await?;

        if let Some((id, email, username, avatar_blob_hash, created_at)) = row {
            Ok(UserInfo {
                id,
                email,
                username,
                avatar_blob_hash,
                created_at: created_at
                    .parse()
                    .context("Invalid stored user creation timestamp")?,
            })
        } else {
            Err(anyhow::anyhow!("User not found"))
        }
    }

    /// List all users (for contact discovery)
    pub async fn list_users(&self) -> Result<Vec<UserInfo>> {
        let pool = self.get_pool().await?;

        let rows: Vec<(String, String, String, Option<String>, String)> = sqlx::query_as(
            "SELECT id, email, username, avatar_blob_hash, created_at FROM users WHERE is_active = 1"
        )
        .fetch_all(&pool)
        .await?;

        rows.into_iter()
            .map(|(id, email, username, avatar_blob_hash, created_at)| {
                Ok(UserInfo {
                    id,
                    email,
                    username,
                    avatar_blob_hash,
                    created_at: created_at
                        .parse()
                        .context("Invalid stored user creation timestamp")?,
                })
            })
            .collect()
    }

    /// Update user profile
    pub async fn update_user(
        &self,
        user_id: &str,
        username: Option<String>,
        email: Option<String>,
        password: Option<String>,
        avatar_blob_hash: Option<String>,
    ) -> Result<UserInfo> {
        let pool = self.get_pool().await?;

        if let Some(username) = username {
            sqlx::query("UPDATE users SET username = ? WHERE id = ?")
                .bind(username)
                .bind(user_id)
                .execute(&pool)
                .await?;
        }

        if let Some(email) = email {
            // Check if email already exists for another user
            let existing: Option<(String,)> =
                sqlx::query_as("SELECT id FROM users WHERE email = ? AND id != ?")
                    .bind(&email)
                    .bind(user_id)
                    .fetch_optional(&pool)
                    .await?;

            if existing.is_some() {
                return Err(anyhow::anyhow!("Email already in use"));
            }

            sqlx::query("UPDATE users SET email = ? WHERE id = ?")
                .bind(email)
                .bind(user_id)
                .execute(&pool)
                .await?;
        }

        if let Some(password) = password {
            let password_hash = password_hash(password).await?;

            sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
                .bind(password_hash)
                .bind(user_id)
                .execute(&pool)
                .await?;
        }

        if let Some(avatar) = avatar_blob_hash {
            sqlx::query("UPDATE users SET avatar_blob_hash = ? WHERE id = ?")
                .bind(avatar)
                .bind(user_id)
                .execute(&pool)
                .await?;
        }

        let user = self.get_user(user_id).await?;
        Ok(user)
    }

    /// Set user avatar
    pub async fn set_avatar(&self, user_id: &str, avatar_hash: String) -> Result<()> {
        let pool = self.get_pool().await?;
        sqlx::query("UPDATE users SET avatar_blob_hash = ? WHERE id = ?")
            .bind(avatar_hash)
            .bind(user_id)
            .execute(&pool)
            .await?;
        Ok(())
    }
}

async fn password_work<T: Send + 'static>(
    work: impl FnOnce() -> bcrypt::BcryptResult<T> + Send + 'static,
) -> Result<T> {
    static LIMIT: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
        std::sync::OnceLock::new();
    let permit = LIMIT
        .get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(4)))
        .clone()
        .acquire_owned()
        .await?;
    Ok(tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await??)
}
async fn password_hash(password: String) -> Result<String> {
    password_work(move || hash(password, DEFAULT_COST)).await
}
