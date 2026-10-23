//! Friend Request & Contacts Module
//!
//! Handles friend requests, contacts, and user relationships.
//! Stored in the same SQLite database as auth (users.sqlite).

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqlitePoolOptions;
use std::path::Path;
use tracing::info;
use uuid::Uuid;

/// Friend request status
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[sqlx(rename_all = "snake_case")]
pub enum RequestStatus {
    Pending,
    Accepted,
    Rejected,
}

/// Friend request record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FriendRequest {
    pub id: String,
    pub from_user_id: String,
    pub from_username: String,
    pub from_email: String,
    pub to_user_id: String,
    pub to_email: String,
    pub message: Option<String>,
    pub status: RequestStatus,
    pub created_at: DateTime<Utc>,
    pub responded_at: Option<DateTime<Utc>>,
}

/// Contact (established friend relationship)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contact {
    pub id: String,
    pub user_id: String,
    pub contact_user_id: String,
    pub username: String,
    pub email: String,
    pub avatar_url: Option<String>,
    pub is_online: bool,
    pub last_seen: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Friend manager handles all friend-related operations
pub struct FriendManager {
    db_path: std::path::PathBuf,
}

impl FriendManager {
    /// Create new friend manager
    pub async fn new(base_dir: &Path) -> Result<Self> {
        let db_path = base_dir.join("users.sqlite");

        let manager = Self { db_path };
        manager.init_db().await?;

        info!("[Friends] Initialized");
        Ok(manager)
    }

    /// Get database connection
    async fn get_pool(&self) -> Result<sqlx::SqlitePool> {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
        use std::str::FromStr;

        let options = SqliteConnectOptions::from_str(&format!(
            "sqlite://{}",
            self.db_path.to_string_lossy().replace('\\', "/")
        ))?
        .create_if_missing(true);
        Ok(SqlitePoolOptions::new().connect_with(options).await?)
    }

    /// Initialize database tables
    async fn init_db(&self) -> Result<()> {
        let pool = self.get_pool().await?;

        // Create friend_requests table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS friend_requests (
                id TEXT PRIMARY KEY,
                from_user_id TEXT NOT NULL,
                to_user_id TEXT NOT NULL,
                message TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                responded_at TEXT,
                FOREIGN KEY (from_user_id) REFERENCES users(id),
                FOREIGN KEY (to_user_id) REFERENCES users(id),
                UNIQUE(from_user_id, to_user_id)
            )
            "#,
        )
        .execute(&pool)
        .await?;

        // Create contacts table (established friendships)
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS contacts (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                contact_user_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (contact_user_id) REFERENCES users(id),
                UNIQUE(user_id, contact_user_id)
            )
            "#,
        )
        .execute(&pool)
        .await?;

        pool.close().await;
        Ok(())
    }

    /// Send a friend request
    pub async fn send_request(
        &self,
        from_user_id: String,
        to_email: String,
        message: Option<String>,
    ) -> Result<FriendRequest> {
        let pool = self.get_pool().await?;

        // Get recipient user info
        let recipient: Option<(String, String)> =
            sqlx::query_as("SELECT id, username FROM users WHERE email = ?")
                .bind(&to_email)
                .fetch_optional(&pool)
                .await?;

        let (to_user_id, _to_username) =
            recipient.ok_or_else(|| anyhow::anyhow!("User not found: {}", to_email))?;

        // Check if already friends
        let existing: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM contacts WHERE 
             (user_id = ? AND contact_user_id = ?) OR 
             (user_id = ? AND contact_user_id = ?)",
        )
        .bind(&from_user_id)
        .bind(&to_user_id)
        .bind(&to_user_id)
        .bind(&from_user_id)
        .fetch_optional(&pool)
        .await?;

        if existing.is_some() {
            return Err(anyhow::anyhow!("Already friends with this user"));
        }

        // Check for existing request
        let existing_req: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM friend_requests 
             WHERE from_user_id = ? AND to_user_id = ? AND status = 'pending'",
        )
        .bind(&from_user_id)
        .bind(&to_user_id)
        .fetch_optional(&pool)
        .await?;

        if existing_req.is_some() {
            return Err(anyhow::anyhow!("Friend request already pending"));
        }

        // Get sender info
        let sender: (String, String) =
            sqlx::query_as("SELECT username, email FROM users WHERE id = ?")
                .bind(&from_user_id)
                .fetch_one(&pool)
                .await?;

        let request = FriendRequest {
            id: Uuid::new_v4().to_string(),
            from_user_id: from_user_id.clone(),
            from_username: sender.0,
            from_email: sender.1,
            to_user_id: to_user_id.clone(),
            to_email: to_email.clone(),
            message,
            status: RequestStatus::Pending,
            created_at: Utc::now(),
            responded_at: None,
        };

        // Insert request
        sqlx::query(
            "INSERT INTO friend_requests (id, from_user_id, to_user_id, message, status, created_at) 
             VALUES (?, ?, ?, ?, ?, ?)"
        )
        .bind(&request.id)
        .bind(&from_user_id)
        .bind(&to_user_id)
        .bind(&request.message)
        .bind(&request.status)
        .bind(request.created_at.to_rfc3339())
        .execute(&pool)
        .await?;

        pool.close().await;

        info!(
            "[Friends] Request sent: {} -> {}",
            request.from_username, to_email
        );

        Ok(request)
    }

    /// Get pending friend requests for a user
    pub async fn get_pending_requests(&self, user_id: &str) -> Result<Vec<FriendRequest>> {
        let pool = self.get_pool().await?;

        let rows: Vec<(
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            String,
            String,
        )> = sqlx::query_as(
            r#"
            SELECT 
                fr.id, fr.from_user_id, u.username, u.email, fr.to_user_id,
                fr.message, fr.status, fr.created_at
            FROM friend_requests fr
            JOIN users u ON fr.from_user_id = u.id
            WHERE fr.to_user_id = ? AND fr.status = 'pending'
            ORDER BY fr.created_at DESC
            "#,
        )
        .bind(user_id)
        .fetch_all(&pool)
        .await?;

        pool.close().await;

        Ok(rows
            .into_iter()
            .map(
                |(id, from_id, from_username, from_email, to_id, message, status, created_at)| {
                    FriendRequest {
                        id,
                        from_user_id: from_id.clone(),
                        from_username,
                        from_email: from_email.clone(),
                        to_user_id: to_id,
                        to_email: from_email,
                        message,
                        status: match status.as_str() {
                            "accepted" => RequestStatus::Accepted,
                            "rejected" => RequestStatus::Rejected,
                            _ => RequestStatus::Pending,
                        },
                        created_at: created_at.parse().unwrap_or_else(|_| Utc::now()),
                        responded_at: None,
                    }
                },
            )
            .collect())
    }

    /// Respond to a friend request (accept/reject)
    pub async fn respond_to_request(&self, request_id: &str, accept: bool) -> Result<()> {
        let pool = self.get_pool().await?;

        // Get request details
        let req: (String, String, String) = sqlx::query_as(
            "SELECT from_user_id, to_user_id, status FROM friend_requests WHERE id = ?",
        )
        .bind(request_id)
        .fetch_one(&pool)
        .await?;

        let (from_id, to_id, status) = req;

        if status != "pending" {
            return Err(anyhow::anyhow!("Request already responded to"));
        }

        let new_status = if accept { "accepted" } else { "rejected" };
        let responded_at = Utc::now();

        // Update request status
        sqlx::query("UPDATE friend_requests SET status = ?, responded_at = ? WHERE id = ?")
            .bind(new_status)
            .bind(responded_at.to_rfc3339())
            .bind(request_id)
            .execute(&pool)
            .await?;

        // If accepted, create contact entries (bidirectional)
        if accept {
            let contact_id1 = Uuid::new_v4().to_string();
            let contact_id2 = Uuid::new_v4().to_string();
            let now = Utc::now();

            sqlx::query(
                "INSERT OR IGNORE INTO contacts (id, user_id, contact_user_id, created_at) VALUES (?, ?, ?, ?)"
            )
            .bind(&contact_id1)
            .bind(&from_id)
            .bind(&to_id)
            .bind(now.to_rfc3339())
            .execute(&pool)
            .await?;

            sqlx::query(
                "INSERT OR IGNORE INTO contacts (id, user_id, contact_user_id, created_at) VALUES (?, ?, ?, ?)"
            )
            .bind(&contact_id2)
            .bind(&to_id)
            .bind(&from_id)
            .bind(now.to_rfc3339())
            .execute(&pool)
            .await?;

            info!(
                "[Friends] Request {} accepted, contacts created",
                request_id
            );
        } else {
            info!("[Friends] Request {} rejected", request_id);
        }

        pool.close().await;
        Ok(())
    }

    /// Get user's contacts (friends)
    pub async fn get_contacts(&self, user_id: &str) -> Result<Vec<Contact>> {
        let pool = self.get_pool().await?;

        let rows: Vec<(String, String, String, String, Option<String>)> = sqlx::query_as(
            r#"
            SELECT 
                c.id, c.contact_user_id, u.username, u.email, u.avatar_url
            FROM contacts c
            JOIN users u ON c.contact_user_id = u.id
            WHERE c.user_id = ?
            ORDER BY u.username
            "#,
        )
        .bind(user_id)
        .fetch_all(&pool)
        .await?;

        pool.close().await;

        Ok(rows
            .into_iter()
            .map(|(id, contact_id, username, email, avatar)| Contact {
                id,
                user_id: user_id.to_string(),
                contact_user_id: contact_id,
                username,
                email,
                avatar_url: avatar,
                is_online: false,
                last_seen: None,
                created_at: Utc::now(),
            })
            .collect())
    }

    /// Remove a contact (unfriend)
    pub async fn remove_contact(&self, user_id: &str, contact_user_id: &str) -> Result<()> {
        let pool = self.get_pool().await?;

        // Remove both directions
        sqlx::query(
            "DELETE FROM contacts WHERE 
             (user_id = ? AND contact_user_id = ?) OR
             (user_id = ? AND contact_user_id = ?)",
        )
        .bind(user_id)
        .bind(contact_user_id)
        .bind(contact_user_id)
        .bind(user_id)
        .execute(&pool)
        .await?;

        pool.close().await;

        info!(
            "[Friends] Contact removed: {} <-> {}",
            user_id, contact_user_id
        );

        Ok(())
    }
}
