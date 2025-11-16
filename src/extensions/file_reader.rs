//! File Reading for HTTP/S3 Support
//!
//! Provides functionality to read files from HTTP/HTTPS and S3-compatible storage

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::extensions::aws_signature::{AwsSignatureV4, get_aws_timestamp};
use crate::extensions::secrets::S3Config;
use reqwest::blocking::Client;
use std::collections::HashMap;
use std::time::Duration;

/// File reader supporting HTTP and S3 protocols
pub struct FileReader {
    client: Client,
}

impl FileReader {
    /// Create a new file reader
    pub fn new() -> PrismDBResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(|e| PrismDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create HTTP client: {}", e)
            )))?;

        Ok(Self { client })
    }

    /// Read a file from a URL (http://, https://, or s3://)
    pub fn read_file(&self, url: &str, s3_config: Option<&S3Config>) -> PrismDBResult<Vec<u8>> {
        if url.starts_with("s3://") {
            self.read_s3_file(url, s3_config)
        } else if url.starts_with("http://") || url.starts_with("https://") {
            self.read_http_file(url)
        } else {
            Err(PrismDBError::InvalidArgument(format!(
                "Unsupported URL scheme. Expected http://, https://, or s3://, got: {}",
                url
            )))
        }
    }

    /// Read file via HTTP/HTTPS
    fn read_http_file(&self, url: &str) -> PrismDBResult<Vec<u8>> {
        println!("Reading file from HTTP: {}", url);

        let response = self.client
            .get(url)
            .send()
            .map_err(|e| PrismDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP request failed: {}", e)
            )))?;

        if !response.status().is_success() {
            return Err(PrismDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP request failed with status: {}", response.status())
            )));
        }

        let bytes = response.bytes()
            .map_err(|e| PrismDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to read response body: {}", e)
            )))?;

        println!("Successfully read {} bytes", bytes.len());
        Ok(bytes.to_vec())
    }

    /// Read file from S3-compatible storage (MinIO)
    fn read_s3_file(&self, s3_url: &str, s3_config: Option<&S3Config>) -> PrismDBResult<Vec<u8>> {
        let config = s3_config.ok_or_else(|| {
            PrismDBError::InvalidArgument(
                "S3 configuration required for s3:// URLs. Use SET or CREATE SECRET to configure.".to_string()
            )
        })?;

        // Parse s3://bucket/path/to/file
        let url_without_scheme = s3_url.strip_prefix("s3://")
            .ok_or_else(|| PrismDBError::InvalidArgument("Invalid S3 URL".to_string()))?;

        let parts: Vec<&str> = url_without_scheme.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(PrismDBError::InvalidArgument(
                "Invalid S3 URL format. Expected s3://bucket/path/to/file".to_string()
            ));
        }

        let bucket = parts[0];
        let path = parts[1];

        // Construct HTTP URL for S3-compatible endpoint
        let endpoint = config.endpoint.as_ref().ok_or_else(|| {
            PrismDBError::InvalidArgument("S3 endpoint not configured".to_string())
        })?;

        let protocol = if config.use_ssl { "https" } else { "http" };

        // Use path-style URL for MinIO
        let http_url = format!("{}://{}/{}/{}", protocol, endpoint, bucket, path);

        println!("Reading S3 file: {}", s3_url);
        println!("  Endpoint: {}", endpoint);
        println!("  Bucket: {}", bucket);
        println!("  Path: {}", path);
        println!("  HTTP URL: {}", http_url);

        // Get AWS credentials
        let access_key_id = config.access_key_id.as_ref().ok_or_else(|| {
            PrismDBError::InvalidArgument(
                "S3 access key ID not configured. Use SET s3_access_key_id or CREATE SECRET.".to_string()
            )
        })?;

        let secret_access_key = config.secret_access_key.as_ref().ok_or_else(|| {
            PrismDBError::InvalidArgument(
                "S3 secret access key not configured. Use SET s3_secret_access_key or CREATE SECRET.".to_string()
            )
        })?;

        println!("  Using AWS Signature V4 authentication");
        println!("  Access Key ID: {}", access_key_id);

        // Get current timestamp
        let timestamp = get_aws_timestamp();

        // Prepare headers for signing
        let mut headers_map = HashMap::new();
        headers_map.insert("x-amz-date".to_string(), timestamp.clone());

        // Create AWS Signature V4 signer
        let signer = AwsSignatureV4::new(
            access_key_id.clone(),
            secret_access_key.clone(),
            config.region.clone(),
        );

        // Sign the request
        let authorization = signer.sign_request(
            "GET",
            &http_url,
            &mut headers_map,
            b"", // Empty payload for GET request
            &timestamp,
        );

        // Build the HTTP request with signed headers
        let mut request = self.client.get(&http_url);
        request = request.header("Authorization", authorization);
        request = request.header("x-amz-date", timestamp);

        let response = request
            .send()
            .map_err(|e| PrismDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("S3 request failed: {}", e)
            )))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().unwrap_or_default();
            return Err(PrismDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("S3 request failed with status: {} - {}", status, error_text)
            )));
        }

        let bytes = response.bytes()
            .map_err(|e| PrismDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to read S3 response body: {}", e)
            )))?;

        println!("Successfully read {} bytes from S3", bytes.len());
        Ok(bytes.to_vec())
    }
}

impl Default for FileReader {
    fn default() -> Self {
        Self::new().expect("Failed to create file reader")
    }
}
