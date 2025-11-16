//! AWS Signature V4 Implementation
//!
//! Implements AWS Signature Version 4 for S3 authentication

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

type HmacSha256 = Hmac<Sha256>;

/// AWS Signature V4 signer for S3 requests
pub struct AwsSignatureV4 {
    access_key_id: String,
    secret_access_key: String,
    region: String,
    service: String,
}

impl AwsSignatureV4 {
    /// Create a new AWS Signature V4 signer
    pub fn new(
        access_key_id: String,
        secret_access_key: String,
        region: String,
    ) -> Self {
        Self {
            access_key_id,
            secret_access_key,
            region,
            service: "s3".to_string(),
        }
    }

    /// Sign an HTTP request using AWS Signature V4
    pub fn sign_request(
        &self,
        method: &str,
        url: &str,
        headers: &mut HashMap<String, String>,
        payload: &[u8],
        timestamp: &str,
    ) -> String {
        println!("\n=== AWS Signature V4 Debug ===");

        // Parse URL to get host and path
        let parsed_url = url::Url::parse(url).expect("Invalid URL");

        // Get host with port (if not default port)
        let host = if let Some(port) = parsed_url.port() {
            format!("{}:{}", parsed_url.host_str().unwrap_or(""), port)
        } else {
            parsed_url.host_str().unwrap_or("").to_string()
        };

        let path = parsed_url.path();
        let query = parsed_url.query().unwrap_or("");

        println!("URL: {}", url);
        println!("Host: {}", host);
        println!("Path: {}", path);
        println!("Query: {}", query);
        println!("Method: {}", method);
        println!("Timestamp: {}", timestamp);

        // Extract date from timestamp (YYYYMMDD)
        let date = &timestamp[0..8];
        println!("Date: {}", date);
        println!("Region: {}", self.region);

        // Step 1: Create canonical request
        let canonical_headers = self.create_canonical_headers(headers, &host);
        let signed_headers = self.get_signed_headers(headers);
        let payload_hash = self.sha256_hex(payload);

        println!("\n--- Canonical Request Components ---");
        println!("Payload hash: {}", payload_hash);
        println!("Signed headers: {}", signed_headers);
        println!("Canonical headers:\n{}", canonical_headers);

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            path,
            query,
            canonical_headers,
            signed_headers,
            payload_hash
        );

        println!("--- Full Canonical Request ---");
        println!("{}", canonical_request);
        println!("--- End Canonical Request ---");

        // Step 2: Create string to sign
        let credential_scope = format!("{}/{}/{}/aws4_request", date, self.region, self.service);
        let canonical_request_hash = self.sha256_hex(canonical_request.as_bytes());

        println!("\n--- String to Sign Components ---");
        println!("Canonical request hash: {}", canonical_request_hash);
        println!("Credential scope: {}", credential_scope);

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            timestamp,
            credential_scope,
            canonical_request_hash
        );

        println!("--- String to Sign ---");
        println!("{}", string_to_sign);
        println!("--- End String to Sign ---");

        // Step 3: Calculate signature
        let signature = self.calculate_signature(&string_to_sign, date);
        println!("\n--- Signature ---");
        println!("Signature: {}", signature);

        // Step 4: Create authorization header
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key_id,
            credential_scope,
            signed_headers,
            signature
        );

        println!("\n--- Authorization Header ---");
        println!("{}", authorization);
        println!("=== End AWS Signature V4 Debug ===\n");

        authorization
    }

    /// Create canonical headers string
    fn create_canonical_headers(&self, headers: &HashMap<String, String>, host: &str) -> String {
        let mut canonical_headers_vec: Vec<(String, String)> = Vec::new();

        // Always include host
        canonical_headers_vec.push(("host".to_string(), host.to_string()));

        // Add all x-amz-* headers
        for (key, value) in headers.iter() {
            let key_lower = key.to_lowercase();
            if key_lower.starts_with("x-amz-") {
                canonical_headers_vec.push((key_lower, value.trim().to_string()));
            }
        }

        // Sort by header name
        canonical_headers_vec.sort_by(|a, b| a.0.cmp(&b.0));

        // Format as "key:value\n"
        let mut canonical = String::new();
        for (key, value) in canonical_headers_vec {
            canonical.push_str(&format!("{}:{}\n", key, value));
        }

        canonical
    }

    /// Get signed headers list
    fn get_signed_headers(&self, headers: &HashMap<String, String>) -> String {
        let mut signed = vec!["host".to_string()];

        for key in headers.keys() {
            let key_lower = key.to_lowercase();
            if key_lower.starts_with("x-amz-") {
                signed.push(key_lower);
            }
        }

        signed.sort();
        signed.join(";")
    }

    /// Calculate the signature
    fn calculate_signature(&self, string_to_sign: &str, date: &str) -> String {
        let k_date = self.hmac_sha256(format!("AWS4{}", self.secret_access_key).as_bytes(), date.as_bytes());
        let k_region = self.hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = self.hmac_sha256(&k_region, self.service.as_bytes());
        let k_signing = self.hmac_sha256(&k_service, b"aws4_request");

        let signature = self.hmac_sha256(&k_signing, string_to_sign.as_bytes());
        hex::encode(signature)
    }

    /// Compute HMAC-SHA256
    fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    /// Compute SHA256 hash and return as hex string
    fn sha256_hex(&self, data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }
}

/// Get current timestamp in AWS format (YYYYMMDDTHHMMSSZ)
pub fn get_aws_timestamp() -> String {
    let now = chrono::Utc::now();
    now.format("%Y%m%dT%H%M%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_creation() {
        let signer = AwsSignatureV4::new(
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            "us-east-1".to_string(),
        );

        let mut headers = HashMap::new();
        headers.insert("x-amz-date".to_string(), "20130524T000000Z".to_string());

        let auth = signer.sign_request(
            "GET",
            "https://examplebucket.s3.amazonaws.com/test.txt",
            &mut headers,
            b"",
            "20130524T000000Z",
        );

        assert!(auth.starts_with("AWS4-HMAC-SHA256"));
    }
}
