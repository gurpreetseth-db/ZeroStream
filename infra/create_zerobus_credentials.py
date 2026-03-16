#!/usr/bin/env python3
"""
Create ZeroBus Service Principal with OAuth credentials.

Reads from environment variables:
- ZEROBUS_SP_NAME: Name for the service principal
- ZEROBUS_TOPIC: ZeroBus topic name

Creates:
- A service principal for ZeroBus authentication
- OAuth client credentials (client_id and client_secret)

Uses Databricks CLI for OAuth secret creation (better permission handling).
"""
import os
import sys
import time
import re
import json
import subprocess
import requests


def get_auth():
    """Get Databricks host and token."""
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    
    return host, token


def run_cli(cmd: list[str], timeout: int = 60) -> tuple[bool, str]:
    """Run a Databricks CLI command and return (success, output)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output.strip()
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


def find_service_principal(host: str, token: str, display_name: str) -> dict | None:
    """Find a service principal by display name."""
    url = f"{host}/api/2.0/preview/scim/v2/ServicePrincipals"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"filter": f'displayName eq "{display_name}"'}
    
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        data = resp.json()
        resources = data.get("Resources", [])
        for sp in resources:
            if sp.get("displayName") == display_name:
                return sp
    return None


def create_service_principal(host: str, token: str, name: str) -> dict:
    """Create a service principal for ZeroBus."""
    
    # Check if already exists
    existing = find_service_principal(host, token, name)
    if existing:
        app_id = existing.get("applicationId")
        sp_id = existing.get("id")
        print(f"  ℹ️  Service Principal '{name}' already exists")
        print(f"     Application ID: {app_id}")
        print(f"     ID: {sp_id}")
        return {
            "name": name,
            "application_id": app_id,
            "id": sp_id,
            "status": "exists",
        }
    
    print(f"  🔐 Creating Service Principal: {name}")
    
    try:
        url = f"{host}/api/2.0/preview/scim/v2/ServicePrincipals"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "displayName": name,
            "active": True,
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
        }
        
        resp = requests.post(url, headers=headers, json=payload)
        
        if resp.status_code not in (200, 201):
            raise Exception(f"API error {resp.status_code}: {resp.text}")
        
        sp = resp.json()
        app_id = sp.get("applicationId")
        sp_id = sp.get("id")
        
        print(f"  ✅ Service Principal created")
        print(f"     Application ID: {app_id}")
        print(f"     ID: {sp_id}")
        
        return {
            "name": name,
            "application_id": app_id,
            "id": sp_id,
            "status": "created",
        }
        
    except Exception as e:
        print(f"  ⚠️  Could not create service principal: {e}")
        return {
            "name": name,
            "application_id": None,
            "id": None,
            "status": f"error: {e}",
        }


def _cleanup_old_secrets(host: str, token: str, sp_id: str, application_id: str):
    """Delete the oldest OAuth secret to make room for a new one."""
    try:
        url = f"{host}/api/2.0/accounts/servicePrincipals/{sp_id}/credentials/secrets"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            return
        secrets = resp.json().get("secrets", [])
        if len(secrets) < 5:
            return
        # Sort by create_time ascending, delete the oldest
        secrets.sort(key=lambda s: s.get("create_time", 0))
        oldest = secrets[0]
        secret_id = oldest.get("id")
        if secret_id:
            print(f"  🗑️  Deleting oldest OAuth secret {secret_id} to free quota...")
            del_url = f"{url}/{secret_id}"
            requests.delete(del_url, headers=headers)
    except Exception as e:
        print(f"  ⚠️  Could not clean up old secrets: {e}")


def create_oauth_secret(host: str, token: str, application_id: str, id: str, sp_name: str) -> dict:
    """Create OAuth client secret for the service principal.
    
    Uses Databricks CLI for better permission handling.
    
    Args:
        host: Databricks workspace host
        token: Databricks API token
        application_id: The service principal's application ID (UUID format)
        id: The service principal's numeric ID
        sp_name: Service principal display name (for error messages)
    """
    
    print(f"  🔑 Creating OAuth secret for {application_id}...")
    
    # First, try to clean up old secrets if quota is full
    _cleanup_old_secrets(host, token, id, application_id)
    
    # Use the correct CLI command: service-principal-secrets-proxy
    cmd = [
        "databricks", "service-principal-secrets-proxy", "create",
        id,
        "--output", "json",
    ]
    
    success, output = run_cli(cmd)
    
    if success:
        try:
            secret_data = json.loads(output)
            secret_value = secret_data.get("secret")
            secret_id = secret_data.get("id")
            
            print(f"  ✅ OAuth secret created via CLI")
            print(f"     Secret ID: {secret_id}")
            
            return {
                "secret_id": secret_id,
                "secret": secret_value,
                "status": "created",
            }
        except json.JSONDecodeError:
            # CLI succeeded but output wasn't JSON, try to parse
            if "secret" in output.lower():
                print(f"  ✅ OAuth secret created (CLI output: {output[:100]}...)")
                return {"secret_id": None, "secret": None, "status": "created_no_value"}
    
    # CLI failed, try REST API as fallback
    print(f"  ⚠️  CLI method failed: {output[:200]}")
    print(f"     Trying REST API fallback...")
    
    try:
        # Use the workspace-level service principal secrets API (numeric ID, not application_id)
        url = f"{host}/api/2.0/accounts/servicePrincipals/{id}/credentials/secrets"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        
        resp = requests.post(url, headers=headers, json={})
        
        if resp.status_code in (200, 201):
            secret_data = resp.json()
            secret_value = secret_data.get("secret")
            secret_id = secret_data.get("id")
            
            print(f"  ✅ OAuth secret created via REST API")
            print(f"     Secret ID: {secret_id}")
            
            return {
                "secret_id": secret_id,
                "secret": secret_value,
                "status": "created",
            }
        else:
            raise Exception(f"API error {resp.status_code}: {resp.text[:200]}")
        
    except Exception as e:
        print(f"  ⚠️  Could not create OAuth secret: {e}")
        print(f"")
        print(f"     ╔══════════════════════════════════════════════════════════════╗")
        print(f"     ║  MANUAL STEP REQUIRED                                        ║")
        print(f"     ╠══════════════════════════════════════════════════════════════╣")
        print(f"     ║  Create the OAuth secret manually in Databricks UI:          ║")
        print(f"     ║  1. Go to Settings → Identity & access → Service principals  ║")
        print(f"     ║  2. Find '{sp_name}'                                         ║")
        print(f"     ║  3. Click 'Generate secret'                                  ║")
        print(f"     ║  4. Copy the secret and add to generated_config.env:         ║")
        print(f"     ║     ZEROBUS_CLIENT_SECRET=<your_secret>                      ║")
        print(f"     ╚══════════════════════════════════════════════════════════════╝")
        return {
            "secret_id": None,
            "secret": None,
            "status": f"error: {e}",
        }


def save_config(sp_info: dict, secret_info: dict, topic: str):
    """Save ZeroBus credentials to generated config file in main directory."""
    script_dir = os.path.dirname(__file__)
    root_dir = os.path.dirname(script_dir)
    config_file = os.path.join(root_dir, "generated_config.env")
    
    # Collect values to append
    lines = ["\n# ── ZeroBus Credentials ─────────────────────────────────────────"]
    
    lines.append(f"ZEROBUS_TOPIC={topic}")
    
    if sp_info.get("application_id"):
        lines.append(f"ZEROBUS_CLIENT_ID={sp_info['application_id']}")
        lines.append(f"ZEROBUS_SP_ID={sp_info.get('id', '')}")
    
    if secret_info.get("secret"):
        lines.append(f"ZEROBUS_CLIENT_SECRET={secret_info['secret']}")
        print(f"\n  ⚠️  IMPORTANT: Save this secret - it won't be shown again!")
        print(f"     ZEROBUS_CLIENT_SECRET={secret_info['secret']}")
    
    # Append to file
    with open(config_file, "a") as f:
        f.write("\n".join(lines))
        f.write("\n")
    
    print(f"\n  💾 ZeroBus credentials saved to generated_config.env")
    print(f"\n  📝 You also need to set ZEROBUS_SERVER_ENDPOINT in your .env:")
    print(f"     Format: <workspace_id>.zerobus.<region>.cloud.databricks.com")


def main():
    try:
        host, token = get_auth()
        
        sp_name = os.environ.get("ZEROBUS_SP_NAME", "zerostream-zerobus-service")
        topic = os.environ.get("ZEROBUS_TOPIC", "sensor_stream_topic")
        
        print(f"\n  ZeroBus Configuration:")
        print(f"     Service Principal: {sp_name}")
        print(f"     Topic: {topic}")
        
        # Create service principal
        sp_info = create_service_principal(host, token, sp_name)
        
        # Create OAuth secret - always create a new one to capture the value
        # (OAuth secrets can only be retrieved at creation time)
        secret_info = {"secret": None, "status": "skipped"}
        if sp_info.get("application_id"):
            if sp_info.get("status") == "exists":
                print(f"\n  ℹ️  Service principal exists - creating new OAuth secret")
                print(f"     (Previous secrets will still work until deleted)")
            secret_info = create_oauth_secret(host, token, sp_info["application_id"], sp_info["id"],sp_name)
        
        # Save config
        save_config(sp_info, secret_info, topic)
        
    except Exception as e:
        print(f"  ❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
