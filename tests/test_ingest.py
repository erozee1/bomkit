#!/usr/bin/env python3
"""
Test script for BOM snapshot ingestion.

Tests the ingestion system with bom-3.csv file.
"""

import os
import sys
from pathlib import Path
from uuid import UUID

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✅ Loaded environment variables from .env file")
    else:
        print(f"⚠️  No .env file found at {env_path}")
except ImportError:
    # python-dotenv not installed, try to load manually
    env_path = project_root / ".env"
    if env_path.exists():
        print(f"⚠️  python-dotenv not installed. Install it with: pip install python-dotenv")
        print(f"   Attempting to load .env manually...")
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
        print(f"✅ Loaded environment variables from .env file (manual)")

from bomkit import BomParser
from bomkit.adapters.csv_adapter import CsvAdapter
from bomkit.ingest import (
    ingest_bom_snapshot,
    normalize_row_from_dict,
    SupabaseClient
)


def test_ingest_bom3():
    """Test ingesting BOM-3.csv into Supabase."""
    
    # 1. Parse and normalize the BOM file
    print("📄 Parsing BOM-3.csv...")
    parser = BomParser(normalize=True)
    parser.register_adapter(CsvAdapter())
    
    bom_file = project_root / "tests" / "BOM-4.csv"
    if not bom_file.exists():
        print(f"❌ Error: {bom_file} not found")
        return
    
    try:
        normalized_dicts = parser.parse(str(bom_file))
        print(f"✅ Parsed {len(normalized_dicts)} rows")
        
        # Show first normalized row for debugging
        if normalized_dicts:
            print("\n📋 First normalized row:")
            for key, value in list(normalized_dicts[0].items())[:5]:
                print(f"  {key}: {value}")
            print("  ...")
        
    except Exception as e:
        print(f"❌ Error parsing file: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 2. Convert to NormalizedRow objects
    print("\n🔄 Converting to NormalizedRow objects...")
    normalized_rows = [
        normalize_row_from_dict(row_dict, row_index=i)
        for i, row_dict in enumerate(normalized_dicts)
    ]
    print(f"✅ Converted {len(normalized_rows)} rows")
    
    # Show first NormalizedRow for debugging
    if normalized_rows:
        first_row = normalized_rows[0]
        print(f"\n📦 First NormalizedRow:")
        print(f"  part_name: {first_row.part_name}")
        print(f"  quantity: {first_row.quantity}")
        print(f"  attributes: {first_row.attributes}")
        print(f"  context: {first_row.context}")
    
    # 3. Get connection details
    print("\n🔌 Connecting to Supabase...")
    
    # Try to get connection string from environment
    db_url = os.getenv("SUPABASE_DB_URL")
    
    # Debug: Show what we found (but don't print the full URL for security)
    if db_url:
        # Show first/last few chars to verify it's loaded
        masked_url = db_url[:20] + "..." + db_url[-10:] if len(db_url) > 30 else "***"
        print(f"   Found SUPABASE_DB_URL: {masked_url}")
    else:
        print("   SUPABASE_DB_URL not found in environment")
    
    if not db_url or not db_url.strip():
        print("⚠️  SUPABASE_DB_URL not set. Trying individual connection params...")
        # Try individual params
        host = os.getenv("SUPABASE_DB_HOST")
        database = os.getenv("SUPABASE_DB_NAME", "postgres")
        user = os.getenv("SUPABASE_DB_USER", "postgres")
        password = os.getenv("SUPABASE_DB_PASSWORD")
        
        if not all([host, password]):
            print("❌ Error: Missing database connection details.")
            print("\nPlease set one of:")
            print("  - SUPABASE_DB_URL (full connection string)")
            print("  - SUPABASE_DB_HOST, SUPABASE_DB_PASSWORD (and optionally SUPABASE_DB_NAME, SUPABASE_DB_USER)")
            print("\nTo get your connection string:")
            print("  Supabase Dashboard → Project Settings → Database → Connection string")
            print("  Use 'Session mode' for persistent connections")
            return
        
        db = SupabaseClient(
            host=host,
            database=database,
            user=user,
            password=password
        )
    else:
        db = SupabaseClient(db_url=db_url)
    
    print("✅ Connected to Supabase")
    
    # 4. Get org_id (you'll need to provide this or create one)
    org_id_str = os.getenv("ORG_ID")
    if not org_id_str:
        print("\n⚠️  ORG_ID not set. Using a test UUID...")
        print("   (In production, use a real organization UUID)")
        org_id = UUID("00000000-0000-0000-0000-000000000001")
    else:
        try:
            org_id = UUID(org_id_str)
        except ValueError:
            print(f"❌ Error: Invalid ORG_ID format: {org_id_str}")
            return
    
    # 5. Ingest the BOM snapshot
    print(f"\n📤 Ingesting BOM snapshot...")
    print(f"   Organization ID: {org_id}")
    print(f"   Assembly name: BOM-3 Test Assembly (creating new)")
    print(f"   Rows: {len(normalized_rows)}")
    
    try:
        # Example: Creating a new assembly
        # To update an existing assembly, use: assembly_id=existing_uuid
        snapshot_id = ingest_bom_snapshot(
            org_id=org_id,
            assembly_name="BOM-3 Test Assembly",  # Creating new assembly
            rows=normalized_rows,
            db=db,
            debug=True  # Enable debug logging to see what's happening
        )
        
        print(f"\n✅ Successfully ingested BOM snapshot!")
        print(f"   Snapshot ID: {snapshot_id}")
        print(f"\n🎉 Test completed successfully!")
        
        return snapshot_id
        
    except Exception as e:
        print(f"\n❌ Error during ingestion: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        # Clean up connection pool
        db.close()
        print("\n🔌 Database connection closed")


if __name__ == "__main__":
    print("=" * 60)
    print("BOM Snapshot Ingestion Test")
    print("=" * 60)
    print()
    
    test_ingest_bom3()

