"""
Supabase Postgres database client implementation.

This module provides a concrete implementation of DatabaseClient that connects
to a Supabase Postgres database instance using direct database connections.

IMPORTANT: This uses direct Postgres connections, NOT Supabase API keys.
- Direct Postgres connection is the RECOMMENDED approach for server-side Python applications
- API keys (publishable/secret) are for Supabase's REST/GraphQL API, not direct DB connections
- For server-side operations, direct connection provides better performance and full SQL access

Connection Options:
1. Direct Connection (default): Connects directly to Postgres (requires IPv6 support)
2. Supavisor Session Mode: Use connection pooler for persistent clients (IPv4 compatible)
3. Supavisor Transaction Mode: For serverless/edge functions (many transient connections)

To get your connection string:
- Go to Supabase Dashboard → Project Settings → Database
- Copy the connection string (use "Session mode" or "Transaction mode" if needed)
- Format: postgresql://postgres:[password]@[host]:5432/postgres
"""

import logging
import os
from typing import Optional, Dict, Any, List, Tuple
from uuid import UUID, uuid4
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2.pool import SimpleConnectionPool
from difflib import SequenceMatcher

from .snapshot_ingest import DatabaseClient

logger = logging.getLogger(__name__)


def _string_similarity(a: str, b: str) -> float:
    """
    Compute string similarity using SequenceMatcher.
    
    Returns a value between 0.0 (no similarity) and 1.0 (identical).
    """
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _jsonb_similarity(
    attrs1: Dict[str, Any],
    attrs2: Dict[str, Any]
) -> float:
    """
    Compute similarity between two JSONB attribute dictionaries.
    
    Compares overlapping keys and their values. Returns a score between 0.0 and 1.0.
    """
    if not attrs1 and not attrs2:
        return 1.0
    if not attrs1 or not attrs2:
        return 0.0
    
    # Get all unique keys
    all_keys = set(attrs1.keys()) | set(attrs2.keys())
    if not all_keys:
        return 1.0
    
    matches = 0
    total = 0
    
    for key in all_keys:
        val1 = attrs1.get(key)
        val2 = attrs2.get(key)
        
        if val1 is None and val2 is None:
            continue  # Both missing, skip
        
        total += 1
        
        if val1 is None or val2 is None:
            continue  # One missing, no match
        
        # Compare values
        if isinstance(val1, str) and isinstance(val2, str):
            # String similarity
            if val1.lower() == val2.lower():
                matches += 1
            else:
                # Partial credit for similar strings
                matches += _string_similarity(val1, val2) * 0.8
        elif val1 == val2:
            matches += 1
    
    return matches / total if total > 0 else 0.0


class SupabaseClient(DatabaseClient):
    """
    Supabase Postgres database client implementation.
    
    Connects to Supabase Postgres using connection pooling and implements
    all database operations required for snapshot-based BOM ingestion.
    
    Connection can be configured via:
    - Environment variables (SUPABASE_DB_URL or individual connection params)
    - Constructor parameters
    """
    
    def __init__(
        self,
        db_url: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        minconn: int = 1,
        maxconn: int = 10
    ):
        """
        Initialize Supabase database client.
        
        Uses direct Postgres connection (recommended for server-side Python apps).
        This is NOT using Supabase API keys - those are for REST/GraphQL API only.
        
        To get your connection string:
        - Supabase Dashboard → Project Settings → Database → Connection string
        - Use "Session mode" for persistent connections (recommended)
        - Use "Transaction mode" for serverless/edge functions
        
        Args:
            db_url: Full database URL (e.g., postgresql://postgres:pass@host:5432/postgres)
                   If not provided, checks SUPABASE_DB_URL env var, then builds from
                   individual params or env vars.
            host: Database host (defaults to SUPABASE_DB_HOST env var)
            port: Database port (defaults to SUPABASE_DB_PORT or 5432)
            database: Database name (defaults to SUPABASE_DB_NAME env var)
            user: Database user (defaults to SUPABASE_DB_USER env var)
            password: Database password (defaults to SUPABASE_DB_PASSWORD env var)
            minconn: Minimum connections in pool
            maxconn: Maximum connections in pool
        """
        # Parse connection parameters
        # Priority: 1) db_url param, 2) SUPABASE_DB_URL env var, 3) individual params/env vars
        if db_url:
            self.db_url = db_url
        elif os.getenv("SUPABASE_DB_URL"):
            self.db_url = os.getenv("SUPABASE_DB_URL")
        else:
            # Build connection string from individual params or env vars
            self.db_url = self._build_connection_string(
                host=host or os.getenv("SUPABASE_DB_HOST"),
                port=port or int(os.getenv("SUPABASE_DB_PORT", "5432")),
                database=database or os.getenv("SUPABASE_DB_NAME"),
                user=user or os.getenv("SUPABASE_DB_USER"),
                password=password or os.getenv("SUPABASE_DB_PASSWORD")
            )
        
        self.connection_pool = None  # Will be created on first use
        
        self.minconn = minconn
        self.maxconn = maxconn
        self._pool = None
        self._transaction_conn = None
    
    def _build_connection_string(
        self,
        host: Optional[str],
        port: Optional[int],
        database: Optional[str],
        user: Optional[str],
        password: Optional[str]
    ) -> str:
        """Build PostgreSQL connection string from components."""
        if not all([host, database, user, password]):
            raise ValueError(
                "Missing required connection parameters. Provide db_url or set "
                "SUPABASE_DB_HOST, SUPABASE_DB_NAME, SUPABASE_DB_USER, "
                "SUPABASE_DB_PASSWORD environment variables."
            )
        
        port = port or 5432
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    def _get_connection_pool(self) -> SimpleConnectionPool:
        """Get or create connection pool."""
        if self._pool is None:
            self._pool = SimpleConnectionPool(
                self.minconn,
                self.maxconn,
                dsn=self.db_url
            )
            if not self._pool:
                raise RuntimeError("Failed to create database connection pool")
        return self._pool
    
    def _get_connection(self):
        """Get a connection from the pool."""
        pool = self._get_connection_pool()
        return pool.getconn()
    
    def _return_connection(self, conn):
        """Return a connection to the pool."""
        pool = self._get_connection_pool()
        pool.putconn(conn)
    
    def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if self._transaction_conn is not None:
            raise RuntimeError("Transaction already in progress")
        
        self._transaction_conn = self._get_connection()
        self._transaction_conn.autocommit = False
    
    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if self._transaction_conn is None:
            raise RuntimeError("No transaction in progress")
        
        try:
            self._transaction_conn.commit()
        finally:
            self._return_connection(self._transaction_conn)
            self._transaction_conn = None
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if self._transaction_conn is None:
            raise RuntimeError("No transaction in progress")
        
        try:
            self._transaction_conn.rollback()
        finally:
            self._return_connection(self._transaction_conn)
            self._transaction_conn = None
    
    def _get_cursor(self, dict_cursor: bool = True):
        """Get a cursor from the current transaction connection."""
        if self._transaction_conn is None:
            raise RuntimeError("No transaction in progress. Call begin_transaction() first.")
        
        if dict_cursor:
            return self._transaction_conn.cursor(cursor_factory=RealDictCursor)
        return self._transaction_conn.cursor()
    
    def get_assembly_by_id(
        self,
        org_id: UUID,
        assembly_id: UUID
    ) -> UUID:
        """
        Get an existing assembly by ID, verifying it belongs to the organization.
        
        Raises ValueError if assembly doesn't exist or doesn't belong to org.
        """
        cursor = self._get_cursor()
        
        try:
            cursor.execute("""
                SELECT id FROM assemblies
                WHERE id = %s AND org_id = %s
                LIMIT 1
            """, (str(assembly_id), str(org_id)))
            
            result = cursor.fetchone()
            if not result:
                raise ValueError(
                    f"Assembly {assembly_id} not found or does not belong to "
                    f"organization {org_id}. Use assembly_name to create a new assembly."
                )
            
            return UUID(result['id'])
            
        finally:
            cursor.close()
    
    def get_or_create_organization(
        self,
        org_id: UUID,
        org_name: Optional[str] = None
    ) -> UUID:
        """
        Resolve or create an organization.
        
        If org_id exists, returns it. If not, creates a new organization.
        """
        cursor = self._get_cursor()
        
        try:
            # Check if organization exists
            cursor.execute("""
                SELECT id FROM organizations
                WHERE id = %s
                LIMIT 1
            """, (str(org_id),))
            
            result = cursor.fetchone()
            if result:
                return UUID(result['id'])
            
            # Create new organization
            name = org_name or f"Organization {str(org_id)[:8]}"
            cursor.execute("""
                INSERT INTO organizations (id, name, created_at)
                VALUES (%s, %s, NOW())
            """, (str(org_id), name))
            
            return org_id
            
        finally:
            cursor.close()
    
    def get_or_create_assembly(
        self,
        org_id: UUID,
        assembly_name: str
    ) -> UUID:
        """
        Resolve or create an assembly.
        
        Reuses assembly if name matches within org, creates if missing.
        """
        cursor = self._get_cursor()
        
        try:
            # Try to find existing assembly
            cursor.execute("""
                SELECT id FROM assemblies
                WHERE org_id = %s AND name = %s
                LIMIT 1
            """, (str(org_id), assembly_name))
            
            result = cursor.fetchone()
            if result:
                return UUID(result['id'])
            
            # Create new assembly
            assembly_id = uuid4()
            cursor.execute("""
                INSERT INTO assemblies (id, org_id, name, created_at)
                VALUES (%s, %s, %s, NOW())
            """, (str(assembly_id), str(org_id), assembly_name))
            
            return assembly_id
            
        finally:
            cursor.close()
    
    def find_similar_parts(
        self,
        org_id: UUID,
        part_name: str,
        attributes: Dict[str, Any],
        similarity_threshold: float = 0.8
    ) -> List[Tuple[UUID, float]]:
        """
        Find existing parts that might match this one.
        
        Uses name similarity and attribute overlap to find matches.
        """
        cursor = self._get_cursor()
        
        try:
            # Get all parts in the organization
            cursor.execute("""
                SELECT id, name, attributes
                FROM parts
                WHERE org_id = %s
            """, (str(org_id),))
            
            candidates = cursor.fetchall()
            matches = []
            
            for candidate in candidates:
                # Compute name similarity
                name_sim = _string_similarity(part_name, candidate['name'])
                
                # Compute attribute similarity
                candidate_attrs = candidate['attributes'] or {}
                attr_sim = _jsonb_similarity(attributes, candidate_attrs)
                
                # Combined score (weighted: 60% name, 40% attributes)
                combined_score = (name_sim * 0.6) + (attr_sim * 0.4)
                
                if combined_score >= similarity_threshold:
                    matches.append((UUID(candidate['id']), combined_score))
            
            # Sort by confidence descending
            matches.sort(key=lambda x: x[1], reverse=True)
            
            return matches
            
        finally:
            cursor.close()
    
    def create_part(
        self,
        org_id: UUID,
        part_name: str,
        attributes: Dict[str, Any]
    ) -> UUID:
        """Create a new part."""
        cursor = self._get_cursor()
        
        try:
            part_id = uuid4()
            cursor.execute("""
                INSERT INTO parts (id, org_id, name, attributes, created_at)
                VALUES (%s, %s, %s, %s::jsonb, NOW())
            """, (str(part_id), str(org_id), part_name, Json(attributes)))
            
            return part_id
            
        finally:
            cursor.close()
    
    def find_similar_bom_items(
        self,
        assembly_id: UUID,
        part_id: UUID,
        context: Dict[str, Any],
        similarity_threshold: float = 0.7
    ) -> List[Tuple[UUID, float]]:
        """
        Find existing bom_items that might match this usage.
        
        Matches using assembly_id, part_id, and context similarity.
        """
        cursor = self._get_cursor()
        
        try:
            # First, check if context column exists
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'bom_items_' AND column_name = 'context'
            """)
            has_context = cursor.fetchone() is not None
            
            if not has_context:
                # If context column doesn't exist, just match by assembly_id and part_id
                # This allows the system to work even if schema is slightly different
                logger.warning(
                    "bom_items_ table does not have 'context' column. "
                    "Matching only by assembly_id and part_id. "
                    "Consider adding 'context jsonb' column to bom_items_ table."
                )
                cursor.execute("""
                    SELECT id
                    FROM bom_items_
                    WHERE assembly_id = %s AND part_id = %s
                """, (str(assembly_id), str(part_id)))
                
                candidates = cursor.fetchall()
                matches = []
                
                for candidate in candidates:
                    # If no context column, assume perfect match (1.0) if assembly+part match
                    matches.append((UUID(candidate['id']), 1.0))
                
                matches.sort(key=lambda x: x[1], reverse=True)
                return matches
            
            # Get all bom_items_ for this assembly and part with context
            cursor.execute("""
                SELECT id, context
                FROM bom_items_
                WHERE assembly_id = %s AND part_id = %s
            """, (str(assembly_id), str(part_id)))
            
            candidates = cursor.fetchall()
            matches = []
            
            for candidate in candidates:
                candidate_context = candidate.get('context') or {}
                if isinstance(candidate_context, str):
                    import json
                    candidate_context = json.loads(candidate_context)
                context_sim = _jsonb_similarity(context, candidate_context)
                
                if context_sim >= similarity_threshold:
                    matches.append((UUID(candidate['id']), context_sim))
            
            # Sort by confidence descending
            matches.sort(key=lambda x: x[1], reverse=True)
            
            return matches
            
        finally:
            cursor.close()
    
    def create_bom_item(
        self,
        assembly_id: UUID,
        part_id: UUID,
        context: Dict[str, Any]
    ) -> UUID:
        """Create a new bom_item (part usage in assembly)."""
        cursor = self._get_cursor()
        
        try:
            # Check if context column exists
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'bom_items_' AND column_name = 'context'
            """)
            has_context = cursor.fetchone() is not None
            
            bom_item_id = uuid4()
            
            if has_context:
                cursor.execute("""
                    INSERT INTO bom_items_ (id, assembly_id, part_id, context, created_at)
                    VALUES (%s, %s, %s, %s::jsonb, NOW())
                """, (str(bom_item_id), str(assembly_id), str(part_id), Json(context)))
            else:
                # If context column doesn't exist, insert without it
                logger.warning(
                    "bom_items_ table does not have 'context' column. "
                    "Creating bom_item without context. "
                    "Consider adding 'context jsonb' column to bom_items_ table."
                )
                cursor.execute("""
                    INSERT INTO bom_items_ (id, assembly_id, part_id, created_at)
                    VALUES (%s, %s, %s, NOW())
                """, (str(bom_item_id), str(assembly_id), str(part_id)))
            
            return bom_item_id
            
        finally:
            cursor.close()
    
    def create_snapshot(
        self,
        org_id: UUID,
        assembly_id: UUID,
        source: str,
        parent_snapshot_id: Optional[UUID] = None
    ) -> UUID:
        """Create a new snapshot (immutable)."""
        cursor = self._get_cursor()
        
        try:
            snapshot_id = uuid4()
            cursor.execute("""
                INSERT INTO snapshots (id, org_id, assembly_id, source, parent_snapshot_id, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (str(snapshot_id), str(org_id), str(assembly_id), source, str(parent_snapshot_id) if parent_snapshot_id else None))
            
            return snapshot_id
            
        finally:
            cursor.close()
    
    def insert_snapshot_item(
        self,
        snapshot_id: UUID,
        bom_item_id: UUID,
        quantity: Optional[int],
        attributes: Dict[str, Any],
        checksum: str
    ) -> None:
        """
        Insert a snapshot_item (materialized state at snapshot time).
        
        Uses ON CONFLICT to make this idempotent - if the same (snapshot_id, bom_item_id)
        already exists, it updates the values instead of failing.
        This handles cases where the same bom_item appears multiple times in the input.
        """
        cursor = self._get_cursor()
        
        try:
            cursor.execute("""
                INSERT INTO snapshot_items (snapshot_id, bom_item_id, quantity, attributes, checksum)
                VALUES (%s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (snapshot_id, bom_item_id)
                DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    attributes = EXCLUDED.attributes,
                    checksum = EXCLUDED.checksum
            """, (str(snapshot_id), str(bom_item_id), quantity, Json(attributes), checksum))
            
        finally:
            cursor.close()
    
    def get_snapshot_items(
        self,
        snapshot_id: UUID
    ) -> List[Dict[str, Any]]:
        """
        Get all snapshot items for a snapshot.
        
        Returns raw data from snapshot_items table for diffing.
        Note: This method does NOT require a transaction - it uses a new connection.
        """
        # Get a connection from pool (not transaction connection)
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT 
                    bom_item_id,
                    quantity,
                    attributes,
                    checksum
                FROM snapshot_items
                WHERE snapshot_id = %s
            """, (str(snapshot_id),))
            
            items = cursor.fetchall()
            
            # Convert to list of dicts with proper types
            result = []
            for item in items:
                result.append({
                    'bom_item_id': str(item['bom_item_id']),
                    'quantity': item['quantity'],
                    'attributes': item['attributes'] or {},
                    'checksum': item['checksum']
                })
            
            return result
            
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def get_bom_item_details(
        self,
        bom_item_ids: List[UUID]
    ) -> Dict[UUID, Dict[str, Any]]:
        """
        Get details for bom_items including part and assembly information.
        """
        if not bom_item_ids:
            return {}
        
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Convert UUIDs to strings for SQL
            bom_item_id_strs = [str(bid) for bid in bom_item_ids]
            
            # Check if context column exists
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'bom_items_' AND column_name = 'context'
            """)
            has_context = cursor.fetchone() is not None
            
            # Query bom_items_ with joins to parts and assemblies
            if has_context:
                cursor.execute("""
                    SELECT 
                        bi.id as bom_item_id,
                        bi.assembly_id,
                        a.name as assembly_name,
                        bi.part_id,
                        p.name as part_name,
                        p.attributes as part_attributes,
                        bi.context
                    FROM bom_items_ bi
                    JOIN assemblies a ON bi.assembly_id = a.id
                    JOIN parts p ON bi.part_id = p.id
                    WHERE bi.id = ANY(%s::uuid[])
                """, (bom_item_id_strs,))
            else:
                cursor.execute("""
                    SELECT 
                        bi.id as bom_item_id,
                        bi.assembly_id,
                        a.name as assembly_name,
                        bi.part_id,
                        p.name as part_name,
                        p.attributes as part_attributes,
                        NULL as context
                    FROM bom_items_ bi
                    JOIN assemblies a ON bi.assembly_id = a.id
                    JOIN parts p ON bi.part_id = p.id
                    WHERE bi.id = ANY(%s::uuid[])
                """, (bom_item_id_strs,))
            
            results = cursor.fetchall()
            
            # Build result dictionary
            details = {}
            for row in results:
                bom_item_id = UUID(row['bom_item_id'])
                details[bom_item_id] = {
                    'bom_item_id': bom_item_id,
                    'assembly_id': UUID(row['assembly_id']),
                    'assembly_name': row['assembly_name'] or 'Unknown',
                    'part_id': UUID(row['part_id']),
                    'part_name': row['part_name'] or 'Unknown',
                    'part_attributes': row['part_attributes'] or {},
                    'context': row['context'] or {}
                }
            
            return details
            
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def get_snapshot_info(
        self,
        snapshot_id: UUID
    ) -> Dict[str, Any]:
        """
        Get snapshot metadata including assembly information.
        """
        conn = self._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cursor.execute("""
                SELECT 
                    s.id as snapshot_id,
                    s.assembly_id,
                    a.name as assembly_name,
                    s.org_id,
                    s.source,
                    s.created_at,
                    s.parent_snapshot_id
                FROM snapshots s
                JOIN assemblies a ON s.assembly_id = a.id
                WHERE s.id = %s
            """, (str(snapshot_id),))
            
            result = cursor.fetchone()
            
            if not result:
                return {}
            
            return {
                'snapshot_id': UUID(result['snapshot_id']),
                'assembly_id': UUID(result['assembly_id']) if result['assembly_id'] else None,
                'assembly_name': result['assembly_name'] or 'Unknown',
                'org_id': UUID(result['org_id']) if result['org_id'] else None,
                'source': result['source'],
                'created_at': result['created_at'],
                'parent_snapshot_id': UUID(result['parent_snapshot_id']) if result['parent_snapshot_id'] else None
            }
            
        finally:
            cursor.close()
            self._return_connection(conn)
    
    def close(self):
        """Close the connection pool."""
        if self._pool:
            self._pool.closeall()
            self._pool = None

