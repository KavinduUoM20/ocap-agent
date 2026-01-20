"""Service for creating Elasticsearch indexes from XLSX files."""
import uuid
import pandas as pd
from typing import Dict, Any
from collections import defaultdict, Counter
from elasticsearch import helpers
from app.infra.elastic import get_elasticsearch_client
from app.core.logging import logger


class IndexService:
    """Service for creating Elasticsearch indexes from XLSX data."""
    
    def __init__(self):
        """Initialize Index service."""
        self.logger = logger
        self.client = get_elasticsearch_client()
    
    def create_indexes_from_excel(self, file_path: str) -> Dict[str, Any]:
        """
        Create both Elasticsearch indexes from an XLSX file.
        
        Args:
            file_path: Path to the XLSX file
            
        Returns:
            Dictionary with status and details of index creation
        """
        try:
            # Step 1: Read and clean the Excel file
            df = pd.read_excel(file_path)
            
            # Step 2: Filter required columns
            required_columns = ["Style", "Defect", "Operation", "Error", "Action"]
            df_clean = df.loc[:, df.columns.intersection(required_columns)]
            
            # Step 3: Normalize columns (lowercase and replace nan)
            cols = ["Operation", "Defect", "Error", "Action"]
            df_clean[cols] = df_clean[cols].apply(
                lambda s: s.astype(str).str.lower()
            ).replace("nan", pd.NA)
            
            # Step 4: Lowercase column names
            df_clean.columns = df_clean.columns.str.lower()
            
            # Step 5: Build content field
            def build_content(row):
                return (
                    f"Style: {row['style']}. "
                    f"Defect: {row['defect']}. "
                    f"Operation: {row['operation']}. "
                    f"Error: {row['error']}. "
                    f"Action: {row['action']}."
                )
            
            df_clean["content"] = df_clean.apply(build_content, axis=1)
            
            # Step 6: Clean and prepare data
            df_clean = df_clean.fillna("")
            df_clean["content"] = df_clean["content"].astype(str).str.strip()
            df_clean = df_clean[df_clean["content"] != ""]
            
            # Step 7: Create fact layer index (ocap-knowledge-base)
            fact_index_name = "ocap-knowledge-base"
            self._create_fact_index(fact_index_name, df_clean)
            
            # Step 8: Create relationship index (ocap-relationship-index)
            rel_index_name = "ocap-relationship-index"
            self._create_relationship_index(rel_index_name, fact_index_name)
            
            return {
                "status": "success",
                "message": "Indexes created successfully",
                "fact_index": fact_index_name,
                "relationship_index": rel_index_name,
                "records_processed": len(df_clean)
            }
            
        except Exception as e:
            self.logger.error(f"Error creating indexes: {e}", exc_info=True)
            raise
    
    def _create_fact_index(self, index_name: str, df_clean: pd.DataFrame) -> None:
        """
        Create the fact layer index (ocap-knowledge-base).
        
        Args:
            index_name: Name of the index
            df_clean: Cleaned DataFrame
        """
        # Delete existing index if it exists
        self.client.options(ignore_status=[404]).indices.delete(index=index_name)
        
        # Create index with mappings
        self.client.indices.create(
            index=index_name,
            mappings={
                "properties": {
                    "style": {"type": "keyword"},
                    "defect": {"type": "keyword"},
                    "operation": {"type": "keyword"},
                    "error": {"type": "keyword"},
                    "action": {"type": "text"},
                    "content": {"type": "semantic_text"}
                }
            }
        )
        
        # Prepare documents for bulk indexing
        docs = []
        for _, row in df_clean.iterrows():
            docs.append({
                "_op_type": "index",
                "_index": index_name,
                "_id": str(uuid.uuid4()),
                "style": str(row["style"]),
                "defect": str(row["defect"]),
                "operation": str(row["operation"]),
                "error": str(row["error"]),
                "action": str(row["action"]),
                "content": row["content"]
            })
        
        # Bulk index documents
        success, errors = helpers.bulk(
            self.client.options(request_timeout=600),
            docs,
            refresh="wait_for",
            raise_on_error=False,
            raise_on_exception=False
        )
        
        self.logger.info(f"Fact index created: {success} documents indexed, {len(errors)} errors")
        if errors:
            self.logger.warning(f"Some errors occurred during indexing: {errors[:5]}")  # Log first 5 errors
    
    def _create_relationship_index(self, rel_index_name: str, fact_index_name: str) -> None:
        """
        Create the relationship index (ocap-relationship-index).
        
        Args:
            rel_index_name: Name of the relationship index
            fact_index_name: Name of the fact index
        """
        # Define relationship mapping
        relationship_mapping = {
            "properties": {
                "node_type": {"type": "keyword"},
                "name": {
                    "type": "keyword",
                    "fields": {
                        "text": {"type": "text"}
                    }
                },
                "related_operations": {
                    "type": "nested",
                    "properties": {
                        "name": {"type": "keyword"},
                        "count": {"type": "integer"}
                    }
                },
                "related_defects": {
                    "type": "nested",
                    "properties": {
                        "name": {"type": "keyword"},
                        "count": {"type": "integer"}
                    }
                },
                "related_errors": {
                    "type": "nested",
                    "properties": {
                        "name": {"type": "keyword"},
                        "count": {"type": "integer"}
                    }
                },
                "related_styles": {
                    "type": "nested",
                    "properties": {
                        "name": {"type": "keyword"},
                        "count": {"type": "integer"}
                    }
                },
                "top_actions": {
                    "type": "nested",
                    "properties": {
                        "action": {"type": "text"},
                        "count": {"type": "integer"}
                    }
                },
                "total_cases": {"type": "integer"}
            }
        }
        
        # Delete existing index if it exists
        self.client.options(ignore_status=[404]).indices.delete(index=rel_index_name)
        
        # Create relationship index
        self.client.indices.create(
            index=rel_index_name,
            mappings=relationship_mapping
        )
        
        # Fetch all facts from fact index
        response = self.client.search(
            index=fact_index_name,
            size=1000,
            query={"match_all": {}}
        )
        
        facts = [hit["_source"] for hit in response["hits"]["hits"]]
        
        # Group facts by operation, defect, error, and style
        operations = defaultdict(list)
        defects = defaultdict(list)
        styles = defaultdict(list)
        errors = defaultdict(list)
        
        for row in facts:
            op = row.get("operation", "")
            defect = row.get("defect", "")
            error = row.get("error", "")
            style = row.get("style", "")
            action = row.get("action", "")
            
            if op:
                operations[op].append(row)
            if defect:
                defects[defect].append(row)
            if error:
                errors[error].append(row)
            if style:
                styles[style].append(row)
        
        # Helper to build relationship doc
        def build_relationship_doc(node_type, name, rows):
            return {
                "node_type": node_type,
                "name": name,
                "related_operations": [
                    {"name": k, "count": v}
                    for k, v in Counter(r.get("operation", "") for r in rows).items()
                    if k and k != name
                ],
                "related_defects": [
                    {"name": k, "count": v}
                    for k, v in Counter(r.get("defect", "") for r in rows).items()
                    if k and k != name
                ],
                "related_errors": [
                    {"name": k, "count": v}
                    for k, v in Counter(r.get("error", "") for r in rows).items()
                    if k and k != name
                ],
                "related_styles": [
                    {"name": k, "count": v}
                    for k, v in Counter(r.get("style", "") for r in rows).items()
                    if k and k != name
                ],
                "top_actions": [
                    {"action": k, "count": v}
                    for k, v in Counter(r.get("action", "") for r in rows).most_common(5)
                    if k
                ],
                "total_cases": len(rows)
            }
        
        # Build all relationship documents
        relationship_docs = []
        
        for name, rows in operations.items():
            relationship_docs.append(
                build_relationship_doc("operation", name, rows)
            )
        
        for name, rows in defects.items():
            relationship_docs.append(
                build_relationship_doc("defect", name, rows)
            )
        
        for name, rows in errors.items():
            relationship_docs.append(
                build_relationship_doc("error", name, rows)
            )
        
        for name, rows in styles.items():
            if name.strip():
                relationship_docs.append(
                    build_relationship_doc("style", name, rows)
                )
        
        # Bulk ingest relationship index
        bulk_actions = []
        
        for doc in relationship_docs:
            bulk_actions.append({
                "_op_type": "index",
                "_index": rel_index_name,
                "_id": f"{doc['node_type']}::{doc['name']}",
                **doc
            })
        
        helpers.bulk(
            self.client.options(request_timeout=300),
            bulk_actions,
            refresh="wait_for"
        )
        
        self.logger.info(f"Relationship index created: {len(bulk_actions)} relationship nodes ingested")
