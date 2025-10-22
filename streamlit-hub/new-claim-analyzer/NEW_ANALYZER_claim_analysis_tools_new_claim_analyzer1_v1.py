#!/usr/bin/env python3
"""
New Claim Analysis Engine with Qdrant Integration
Analyzes any new claim using the same logic as the view and stores metadata in Qdrant
"""

import pyodbc
import pandas as pd
import json
import warnings
from datetime import datetime
from typing import Dict, List, Any
import uuid
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
import numpy as np

warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

class NewClaimAnalyzer:
    def __init__(self):
        self.server = "localhost,1433"
        self.database = "_reporting"
        self.username = "SA"
        self.password = "Bbanwo@1980!"
        
        self.conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={self.server};Database={self.database};"
            f"UID={self.username};PWD={self.password};"
            "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
        )
        
        # Initialize Qdrant client
        self.qdrant_client = QdrantClient(host="localhost", port=6333)
        self.collection_name = "claim_analysis_metadata"
        self._ensure_qdrant_collection()
    
    def _ensure_qdrant_collection(self):
        """Ensure Qdrant collection exists with 768 dimensions"""
        try:
            collections = self.qdrant_client.get_collections()
            collection_names = [col.name for col in collections.collections]
            
            if self.collection_name not in collection_names:
                self.qdrant_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=768, distance=Distance.COSINE)
                )
                print(f"Created Qdrant collection: {self.collection_name} with 768 dimensions")
            else:
                # Check if collection needs to be recreated for different size
                collection_info = self.qdrant_client.get_collection(self.collection_name)
                current_size = collection_info.config.params.vectors.size
                if current_size != 768:
                    print(f"Recreating collection {self.collection_name} from {current_size} to 768 dimensions")
                    self.qdrant_client.delete_collection(self.collection_name)
                    self.qdrant_client.create_collection(
                        collection_name=self.collection_name,
                        vectors_config=VectorParams(size=768, distance=Distance.COSINE)
                    )
                else:
                    print(f"Qdrant collection {self.collection_name} already exists with 768 dimensions")
        except Exception as e:
            print(f"Warning: Could not ensure Qdrant collection: {e}")

    def check_qdrant_status(self):
        """Check if collection exists and has points"""
        try:
            # Count points in collection
            count_result = self.qdrant_client.count(
                collection_name=self.collection_name,
                exact=True
            )
            print(f"Number of points in collection: {count_result.count}")
            return count_result.count
        except Exception as e:
            print(f"Error checking Qdrant status: {e}")
            return 0

    def list_all_points(self, limit: int = 10):
        """List all points in the collection to verify data"""
        try:
            # Scroll through all points
            scroll_result = self.qdrant_client.scroll(
                collection_name=self.collection_name,
                limit=limit,
                with_payload=True,
                with_vectors=False
            )
            
            points = scroll_result[0]
            print(f"Found {len(points)} points in collection:")
            
            for i, point in enumerate(points):
                print(f"Point {i+1}:")
                print(f"  ID: {point.id}")
                print(f"  Payload: {point.payload}")
                print("  ---")
                
            return points
            
        except Exception as e:
            print(f"Error listing points: {e}")
            return []

    def get_connection(self):
        """Get database connection"""
        try:
            return pyodbc.connect(self.conn_str)
        except Exception as e:
            print(f"Database connection failed: {e}")
            return None

    def analyze_new_claim(self, claim_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a new claim using the same logic as the view
        
        Args:
            claim_data: Dictionary containing claim information
            
        Returns:
            Dictionary with analysis results
        """
        conn = self.get_connection()
        if not conn:
            return {"error": "Database connection failed"}
        
        try:
            # Extract claim information
            clm_id = claim_data.get('CLM_ID')
            desynpuf_id = claim_data.get('DESYNPUF_ID')
            clm_from_dt = claim_data.get('CLM_FROM_DT')
            clm_thru_dt = claim_data.get('CLM_THRU_DT')
            prvdr_num = claim_data.get('PRVDR_NUM')
            
            # Extract diagnosis codes
            diagnosis_codes = claim_data.get('diagnosis_codes', {})
            dx_codes = []
            for i in range(1, 11):
                code = diagnosis_codes.get(f'ICD9_DGNS_CD_{i}')
                if code:
                    dx_codes.append((i, code))
            
            # Extract procedure codes
            procedure_codes = claim_data.get('procedure_codes', {})
            hcpcs_codes = []
            for i in range(1, 46):
                code = procedure_codes.get(f'HCPCS_CD_{i}')
                if code:
                    hcpcs_codes.append((i, code))
            
            # Create all_procedures list for duplicate and bundling checks
            all_procedures = [code for _, code in hcpcs_codes]
            
            # Convert date format
            clm_from_date = None
            clm_thru_date = None
            if clm_from_dt:
                try:
                    clm_from_date = datetime.strptime(clm_from_dt, '%Y%m%d').date()
                except:
                    pass
            if clm_thru_dt:
                try:
                    clm_thru_date = datetime.strptime(clm_thru_dt, '%Y%m%d').date()
                except:
                    pass
            
            # Analyze each diagnosis-procedure combination
            results = []
            
            for dx_pos, dx_code in dx_codes:
                for hcpcs_pos, hcpcs_code in hcpcs_codes:
                    # Get ICD-10 mapping (check if already ICD-10)
                    if dx_code and dx_code[0].isalpha():
                        # Already ICD-10 format (starts with letter)
                        mapped_icd10 = dx_code
                    else:
                        # ICD-9 format, need mapping
                        mapped_icd10 = self._get_icd10_mapping(conn, dx_code)
                    
                    # Get NCCI data
                    ncci_data = self._get_ncci_data(conn, hcpcs_code)
                    
                    # Get NCD data
                    ncd_data = self._get_ncd_data(conn, hcpcs_code, clm_from_date)
                    
                    # Determine LCD coverage (use mapped ICD-10 code if available)
                    lcd_covered = self._determine_lcd_coverage(conn, hcpcs_code, mapped_icd10 if mapped_icd10 else dx_code)
                    
                    # Calculate risk analysis
                    risk_analysis = self._calculate_risk_analysis(
                        dx_pos, dx_code, mapped_icd10,
                        hcpcs_pos, hcpcs_code,
                        ncci_data, ncd_data, lcd_covered,
                        claim_data, all_procedures
                    )
                    
                    # Get human-readable names
                    diagnosis_name = self._get_diagnosis_name(conn, mapped_icd10) if mapped_icd10 else None
                    procedure_name = self._get_procedure_name(conn, hcpcs_code)
                    
                    results.append({
                        'CLM_ID': clm_id,
                        'DESYNPUF_ID': desynpuf_id,
                        'clm_from_dt': clm_from_date,
                        'clm_thru_dt': clm_thru_date,
                        'PRVDR_NUM': prvdr_num,
                        'dx_position': dx_pos,
                        'icd9_dgns_code': dx_code,
                        'mapped_icd10_code': mapped_icd10,
                        'diagnosis_name': diagnosis_name,
                        'hcpcs_position': hcpcs_pos,
                        'hcpcs_code': hcpcs_code,
                        'procedure_name': procedure_name,
                        'ncd_id': ncd_data.get('ncd_id'),
                        'ncd_title': ncd_data.get('ncd_title'),
                        'ncd_status': ncd_data.get('ncd_status'),
                        'lcd_icd10_covered_group': lcd_covered,
                        'ptp_denial_reason': ncci_data.get('ptp_denial_reason'),
                        'mue_threshold': ncci_data.get('mue_threshold'),
                        'mue_denial_type': ncci_data.get('mue_denial_type'),
                        **risk_analysis,
                        'analysis_timestamp': datetime.now()
                    })
            
            # Generate summary
            summary = self._generate_summary(results)
            
            # Extract comprehensive metadata for RAG models
            metadata = self._extract_metadata(claim_data, results, clm_from_date, hcpcs_codes, dx_codes)
            
            # Store individual DX-PROC combinations in Qdrant
            qdrant_status = self.store_metadata_in_qdrant(metadata, results)
            
            # Search for similar historical claims
            similar_claims = self.search_similar_claims(metadata)
            
            return {
                'claim_summary': summary,
                'detailed_issues': results,
                'actionable_fixes': self._generate_actionable_fixes(results),
                'metadata': metadata,
                'similar_claims': similar_claims,
                'qdrant_storage': qdrant_status  # Add storage status to results
            }
            
        except Exception as e:
            return {"error": f"Analysis failed: {e}"}
        finally:
            conn.close()

    def store_metadata_in_qdrant(self, metadata: Dict[str, Any], detailed_issues: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Store claim analysis metadata in Qdrant - store individual DX-PROC combinations
        
        Args:
            metadata: Analysis metadata dictionary
            detailed_issues: List of detailed issue results from analysis
            
        Returns:
            Dictionary with storage status
        """
        try:
            if not detailed_issues:
                return {
                    "success": False,
                    "error": "No detailed issues provided",
                    "timestamp": datetime.now().isoformat()
                }
            
            print("=== Storing Individual DX-PROC Combinations in Qdrant ===")
            points_added = 0
            vector_ids = []
            
            for issue in detailed_issues:
                try:
                    # Generate unique ID for this DX-PROC combination
                    combo_id = f"{issue['CLM_ID']}_{issue['dx_position']}_{issue['hcpcs_position']}"
                    vector_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, combo_id))
                    
                    # Create embedding from this specific combination
                    combo_metadata = self._create_combo_metadata(metadata, issue)
                    embedding = self._create_embedding_from_metadata(combo_metadata)
                    
                    # Convert embedding to native Python types
                    embedding = [float(x) for x in embedding]
                    
                    # Prepare payload for individual DX-PROC combination
                    payload = {
                        # Basic claim info
                        "claim_id": str(issue.get('CLM_ID', 'unknown')),
                        "patient_id": str(issue.get('DESYNPUF_ID', 'unknown')),
                        "provider_id": str(issue.get('PRVDR_NUM', 'unknown')),
                        "service_date": str(issue.get('clm_from_dt', '')),
                        "analysis_timestamp": datetime.now().isoformat(),
                        
                        # Diagnosis information
                        "icd9_code": str(issue.get('icd9_dgns_code', '')),
                        "icd10_code": str(issue.get('mapped_icd10_code', '')),
                        "dx_position": int(issue.get('dx_position', 0)),
                        "diagnosis_name": str(issue.get('diagnosis_name', '')),
                        
                        # Procedure information  
                        "hcpcs_code": str(issue.get('hcpcs_code', '')),
                        "hcpcs_position": int(issue.get('hcpcs_position', 0)),
                        "procedure_name": str(issue.get('procedure_name', '')),
                        
                        # Coverage and policy data
                        "ncd_id": str(issue.get('ncd_id', '')),
                        "ncd_title": str(issue.get('ncd_title', '')),
                        "ncd_status": str(issue.get('ncd_status', '')),
                        "ptp_denial_reason": str(issue.get('ptp_denial_reason', '')),
                        "mue_threshold": self._convert_to_native(issue.get('mue_threshold')),
                        "mue_denial_type": str(issue.get('mue_denial_type', '')),
                        "lcd_icd10_covered_group": str(issue.get('lcd_icd10_covered_group', 'N')),
                        
                        # Risk analysis
                        "denial_risk_level": str(issue.get('denial_risk_level', 'OK')),
                        "denial_risk_score": float(issue.get('denial_risk_score', 0)),
                        "risk_category": str(issue.get('risk_category', 'LOW')),
                        "action_required": str(issue.get('action_required', '')),
                        "business_impact": str(issue.get('business_impact', '')),
                        
                        # Additional metadata for search
                        "combo_id": combo_id,
                        "claim_metadata": {
                            "total_issues": int(metadata.get("total_issues", 0)),
                            "critical_issues": int(metadata.get("critical_issues", 0)),
                            "high_issues": int(metadata.get("high_issues", 0)),
                            "max_risk_score": float(metadata.get("max_risk_score", 0)),
                            "avg_risk_score": float(metadata.get("avg_risk_score", 0))
                        }
                    }
                    
                    # Clean up empty values and ensure all values are JSON serializable
                    payload = self._clean_payload(payload)
                    
                    # Create point
                    point = PointStruct(
                        id=vector_id,
                        vector=embedding,
                        payload=payload
                    )
                    
                    # Upsert to Qdrant
                    self.qdrant_client.upsert(
                        collection_name=self.collection_name,
                        wait=True,
                        points=[point]
                    )
                    
                    points_added += 1
                    vector_ids.append(vector_id)
                    
                    print(f"  Stored: DX{issue['dx_position']} {issue.get('icd9_dgns_code', '')}  PROC{issue['hcpcs_position']} {issue.get('hcpcs_code', '')}")
                    print(f"     Risk: {issue.get('denial_risk_level', 'OK')} (Score: {issue.get('denial_risk_score', 0)})")
                    
                except Exception as e:
                    print(f"  ERROR: Failed to store combination {combo_id}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
                
            # Final verification
            final_count = self.check_qdrant_status()
            
            return {
                "success": True,
                "points_added": points_added,
                "vector_ids": vector_ids,
                "collection": self.collection_name,
                "timestamp": datetime.now().isoformat(),
                "final_point_count": final_count
            }
            
        except Exception as e:
            print(f"Error storing in Qdrant: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    def _convert_to_native(self, value):
        """Convert numpy/pandas types to native Python types"""
        if value is None:
            return None
        elif hasattr(value, 'item'):  # numpy types
            return value.item()
        elif hasattr(value, 'dtype'):  # pandas types
            return value.item() if hasattr(value, 'item') else value
        else:
            return value

    def _clean_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Clean payload to ensure all values are JSON serializable"""
        cleaned = {}
        
        for key, value in payload.items():
            if value is None:
                continue  # Skip None values
            elif isinstance(value, (str, int, float, bool)):
                cleaned[key] = value
            elif isinstance(value, (list, tuple)):
                # Recursively clean lists
                cleaned[key] = [self._convert_to_native(item) for item in value if item is not None]
            elif isinstance(value, dict):
                # Recursively clean dictionaries
                cleaned[key] = self._clean_payload(value)
            elif hasattr(value, 'item'):  # numpy types
                cleaned[key] = value.item()
            elif hasattr(value, 'isoformat'):  # datetime objects
                cleaned[key] = value.isoformat()
            else:
                # Convert to string as last resort
                try:
                    cleaned[key] = str(value)
                except:
                    print(f"Warning: Could not serialize key '{key}' with value type {type(value)}")
                    continue
        
        return cleaned

    def _create_combo_metadata(self, claim_metadata: Dict[str, Any], issue: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create metadata for individual DX-PROC combination
        """
        combo_metadata = claim_metadata.copy()
        
        # Add combination-specific fields
        combo_metadata.update({
            "dx_position": issue.get('dx_position'),
            "hcpcs_position": issue.get('hcpcs_position'),
            "icd9_code": issue.get('icd9_dgns_code'),
            "icd10_code": issue.get('mapped_icd10_code'),
            "hcpcs_code": issue.get('hcpcs_code'),
            "denial_risk_level": issue.get('denial_risk_level'),
            "denial_risk_score": issue.get('denial_risk_score'),
            "risk_category": issue.get('risk_category'),
            "lcd_coverage": issue.get('lcd_icd10_covered_group'),
            "ncd_status": issue.get('ncd_status'),
            "ptp_denial_reason": issue.get('ptp_denial_reason'),
            "mue_threshold": issue.get('mue_threshold')
        })
        
        return combo_metadata

    def _create_embedding_from_metadata(self, metadata: Dict[str, Any]) -> List[float]:
        """
        Create a 768-dimensional vector embedding from metadata
        
        Args:
            metadata: Analysis metadata
            
        Returns:
            Vector embedding as list of floats (768 dimensions)
        """
        # Create feature vector from metadata
        features = []
        
        # Risk scores (normalized)
        max_risk = min(metadata.get("max_risk_score", 0) / 150.0, 1.0)  # Normalize to 0-1
        avg_risk = min(metadata.get("avg_risk_score", 0) / 150.0, 1.0)
        features.extend([max_risk, avg_risk])
        
        # Issue counts (normalized)
        total_issues = metadata.get("total_issues", 0)
        critical_issues = metadata.get("critical_issues", 0)
        high_issues = metadata.get("high_issues", 0)
        medium_issues = metadata.get("medium_issues", 0)
        low_issues = metadata.get("low_issues", 0)
        
        # Normalize issue counts (assuming max 20 issues for normalization)
        features.extend([
            min(total_issues / 20.0, 1.0),
            min(critical_issues / 10.0, 1.0),
            min(high_issues / 10.0, 1.0),
            min(medium_issues / 15.0, 1.0),
            min(low_issues / 20.0, 1.0)
        ])
        
        # Procedure and diagnosis counts
        cpt_count = len(metadata.get("all_cpt_codes", []))
        icd_count = len(metadata.get("icd_codes", []))
        features.extend([
            min(cpt_count / 20.0, 1.0),
            min(icd_count / 10.0, 1.0)
        ])
        
        # Denial indicators (binary features)
        denial_indicators = metadata.get("denial_indicators", {})
        features.extend([
            1.0 if denial_indicators.get("high_denial_risk", False) else 0.0,
            1.0 if denial_indicators.get("critical_issues_present", False) else 0.0,
            1.0 if denial_indicators.get("duplicate_billing_risk", False) else 0.0,
            1.0 if denial_indicators.get("bundling_risk", False) else 0.0,
            1.0 if denial_indicators.get("pa_required", False) else 0.0,
            1.0 if denial_indicators.get("ncci_conflicts", False) else 0.0,
            1.0 if denial_indicators.get("credentialing_issues", False) else 0.0,
            1.0 if denial_indicators.get("modifier_issues", False) else 0.0,
            1.0 if denial_indicators.get("frequency_issues", False) else 0.0,
            1.0 if denial_indicators.get("coverage_issues", False) else 0.0
        ])
        
        # Provider type encoding (one-hot like)
        provider_type = metadata.get("provider_type", "Unknown")
        provider_features = [0.0] * 8  # 8 provider types
        provider_types = ["Hospital", "Facility", "Physician", "Group", "Laboratory", "Radiology", "Ambulance", "DME"]
        if provider_type in provider_types:
            provider_features[provider_types.index(provider_type)] = 1.0
        features.extend(provider_features)
        
        # Procedure categories (normalized counts)
        procedure_categories = metadata.get("procedure_categories", {})
        category_features = []
        for category in ["surgery", "radiology", "laboratory", "evaluation_management", 
                        "anesthesia", "pathology", "medicine", "other"]:
            count = len(procedure_categories.get(category, []))
            category_features.append(min(count / 10.0, 1.0))
        features.extend(category_features)
        
        # Diagnosis categories (normalized counts)
        diagnosis_categories = metadata.get("diagnosis_categories", {})
        diag_features = []
        for category in ["musculoskeletal", "cardiovascular", "endocrine", "respiratory", 
                        "gastrointestinal", "neurological", "mental_health", "infectious_disease", 
                        "neoplasms", "other"]:
            count = len(diagnosis_categories.get(category, []))
            diag_features.append(min(count / 5.0, 1.0))
        features.extend(diag_features)
        
        # Billing patterns
        billing_patterns = metadata.get("billing_patterns", {})
        features.extend([
            min(len(billing_patterns.get("duplicate_procedures", [])) / 5.0, 1.0),
            min(len(billing_patterns.get("high_cost_procedures", [])) / 3.0, 1.0),
            min(len(billing_patterns.get("bundled_procedures", [])) / 5.0, 1.0),
            min(len(billing_patterns.get("frequent_procedures", [])) / 5.0, 1.0),
            min(len(billing_patterns.get("unusual_combinations", [])) / 3.0, 1.0)
        ])
        
        # Risk metadata features
        risk_metadata = metadata.get("risk_metadata", {})
        features.extend([
            min(risk_metadata.get("duplicate_issues", 0) / 5.0, 1.0),
            min(risk_metadata.get("bundling_issues", 0) / 5.0, 1.0),
            min(risk_metadata.get("pa_issues", 0) / 3.0, 1.0),
            min(risk_metadata.get("credentialing_issues", 0) / 2.0, 1.0),
            min(risk_metadata.get("modifier_issues", 0) / 5.0, 1.0),
            min(risk_metadata.get("frequency_issues", 0) / 5.0, 1.0),
            min(risk_metadata.get("ncci_conflicts", 0) / 3.0, 1.0),
            min(risk_metadata.get("coverage_issues", 0) / 5.0, 1.0),
            min(risk_metadata.get("mue_risks", 0) / 5.0, 1.0),
            min(risk_metadata.get("ncd_issues", 0) / 3.0, 1.0)
        ])
        
        # Units information
        units = metadata.get("units", {})
        total_units = sum(units.values())
        unique_procedures = len(units)
        avg_units_per_procedure = total_units / unique_procedures if unique_procedures > 0 else 0
        features.extend([
            min(total_units / 50.0, 1.0),
            min(unique_procedures / 20.0, 1.0),
            min(avg_units_per_procedure / 5.0, 1.0)
        ])
        
        # Modifiers count
        modifiers_count = len(metadata.get("modifiers", []))
        features.append(min(modifiers_count / 10.0, 1.0))
        
        # Service date features (if available)
        service_date = metadata.get("service_date")
        if service_date:
            # Extract month and day of week as cyclical features
            month_sin = np.sin(2 * np.pi * service_date.month / 12)
            month_cos = np.cos(2 * np.pi * service_date.month / 12)
            day_of_week_sin = np.sin(2 * np.pi * service_date.weekday() / 7)
            day_of_week_cos = np.cos(2 * np.pi * service_date.weekday() / 7)
            features.extend([month_sin, month_cos, day_of_week_sin, day_of_week_cos])
        else:
            features.extend([0.0, 0.0, 0.0, 0.0])
        
        # Pad or truncate to exactly 768 dimensions
        target_size = 768
        if len(features) < target_size:
            # Pad with small random values (better than zeros for some models)
            remaining = target_size - len(features)
            padding = np.random.normal(0, 0.01, remaining).tolist()  # Convert to list
            features.extend(padding)
        elif len(features) > target_size:
            # Truncate
            features = features[:target_size]
        
        # Ensure we have exactly 768 dimensions and convert to native Python floats
        assert len(features) == 768, f"Expected 768 dimensions, got {len(features)}"
        
        # Convert all values to native Python floats
        features = [float(x) for x in features]
        
        return features

    def search_similar_claims(self, metadata: Dict[str, Any], limit: int = 5) -> List[Dict[str, Any]]:
        """
        Search for similar claims in Qdrant
        
        Args:
            metadata: Current claim metadata
            limit: Maximum number of similar claims to return
            
        Returns:
            List of similar claims with scores
        """
        try:
            # Create query embedding
            query_embedding = self._create_embedding_from_metadata(metadata)
            
            # Search in Qdrant
            search_result = self.qdrant_client.search(
                collection_name=self.collection_name,
                query_vector=query_embedding,
                limit=limit
            )
            
            similar_claims = []
            for result in search_result:
                similar_claims.append({
                    "score": result.score,
                    "vector_id": result.id,
                    "payload": result.payload
                })
            
            return similar_claims
            
        except Exception as e:
            print(f"Qdrant search failed: {e}")
            return []

    # Add all the missing helper methods that are called by analyze_new_claim
    def _get_icd10_mapping(self, conn, icd9_code):
        """Get ICD-10 mapping for ICD-9 code"""
        try:
            # Use the view that includes descriptions to get the best mapping
            query = """
            SELECT icd10_code, icd10_description
            FROM [_gems].[dbo].[vw_icd9_to_icd10_cm_mapping]
            WHERE icd9_code = ?
            ORDER BY 
                CASE 
                    WHEN icd10_code LIKE 'I10%' THEN 1  -- Essential hypertension first
                    WHEN icd10_code LIKE 'E11%' THEN 2  -- Type 2 diabetes second
                    WHEN icd10_code LIKE 'I25%' THEN 3  -- Coronary artery disease third
                    ELSE 4  -- Other codes last
                END,
                icd10_code
            """
            df = pd.read_sql(query, conn, params=[icd9_code])
            if not df.empty:
                return df.iloc[0]['icd10_code']
        except:
            pass
        return None

    def _get_ncci_data(self, conn, hcpcs_code):
        """Get NCCI data for HCPCS code"""
        try:
            query = """
            SELECT TOP 1 ptp_denial_reason, mue_threshold, mue_denial_type
            FROM [_ncci_].[dbo].[vw_NCCI_Daily_Denial_Alerts]
            WHERE procedure_code = ?
            """
            df = pd.read_sql(query, conn, params=[hcpcs_code])
            if not df.empty:
                return {
                    'ptp_denial_reason': df.iloc[0]['ptp_denial_reason'],
                    'mue_threshold': df.iloc[0]['mue_threshold'],
                    'mue_denial_type': df.iloc[0]['mue_denial_type']
                }
        except:
            pass
        return {}

    def _get_diagnosis_name(self, conn, icd10_code):
        """Get human-readable diagnosis name for ICD-10 code"""
        try:
            # Remove decimal points from ICD-10 code for database lookup
            clean_code = icd10_code.replace('.', '') if icd10_code else ''
            
            query = """
            SELECT TOP 1 description
            FROM [_gems].[dbo].[icd10cm_codes_2018_fixed]
            WHERE icd10_code = ?
            """
            df = pd.read_sql(query, conn, params=[clean_code])
            if not df.empty:
                return df.iloc[0]['description']
        except:
            pass
        return None

    def _get_procedure_name(self, conn, hcpcs_code):
        """Get human-readable procedure name for HCPCS code"""
        try:
            # Try to get procedure name from HCPCS master table first
            query = """
            SELECT TOP 1 long_description, short_description
            FROM [_ref].[dbo].[hcpcs_master]
            WHERE hcpcs_code = ?
            ORDER BY seqnum
            """
            df = pd.read_sql(query, conn, params=[hcpcs_code])
            if not df.empty:
                long_desc = df.iloc[0]['long_description']
                short_desc = df.iloc[0]['short_description']
                
                # Prefer short description if available and meaningful
                if short_desc and short_desc != 'None' and len(short_desc.strip()) > 5:
                    return short_desc.strip()
                elif long_desc:
                    # Clean up the long description (remove prefixes like "003", "004")
                    if len(long_desc) > 3 and long_desc[:3].isdigit():
                        long_desc = long_desc[3:]
                    return long_desc.strip()
        except:
            pass
        
        try:
            # Fallback: Try NCD HCPCS matches
            query = """
            SELECT TOP 1 thm.long_description
            FROM [_ncd].[dbo].[toc_hcpcs_matches] thm
            INNER JOIN [_ncd].[dbo].[ncd_trkg] nt
                ON TRY_CONVERT(FLOAT, nt.NCD_mnl_sect) = thm.section
            WHERE thm.hcpcs_code = ?
            """
            df = pd.read_sql(query, conn, params=[hcpcs_code])
            if not df.empty:
                description = df.iloc[0]['long_description']
                # Clean up the description (remove prefixes like "003", "004")
                if description and len(description) > 3 and description[:3].isdigit():
                    description = description[3:]
                return description.strip()
        except:
            pass
        
        # Final fallback: Return a generic description based on HCPCS code patterns
        if hcpcs_code:
            if hcpcs_code.startswith('2'):
                return f"Surgical procedure {hcpcs_code}"
            elif hcpcs_code.startswith('G'):
                return f"Healthcare service {hcpcs_code}"
            elif hcpcs_code.startswith('8'):
                return f"Laboratory test {hcpcs_code}"
            elif hcpcs_code.startswith('9'):
                return f"Medical service {hcpcs_code}"
            else:
                return f"Medical procedure {hcpcs_code}"
        
        return None

    def _get_ncd_data(self, conn, hcpcs_code, service_date):
        """Get NCD data for HCPCS code"""
        try:
            query = """
            SELECT TOP 1 
                nt.NCD_id,
                nt.NCD_mnl_sect_title,
                nt.NCD_efctv_dt,
                nt.NCD_trmntn_dt
            FROM [_ncd].[dbo].[ncd_trkg] nt
            INNER JOIN [_ncd].[dbo].[toc_hcpcs_matches] thm
                ON TRY_CONVERT(FLOAT, nt.NCD_mnl_sect) = thm.section
            WHERE thm.hcpcs_code = ?
            """
            df = pd.read_sql(query, conn, params=[hcpcs_code])
            if not df.empty:
                row = df.iloc[0]
                ncd_status = 'Unknown'
                if row['NCD_trmntn_dt'] and row['NCD_trmntn_dt'] != '':
                    ncd_status = 'Terminated'
                elif row['NCD_efctv_dt'] and row['NCD_efctv_dt'] != '':
                    ncd_status = 'Active'
                
                return {
                    'ncd_id': row['NCD_id'],
                    'ncd_title': row['NCD_mnl_sect_title'],
                    'ncd_status': ncd_status
                }
        except:
            pass
        return {}

    def _determine_lcd_coverage(self, conn, hcpcs_code, diagnosis_code):
        """
        Determine LCD coverage for diagnosis + procedure combination
        
        UPDATED: Now queries _article_ database for surgical LCD coverage
        Backup saved as: new_claim_analyzer1.py.backup_before_article_db_YYYYMMDD_HHMMSS
        
        Args:
            conn: Database connection
            hcpcs_code: CPT/HCPCS procedure code
            diagnosis_code: ICD-9 or ICD-10 diagnosis code
            
        Returns:
            'Y' if covered, 'N' if not covered
        """
        if not diagnosis_code or not hcpcs_code:
            return 'N'
        
        try:
            # Strategy 1: Check surgical LCD crosswalk in _article_ database for CPT codes
            if hcpcs_code and hcpcs_code[0].isdigit():  # Numeric CPT codes
                try:
                    query = """
                        SELECT TOP 1 coverage_status
                        FROM [_article_].[dbo].[vw_Surgical_LCD_Crosswalk]
                        WHERE cpt_hcpcs_code = ?
                          AND icd10_code = ?
                          AND coverage_status = 'COVERED'
                    """
                    df = pd.read_sql(query, conn, params=[hcpcs_code, diagnosis_code])
                    
                    if not df.empty and df['coverage_status'].iloc[0] == 'COVERED':
                        return 'Y'
                    
                    # If no exact match, assume covered for surgical procedures
                    # (absence from LCD doesn't mean not covered, just not specified)
                    return 'Y'
                    
                except Exception as e:
                    # If article database query fails, fall back to conservative approach
                    # Assume surgical CPT codes are covered unless explicitly excluded
                    return 'Y'
            
            # Strategy 2: For HCPCS codes (A-Z prefix), use enhanced whitelist
            # These are typically DME/supplies covered by _lcd database
            covered_codes = [
                # ICD-9 codes
                'V', 'Z',  # Preventive codes
                '25000', '25001', '25002', '25003', '25010', '25011', '25012', '25013',  # Diabetes
                '4010', '4011', '4019',  # Hypertension
                '2720', '2721', '2722', '2723', '2724',  # Lipid disorders
                '4140', '4141', '4148', '4149',  # Coronary artery disease
                '4280', '4281', '4282', '4283', '4284', '4289',  # Heart failure
                '49300', '49301', '49302', '49310', '49311', '49312', '49320', '49321', '49322', '49381', '49382', '49390', '49391',  # Asthma
                '496', '4910', '4911', '4912', '4918', '4919', '4920', '4928',  # COPD
                '5851', '5852', '5853', '5854', '5855', '5856', '5859',  # Chronic kidney disease
                '3310', '3311', '3312', '3313', '3314', '3315', '3316', '3317', '3318', '3319',  # Alzheimer's/dementia
                '340', '3410', '3411', '3412', '3413', '3414', '3415', '3416', '3417', '3418', '3419',  # Multiple sclerosis
                
                # ICD-10 codes
                'I10', 'I11', 'I12', 'I13', 'I15',  # Hypertension
                'E10', 'E11', 'E12', 'E13', 'E14',  # Diabetes
                'E78', 'E79',  # Lipid disorders
                'I25',  # Coronary artery disease
                'I50',  # Heart failure
                'J45', 'J46',  # Asthma
                'J40', 'J41', 'J42', 'J43', 'J44',  # COPD
                'N18',  # Chronic kidney disease
                'F01', 'F02', 'F03', 'G30',  # Alzheimer's/dementia
                'G35',  # Multiple sclerosis
                'Z00', 'Z01', 'Z02', 'Z03', 'Z04', 'Z05', 'Z06', 'Z07', 'Z08', 'Z09',  # Preventive codes
            ]
            
            # Check if code starts with any covered prefix
            for code in covered_codes:
                if diagnosis_code.startswith(code):
                    return 'Y'
            
            return 'N'
            
        except Exception as e:
            # If any error occurs, default to 'Y' to avoid false denials
            print(f"Warning: LCD coverage check failed for {hcpcs_code} + {diagnosis_code}: {e}")
            return 'Y'

    def _check_duplicate_procedures(self, hcpcs_code, all_procedures):
        """Check for duplicate procedure billing"""
        procedure_count = all_procedures.count(hcpcs_code)
        return {
            'is_duplicate': procedure_count > 1,
            'count': procedure_count,
            'message': f"Procedure {hcpcs_code} appears {procedure_count} times" if procedure_count > 1 else None
        }

    def _check_global_period_bundling(self, hcpcs_code, all_procedures, claim_data):
        """Check for global period bundling issues"""
        # Major surgical procedures that create global periods
        major_surgery_codes = ['27130', '27447', '27132', '27446', '27134', '27445']  # Hip/knee replacements
        
        # Procedures typically bundled under global period
        bundled_procedures = ['93000', '80053', '36415', '99284', '85018']  # ECG, labs, venipuncture, ED visit, CBC
        
        is_bundled = False
        bundling_reason = None
        
        # Check if this is a bundled procedure and there's a major surgery
        if hcpcs_code in bundled_procedures:
            for proc in all_procedures:
                if proc in major_surgery_codes:
                    is_bundled = True
                    bundling_reason = f"Procedure {hcpcs_code} may be bundled under global period of {proc}"
                    break
        
        return {
            'is_bundled': is_bundled,
            'reason': bundling_reason
        }

    def _check_prior_authorization(self, hcpcs_code, claim_data):
        """Check for prior authorization requirements"""
        # High-cost procedures requiring PA
        pa_required_codes = ['27130', '27447', '27132', '27446', '27134', '27445', 'G0299']  # Major surgeries and expensive procedures
        
        # Check if PA is present in claim data (this would need to be added to claim schema)
        pa_present = claim_data.get('prior_authorization', {}).get('approved', False)
        
        return {
            'pa_required': hcpcs_code in pa_required_codes,
            'pa_present': pa_present,
            'message': f"Prior authorization required for {hcpcs_code}" if hcpcs_code in pa_required_codes and not pa_present else None
        }

    def _check_provider_credentialing(self, claim_data):
        """Check for provider credentialing issues"""
        # This would typically check against a provider database
        # For now, we'll do basic validation
        provider_num = claim_data.get('PRVDR_NUM', '')
        
        # Basic validation - check if provider number looks valid
        credentialing_issue = False
        issue_reason = None
        
        if not provider_num or len(provider_num) < 6:
            credentialing_issue = True
            issue_reason = "Invalid or missing provider number"
        elif provider_num.startswith('999'):
            credentialing_issue = True
            issue_reason = "Provider number appears to be test/invalid"
        
        return {
            'credentialing_issue': credentialing_issue,
            'reason': issue_reason
        }

    def _check_modifier_requirements(self, hcpcs_code, claim_data):
        """Check for required modifiers"""
        # Procedures that typically require specific modifiers
        modifier_requirements = {
            '27130': ['50', '51'],  # Bilateral, multiple procedures
            '27447': ['50', '51'],  # Bilateral, multiple procedures
            '93000': ['26'],        # Professional component
            '80053': ['26']         # Professional component
        }
        
        # Check if modifiers are present (this would need to be added to claim schema)
        modifiers = claim_data.get('modifiers', {}).get(hcpcs_code, [])
        required_modifiers = modifier_requirements.get(hcpcs_code, [])
        
        missing_modifiers = []
        for req_mod in required_modifiers:
            if req_mod not in modifiers:
                missing_modifiers.append(req_mod)
        
        return {
            'modifier_required': len(required_modifiers) > 0,
            'modifier_present': len(missing_modifiers) == 0,
            'missing_modifiers': missing_modifiers,
            'message': f"Missing required modifiers: {', '.join(missing_modifiers)}" if missing_modifiers else None
        }

    def _check_frequency_limits(self, hcpcs_code, all_procedures, claim_data):
        """Check for frequency limits"""
        # Frequency limits for common procedures
        frequency_limits = {
            '93000': 1,  # ECG - once per day
            '80053': 1,  # Comprehensive metabolic panel - once per day
            '36415': 4,  # Venipuncture - max 4 per day
            '99284': 1   # ED visit - once per day
        }
        
        limit = frequency_limits.get(hcpcs_code, None)
        if limit:
            count = all_procedures.count(hcpcs_code)
            frequency_exceeded = count > limit
            return {
                'frequency_exceeded': frequency_exceeded,
                'count': count,
                'limit': limit,
                'message': f"Procedure {hcpcs_code} appears {count} times, limit is {limit}" if frequency_exceeded else None
            }
        
        return {
            'frequency_exceeded': False,
            'count': 0,
            'limit': None,
            'message': None
        }

    def _calculate_risk_analysis(self, dx_pos, dx_code, mapped_icd10, hcpcs_pos, hcpcs_code, ncci_data, ncd_data, lcd_covered, claim_data, all_procedures):
        """Calculate risk analysis for diagnosis-procedure combination"""
        
        # Check for duplicate procedures
        duplicate_check = self._check_duplicate_procedures(hcpcs_code, all_procedures)
        
        # Check for global period bundling
        global_period_check = self._check_global_period_bundling(hcpcs_code, all_procedures, claim_data)
        
        # Check for prior authorization requirements
        pa_check = self._check_prior_authorization(hcpcs_code, claim_data)
        
        # Check for provider credentialing issues
        provider_check = self._check_provider_credentialing(claim_data)
        
        # Check for modifier issues
        modifier_check = self._check_modifier_requirements(hcpcs_code, claim_data)
        
        # Check for frequency limits
        frequency_check = self._check_frequency_limits(hcpcs_code, all_procedures, claim_data)
        
        # Determine denial risk level (prioritize by severity)
        if duplicate_check['is_duplicate']:
            denial_risk_level = 'HIGH: Duplicate Procedure Billing'
        elif ncci_data.get('ptp_denial_reason') and hcpcs_pos == 1:
            denial_risk_level = 'HIGH: NCCI PTP Conflict'
        elif global_period_check['is_bundled']:
            denial_risk_level = 'HIGH: Global Period Bundling'
        elif lcd_covered == 'N' and dx_pos == 1:
            denial_risk_level = 'HIGH: Primary DX Not Covered'
        elif pa_check['pa_required'] and not pa_check['pa_present']:
            denial_risk_level = 'HIGH: Prior Authorization Missing'
        elif provider_check['credentialing_issue']:
            denial_risk_level = 'HIGH: Provider Credentialing Issue'
        elif modifier_check['modifier_required'] and not modifier_check['modifier_present']:
            denial_risk_level = 'MEDIUM: Required Modifier Missing'
        elif ncci_data.get('mue_threshold'):
            denial_risk_level = 'MEDIUM: MUE Risk'
        elif frequency_check['frequency_exceeded']:
            denial_risk_level = 'MEDIUM: Frequency Limit Exceeded'
        elif ncd_data.get('ncd_status') == 'Terminated':
            denial_risk_level = 'MEDIUM: NCD Terminated'
        elif lcd_covered == 'N' and dx_pos > 1:
            denial_risk_level = 'LOW: Secondary DX Not Covered'
        else:
            denial_risk_level = 'OK'
        
        # Calculate risk score (updated with new checks)
        base_score = 0
        if duplicate_check['is_duplicate']:
            base_score = 120  # Highest priority - duplicate billing
        elif ncci_data.get('ptp_denial_reason'):
            base_score = 100
        elif global_period_check['is_bundled']:
            base_score = 95  # High priority - bundling issue
        elif lcd_covered == 'N' and dx_pos == 1:
            base_score = 90
        elif pa_check['pa_required'] and not pa_check['pa_present']:
            base_score = 85  # High priority - missing PA
        elif provider_check['credentialing_issue']:
            base_score = 80  # High priority - credentialing
        elif modifier_check['modifier_required'] and not modifier_check['modifier_present']:
            base_score = 70  # Medium-high priority - missing modifier
        elif ncci_data.get('mue_threshold'):
            base_score = 60
        elif frequency_check['frequency_exceeded']:
            base_score = 55  # Medium priority - frequency limit
        elif ncd_data.get('ncd_status') == 'Terminated':
            base_score = 50
        elif lcd_covered == 'N' and dx_pos > 1:
            base_score = 30
        
        # Apply multipliers
        if hcpcs_pos == 1:
            multiplier = 1.5
        elif hcpcs_pos <= 5:
            multiplier = 1.2
        else:
            multiplier = 1.0
        
        denial_risk_score = base_score * multiplier
        
        # Determine risk category
        if ncci_data.get('ptp_denial_reason') and hcpcs_pos == 1:
            risk_category = 'CRITICAL'
        elif lcd_covered == 'N' and dx_pos == 1:
            risk_category = 'CRITICAL'
        elif ncci_data.get('mue_threshold') or ncd_data.get('ncd_status') == 'Terminated':
            risk_category = 'HIGH'
        elif lcd_covered == 'N' and dx_pos > 1:
            risk_category = 'MEDIUM'
        else:
            risk_category = 'LOW'
        
        # Determine action required
        if ncci_data.get('ptp_denial_reason') and hcpcs_pos == 1:
            action_required = 'IMMEDIATE: Fix PTP conflict or claim will be denied'
        elif lcd_covered == 'N' and dx_pos == 1:
            action_required = 'IMMEDIATE: Add covered diagnosis or claim will be rejected'
        elif ncci_data.get('mue_threshold'):
            action_required = 'REVIEW: Verify documentation supports units billed'
        elif ncd_data.get('ncd_status') == 'Terminated':
            action_required = 'REVIEW: Check if NCD termination affects coverage'
        elif lcd_covered == 'N' and dx_pos > 1:
            action_required = 'MONITOR: Secondary diagnosis not covered'
        else:
            action_required = 'NO ACTION: Claim appears compliant'
        
        # Determine business impact
        if ncci_data.get('ptp_denial_reason') and hcpcs_pos == 1:
            business_impact = 'FULL DENIAL: Primary procedure will be denied'
        elif lcd_covered == 'N' and dx_pos == 1:
            business_impact = 'FULL DENIAL: Entire claim will be rejected'
        elif ncci_data.get('mue_threshold'):
            business_impact = 'PARTIAL DENIAL: Units may be reduced'
        elif ncd_data.get('ncd_status') == 'Terminated':
            business_impact = 'COVERAGE RISK: May affect reimbursement'
        elif lcd_covered == 'N' and dx_pos > 1:
            business_impact = 'MINIMAL IMPACT: Secondary diagnosis issue'
        else:
            business_impact = 'NO IMPACT: Claim should process normally'
        
        return {
            'denial_risk_level': denial_risk_level,
            'denial_risk_score': round(denial_risk_score, 1),
            'risk_category': risk_category,
            'action_required': action_required,
            'business_impact': business_impact
        }

    def _generate_summary(self, results):
        """Generate claim summary"""
        if not results:
            return {}
        
        # Calculate summary statistics
        total_combinations = len(results)
        unique_procedures = len(set(r['hcpcs_code'] for r in results))
        unique_diagnoses = len(set(r['icd9_dgns_code'] for r in results))
        
        max_risk_score = max(r['denial_risk_score'] for r in results)
        avg_risk_score = sum(r['denial_risk_score'] for r in results) / len(results)
        
        # Count issues by category
        critical_issues = sum(1 for r in results if r['risk_category'] == 'CRITICAL')
        high_issues = sum(1 for r in results if r['risk_category'] == 'HIGH')
        medium_issues = sum(1 for r in results if r['risk_category'] == 'MEDIUM')
        low_issues = sum(1 for r in results if r['risk_category'] == 'LOW')
        ok_combinations = sum(1 for r in results if r['denial_risk_level'] == 'OK')
        
        # Determine decision and priority
        if critical_issues > 0:
            decision = 'DENY'
            priority = 'CRITICAL'
        elif high_issues > 0:
            decision = 'REVIEW'
            priority = 'HIGH'
        elif medium_issues > 0:
            decision = 'MONITOR'
            priority = 'MEDIUM'
        else:
            decision = 'APPROVE'
            priority = 'LOW'
        
        # Determine action required
        if critical_issues > 0:
            action_required = 'IMMEDIATE: Fix critical issues or claim will be denied'
            business_impact = 'HIGH: Full denial risk due to critical issues'
        elif high_issues > 0:
            action_required = 'REVIEW: Address high-risk issues before submission'
            business_impact = 'MEDIUM: Partial denial risk'
        elif medium_issues > 0:
            action_required = 'MONITOR: Review medium-risk issues'
            business_impact = 'LOW: Minor issues to monitor'
        else:
            action_required = 'NO ACTION: Claim appears compliant'
            business_impact = 'NO IMPACT: Claim should process normally'
        
        # Determine submission recommendation
        if critical_issues > 0:
            submission_recommendation = 'DO NOT SUBMIT - Fix critical issues first'
        elif high_issues > 0:
            submission_recommendation = 'REVIEW BEFORE SUBMISSION - Address high-risk issues'
        elif medium_issues > 0:
            submission_recommendation = 'SUBMIT WITH CAUTION - Monitor medium-risk issues'
        else:
            submission_recommendation = 'SUBMIT - Claim appears compliant'
        
        return {
            'CLM_ID': results[0]['CLM_ID'],
            'DESYNPUF_ID': results[0]['DESYNPUF_ID'],
            'clm_from_dt': results[0]['clm_from_dt'],
            'clm_thru_dt': results[0]['clm_thru_dt'],
            'PRVDR_NUM': results[0]['PRVDR_NUM'],
            'total_combinations': total_combinations,
            'unique_procedures': unique_procedures,
            'unique_diagnoses': unique_diagnoses,
            'max_risk_score': max_risk_score,
            'avg_risk_score': round(avg_risk_score, 1),
            'decision': decision,
            'priority': priority,
            'critical_issues': critical_issues,
            'high_issues': high_issues,
            'medium_issues': medium_issues,
            'low_issues': low_issues,
            'ok_combinations': ok_combinations,
            'action_required': action_required,
            'business_impact': business_impact,
            'submission_recommendation': submission_recommendation
        }

    def _generate_actionable_fixes(self, results):
        """Generate actionable fixes"""
        fixes = []
        fix_id = 1
        
        # Filter out OK combinations
        problematic_results = [r for r in results if r['denial_risk_level'] != 'OK']
        
        for result in problematic_results:
            # Generate specific fix based on issue type
            if 'Duplicate Procedure Billing' in result['denial_risk_level']:
                fix = f"Remove duplicate procedure {result['hcpcs_code']} or verify multiple units are justified"
            elif 'Global Period Bundling' in result['denial_risk_level']:
                fix = f"Verify procedure {result['hcpcs_code']} is not bundled under surgical global period"
            elif 'Prior Authorization Missing' in result['denial_risk_level']:
                fix = f"Obtain prior authorization for procedure {result['hcpcs_code']} before billing"
            elif 'Provider Credentialing Issue' in result['denial_risk_level']:
                fix = f"Verify provider credentials and NPI/TIN are valid and active"
            elif 'Required Modifier Missing' in result['denial_risk_level']:
                fix = f"Add required modifiers for procedure {result['hcpcs_code']}"
            elif 'Frequency Limit Exceeded' in result['denial_risk_level']:
                fix = f"Reduce frequency of procedure {result['hcpcs_code']} to within allowed limits"
            elif 'NCCI PTP Conflict' in result['denial_risk_level']:
                fix = f"Remove conflicting procedure or verify they can be billed together"
            elif 'Primary DX Not Covered' in result['denial_risk_level']:
                fix = f"Replace primary diagnosis {result['icd9_dgns_code']} with a diagnosis that justifies procedure {result['hcpcs_code']}"
            elif 'MUE Risk' in result['denial_risk_level']:
                fix = f"Verify documentation supports units billed for procedure {result['hcpcs_code']}"
            elif 'NCD Terminated' in result['denial_risk_level']:
                fix = f"Check if NCD termination affects coverage for procedure {result['hcpcs_code']}"
            elif 'Secondary DX Not Covered' in result['denial_risk_level']:
                fix = f"Review secondary diagnosis {result['icd9_dgns_code']} for procedure {result['hcpcs_code']}"
            else:
                fix = result['action_required']
            
            fixes.append({
                'fix_id': fix_id,
                'dx_code': result['icd9_dgns_code'],
                'diagnosis_name': result.get('diagnosis_name'),
                'hcpcs_code': result['hcpcs_code'],
                'procedure_name': result.get('procedure_name'),
                'issue': result['denial_risk_level'],
                'risk_score': result['denial_risk_score'],
                'fix': fix,
                'impact': result['business_impact'],
                'priority': result['risk_category']
            })
            fix_id += 1
        
        return fixes

    def _extract_metadata(self, claim_data, results, service_date, hcpcs_codes, dx_codes):
        """Extract comprehensive metadata for RAG models"""
        
        # Basic claim information
        claim_id = claim_data.get('CLM_ID', 'Unknown')
        patient_id = claim_data.get('DESYNPUF_ID', 'Unknown')
        provider_id = claim_data.get('PRVDR_NUM', 'Unknown')
        
        # Extract all procedure codes with positions
        all_cpt_codes = [code for _, code in hcpcs_codes]
        cpt_primary = all_cpt_codes[0] if all_cpt_codes else None
        cpt_secondary = all_cpt_codes[1:] if len(all_cpt_codes) > 1 else []
        
        # Extract all diagnosis codes
        all_icd_codes = [code for _, code in dx_codes]
        
        # Extract modifiers (if available in claim data)
        modifiers = []
        if 'modifiers' in claim_data:
            for proc_modifiers in claim_data['modifiers'].values():
                if isinstance(proc_modifiers, list):
                    modifiers.extend(proc_modifiers)
                else:
                    modifiers.append(proc_modifiers)
        modifiers = list(set(modifiers))  # Remove duplicates
        
        # Calculate units for each procedure
        units = {}
        for _, code in hcpcs_codes:
            units[code] = units.get(code, 0) + 1
        
        # Determine provider type based on provider ID patterns
        provider_type = self._determine_provider_type(provider_id)
        
        # Determine place of service (if available)
        place_of_service = claim_data.get('place_of_service', 'Unknown')
        
        # Extract payer information
        payer_name = claim_data.get('payer_name', 'Unknown')
        jurisdiction = claim_data.get('jurisdiction', 'Unknown')
        
        # Extract risk analysis metadata
        risk_metadata = self._extract_risk_metadata(results)
        
        # Extract issue categories
        issue_categories = self._extract_issue_categories(results)
        
        # Extract procedure categories
        procedure_categories = self._extract_procedure_categories(all_cpt_codes)
        
        # Extract diagnosis categories
        diagnosis_categories = self._extract_diagnosis_categories(all_icd_codes)
        
        # Extract billing patterns
        billing_patterns = self._extract_billing_patterns(results, all_cpt_codes)
        
        # Extract denial risk indicators
        denial_indicators = self._extract_denial_indicators(results)
        
        return {
            # Basic claim information
            "claim_id": claim_id,
            "patient_id": patient_id,
            "provider_id": provider_id,
            "service_date": service_date,
            
            # Payer information
            "payer_name": payer_name,
            "jurisdiction": jurisdiction,
            
            # Procedure information
            "cpt_primary": cpt_primary,
            "cpt_secondary": cpt_secondary,
            "all_cpt_codes": all_cpt_codes,
            "units": units,
            
            # Diagnosis information
            "icd_codes": all_icd_codes,
            "primary_diagnosis": all_icd_codes[0] if all_icd_codes else None,
            "secondary_diagnoses": all_icd_codes[1:] if len(all_icd_codes) > 1 else [],
            
            # Modifiers and billing
            "modifiers": modifiers,
            "provider_type": provider_type,
            "place_of_service": place_of_service,
            
            # Risk analysis metadata
            "risk_metadata": risk_metadata,
            
            # Issue categories
            "issue_categories": issue_categories,
            
            # Procedure categories
            "procedure_categories": procedure_categories,
            
            # Diagnosis categories
            "diagnosis_categories": diagnosis_categories,
            
            # Billing patterns
            "billing_patterns": billing_patterns,
            
            # Denial risk indicators
            "denial_indicators": denial_indicators,
            
            # Analysis metadata
            "analysis_timestamp": datetime.now().isoformat(),
            "total_issues": len([r for r in results if r['denial_risk_level'] != 'OK']),
            "critical_issues": len([r for r in results if r['risk_category'] == 'CRITICAL']),
            "high_issues": len([r for r in results if r['risk_category'] == 'HIGH']),
            "medium_issues": len([r for r in results if r['risk_category'] == 'MEDIUM']),
            "low_issues": len([r for r in results if r['risk_category'] == 'LOW']),
            "max_risk_score": max([r['denial_risk_score'] for r in results]) if results else 0,
            "avg_risk_score": sum([r['denial_risk_score'] for r in results]) / len(results) if results else 0
        }

    def _determine_provider_type(self, provider_id):
        """Determine provider type based on provider ID patterns"""
        if not provider_id:
            return "Unknown"
        
        # Common provider type patterns
        if provider_id.startswith('H'):
            return "Hospital"
        elif provider_id.startswith('F'):
            return "Facility"
        elif provider_id.startswith('P'):
            return "Physician"
        elif provider_id.startswith('G'):
            return "Group"
        elif provider_id.startswith('L'):
            return "Laboratory"
        elif provider_id.startswith('R'):
            return "Radiology"
        elif provider_id.startswith('A'):
            return "Ambulance"
        elif provider_id.startswith('D'):
            return "DME"
        else:
            return "Other"

    def _extract_risk_metadata(self, results):
        """Extract risk analysis metadata"""
        if not results:
            return {}
        
        risk_levels = [r['denial_risk_level'] for r in results]
        risk_scores = [r['denial_risk_score'] for r in results]
        
        return {
            "risk_levels": list(set(risk_levels)),
            "max_risk_score": max(risk_scores),
            "min_risk_score": min(risk_scores),
            "avg_risk_score": sum(risk_scores) / len(risk_scores),
            "high_risk_issues": len([r for r in results if 'HIGH' in r['denial_risk_level']]),
            "critical_issues": len([r for r in results if r['risk_category'] == 'CRITICAL']),
            "duplicate_issues": len([r for r in results if 'Duplicate' in r['denial_risk_level']]),
            "bundling_issues": len([r for r in results if 'Bundling' in r['denial_risk_level']]),
            "pa_issues": len([r for r in results if 'Prior Authorization' in r['denial_risk_level']]),
            "credentialing_issues": len([r for r in results if 'Credentialing' in r['denial_risk_level']]),
            "modifier_issues": len([r for r in results if 'Modifier' in r['denial_risk_level']]),
            "frequency_issues": len([r for r in results if 'Frequency' in r['denial_risk_level']])
        }

    def _extract_issue_categories(self, results):
        """Extract issue categories from results"""
        categories = {
            "duplicate_billing": [],
            "global_period_bundling": [],
            "prior_authorization": [],
            "provider_credentialing": [],
            "modifier_requirements": [],
            "frequency_limits": [],
            "ncci_conflicts": [],
            "coverage_issues": [],
            "mue_risks": [],
            "ncd_issues": []
        }
        
        for result in results:
            risk_level = result['denial_risk_level']
            if 'Duplicate' in risk_level:
                categories["duplicate_billing"].append(result['hcpcs_code'])
            elif 'Bundling' in risk_level:
                categories["global_period_bundling"].append(result['hcpcs_code'])
            elif 'Prior Authorization' in risk_level:
                categories["prior_authorization"].append(result['hcpcs_code'])
            elif 'Credentialing' in risk_level:
                categories["provider_credentialing"].append(result['hcpcs_code'])
            elif 'Modifier' in risk_level:
                categories["modifier_requirements"].append(result['hcpcs_code'])
            elif 'Frequency' in risk_level:
                categories["frequency_limits"].append(result['hcpcs_code'])
            elif 'NCCI' in risk_level:
                categories["ncci_conflicts"].append(result['hcpcs_code'])
            elif 'Not Covered' in risk_level:
                categories["coverage_issues"].append(result['hcpcs_code'])
            elif 'MUE' in risk_level:
                categories["mue_risks"].append(result['hcpcs_code'])
            elif 'NCD' in risk_level:
                categories["ncd_issues"].append(result['hcpcs_code'])
        
        # Remove duplicates
        for category in categories:
            categories[category] = list(set(categories[category]))
        
        return categories

    def _extract_procedure_categories(self, cpt_codes):
        """Extract procedure categories from CPT codes"""
        categories = {
            "surgery": [],
            "radiology": [],
            "laboratory": [],
            "evaluation_management": [],
            "anesthesia": [],
            "pathology": [],
            "medicine": [],
            "other": []
        }
        
        for code in cpt_codes:
            if code.startswith('2'):
                categories["surgery"].append(code)
            elif code.startswith('7'):
                categories["radiology"].append(code)
            elif code.startswith('8'):
                categories["laboratory"].append(code)
            elif code.startswith('9'):
                categories["evaluation_management"].append(code)
            elif code.startswith('0'):
                categories["anesthesia"].append(code)
            elif code.startswith('8'):
                categories["pathology"].append(code)
            elif code.startswith('9'):
                categories["medicine"].append(code)
            else:
                categories["other"].append(code)
        
        return categories

    def _extract_diagnosis_categories(self, icd_codes):
        """Extract diagnosis categories from ICD codes"""
        categories = {
            "musculoskeletal": [],
            "cardiovascular": [],
            "endocrine": [],
            "respiratory": [],
            "gastrointestinal": [],
            "neurological": [],
            "mental_health": [],
            "infectious_disease": [],
            "neoplasms": [],
            "other": []
        }
        
        for code in icd_codes:
            if code.startswith('M'):
                categories["musculoskeletal"].append(code)
            elif code.startswith('I'):
                categories["cardiovascular"].append(code)
            elif code.startswith('E'):
                categories["endocrine"].append(code)
            elif code.startswith('J'):
                categories["respiratory"].append(code)
            elif code.startswith('K'):
                categories["gastrointestinal"].append(code)
            elif code.startswith('G'):
                categories["neurological"].append(code)
            elif code.startswith('F'):
                categories["mental_health"].append(code)
            elif code.startswith('A') or code.startswith('B'):
                categories["infectious_disease"].append(code)
            elif code.startswith('C') or code.startswith('D'):
                categories["neoplasms"].append(code)
            else:
                categories["other"].append(code)
        
        return categories

    def _extract_billing_patterns(self, results, cpt_codes):
        """Extract billing patterns from results"""
        patterns = {
            "duplicate_procedures": [],
            "high_cost_procedures": [],
            "bundled_procedures": [],
            "frequent_procedures": [],
            "unusual_combinations": []
        }
        
        # High-cost procedures
        high_cost_codes = ['27130', '27447', '27132', '27446', '27134', '27445', 'G0299']
        patterns["high_cost_procedures"] = [code for code in cpt_codes if code in high_cost_codes]
        
        # Duplicate procedures
        from collections import Counter
        procedure_counts = Counter(cpt_codes)
        patterns["duplicate_procedures"] = [code for code, count in procedure_counts.items() if count > 1]
        
        # Bundled procedures
        bundled_codes = ['93000', '80053', '36415', '99284', '85018']
        patterns["bundled_procedures"] = [code for code in cpt_codes if code in bundled_codes]
        
        # Frequent procedures (appearing multiple times)
        patterns["frequent_procedures"] = [code for code, count in procedure_counts.items() if count > 1]
        
        return patterns

    def _extract_denial_indicators(self, results):
        """Extract denial risk indicators"""
        indicators = {
            "high_denial_risk": False,
            "critical_issues_present": False,
            "duplicate_billing_risk": False,
            "bundling_risk": False,
            "pa_required": False,
            "credentialing_issues": False,
            "modifier_issues": False,
            "frequency_issues": False,
            "coverage_issues": False,
            "ncci_conflicts": False
        }
        
        for result in results:
            risk_level = result['denial_risk_level']
            if 'HIGH' in risk_level or 'CRITICAL' in risk_level:
                indicators["high_denial_risk"] = True
            if result['risk_category'] == 'CRITICAL':
                indicators["critical_issues_present"] = True
            if 'Duplicate' in risk_level:
                indicators["duplicate_billing_risk"] = True
            if 'Bundling' in risk_level:
                indicators["bundling_risk"] = True
            if 'Prior Authorization' in risk_level:
                indicators["pa_required"] = True
            if 'Credentialing' in risk_level:
                indicators["credentialing_issues"] = True
            if 'Modifier' in risk_level:
                indicators["modifier_issues"] = True
            if 'Frequency' in risk_level:
                indicators["frequency_issues"] = True
            if 'Not Covered' in risk_level:
                indicators["coverage_issues"] = True
            if 'NCCI' in risk_level:
                indicators["ncci_conflicts"] = True
        
        return indicators

# Example usage
if __name__ == "__main__":
    # Example claim data
    claim_data = {
        "CLM_ID": "123456789012345",
        "DESYNPUF_ID": "000ABC123DEF456",
        "CLM_FROM_DT": "20240115",
        "CLM_THRU_DT": "20240115",
        "PRVDR_NUM": "12345AB",
        "diagnosis_codes": {
            "ICD9_DGNS_CD_1": "25000",
            "ICD9_DGNS_CD_2": "4019"
        },
        "procedure_codes": {
            "HCPCS_CD_1": "99213",
            "HCPCS_CD_2": "80053"
        }
    }
    
    analyzer = NewClaimAnalyzer()
    result = analyzer.analyze_new_claim(claim_data)
    
    print(json.dumps(result, indent=2, default=str))