#!/usr/bin/env python3
"""
MongoDB 메타데이터를 DLT-META 온보딩 JSON으로 변환하는 스크립트

사용법:
    # MongoDB에서 직접 변환
    python3 mongo_to_dltmeta_converter.py \
        --mongo-uri "mongodb://username:password@host:port/pipeline_meta" \
        --pipeline-name sdppoc_gdevdb02_item_goods_option_bronze

    # JSON 파일에서 변환 (MongoDB 없이)
    python3 mongo_to_dltmeta_converter.py \
        --input sample_pipeline_meta.json \
        --output-dir conf/

    # 환경변수 사용
    export MONGO_URI="mongodb://..."
    python3 mongo_to_dltmeta_converter.py --pipeline-name my_pipeline
"""

import json
import argparse
import os
import sys
from typing import Dict, Any, List, Optional
from pathlib import Path


class MongoToDltMetaConverter:
    """레거시 MongoDB 메타데이터를 DLT-META 포맷으로 변환"""
    
    def __init__(self, mongo_uri: Optional[str] = None, connection_info: Optional[Dict] = None):
        """
        Args:
            mongo_uri: MongoDB 연결 URI (선택)
            connection_info: 연결 정보 딕셔너리 (MongoDB 대신 사용 가능)
        """
        self.mongo_uri = mongo_uri
        self.connection_info = connection_info or {}
        self.client = None
        self.db = None
        
        if mongo_uri:
            try:
                from pymongo import MongoClient
                self.client = MongoClient(mongo_uri)
                self.db = self.client['pipeline_meta']
                print("✅ Connected to MongoDB")
            except ImportError:
                print("⚠️  pymongo not installed. Install with: pip install pymongo")
                print("   Falling back to JSON input mode")
            except Exception as e:
                print(f"⚠️  MongoDB connection failed: {e}")
                print("   Falling back to JSON input mode")
    
    def convert_pipeline(self, pipeline_meta: Dict[str, Any]) -> Dict[str, Any]:
        """
        파이프라인 메타데이터를 DLT-META 포맷으로 변환
        
        Args:
            pipeline_meta: MongoDB pipeline_info 문서 또는 JSON
            
        Returns:
            DLT-META 온보딩 JSON
        """
        pipeline_name = pipeline_meta.get("pipeline_name", "unknown")
        
        print(f"\n🔄 Converting pipeline: {pipeline_name}")
        print(f"   Active: {pipeline_meta.get('active', False)}")
        print(f"   Description: {pipeline_meta.get('description', 'N/A')}")
        
        # 기본 설정
        data_flow_config = {
            "data_flow_id": self._generate_data_flow_id(pipeline_name),
            "data_flow_group": self._extract_group_name(pipeline_name),
            "source_system": self._extract_source_system(pipeline_meta),
            "source_format": "jdbc",
        }
        
        stages = pipeline_meta.get("stages", [])
        
        if len(stages) == 0:
            raise ValueError(f"Pipeline '{pipeline_name}' has no stages")
        
        # Stage 1: Source 정의
        stage_1 = stages[0]
        data_flow_config["source_details"] = self._convert_source(stage_1.get("source", {}))
        
        # Bronze reader options
        read_options = stage_1.get("source", {}).get("read_options", {})
        if read_options:
            data_flow_config["bronze_reader_options"] = {}
            if "fetchsize" in read_options:
                data_flow_config["bronze_reader_options"]["fetchsize"] = str(read_options["fetchsize"])
        
        # Bronze 설정
        bronze_target = self._find_bronze_target(stages)
        if bronze_target:
            data_flow_config.update(self._convert_bronze_target(bronze_target))
        
        # Silver 설정
        if len(stages) > 1:
            silver_config = self._convert_silver_stages(stages[1:], pipeline_name)
            data_flow_config.update(silver_config)
        
        return data_flow_config
    
    def convert_from_mongodb(self, pipeline_name: str) -> Dict[str, Any]:
        """MongoDB에서 파이프라인 정보를 가져와 변환"""
        if not self.db:
            raise RuntimeError("MongoDB connection not available. Use JSON input mode instead.")
        
        pipeline_meta = self.db['pipeline_info'].find_one({"pipeline_name": pipeline_name})
        
        if not pipeline_meta:
            raise ValueError(f"Pipeline '{pipeline_name}' not found in MongoDB")
        
        return self.convert_pipeline(pipeline_meta)
    
    def _generate_data_flow_id(self, pipeline_name: str) -> str:
        """파이프라인 이름에서 data_flow_id 생성"""
        parts = pipeline_name.split("_")
        if parts[-1] in ["bronze", "silver"]:
            parts = parts[:-1]
        
        # 카운터 형식으로 ID 생성
        id_num = "001"
        return f"{parts[-1]}_{id_num}"
    
    def _extract_group_name(self, pipeline_name: str) -> str:
        """파이프라인 이름에서 그룹 추출"""
        parts = pipeline_name.split("_")
        if len(parts) >= 3:
            return f"{parts[0]}_{parts[2]}"
        return "default_group"
    
    def _extract_source_system(self, pipeline_meta: Dict[str, Any]) -> str:
        """Source 시스템 이름 추출"""
        stages = pipeline_meta.get("stages", [])
        if stages:
            source = stages[0].get("source", {})
            connection_name = source.get("connection_name", "")
            if connection_name:
                return f"{connection_name}_mssql"
        return "unknown_source"
    
    def _convert_source(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """레거시 Source를 DLT-META source_details로 변환"""
        source_type = source.get("source_type", "")
        connection_name = source.get("connection_name", "")
        
        if source_type == "rdb":
            # Secret Scope는 환경에 따라 설정
            secret_scope = os.getenv("JDBC_SECRET_SCOPE", "pipeline-cred-baikalx")
            
            source_details = {
                "jdbc_driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "jdbc_url_secret_scope": secret_scope,
                "jdbc_url_secret_key": f"{connection_name}_jdbc_url",
                "jdbc_user_secret_scope": secret_scope,
                "jdbc_user_secret_key": f"{connection_name}_username",
                "jdbc_password_secret_scope": secret_scope,
                "jdbc_password_secret_key": f"{connection_name}_password",
            }
            
            # Prepare Query
            prepare_query = source.get("prepare_query", "")
            if prepare_query:
                source_details["prepare_query"] = prepare_query.strip()
            
            # Source Query
            source_query = source.get("query", "").strip()
            if source_query:
                source_details["source_query"] = source_query
            
            # Partition configuration
            partition_col = source.get("partition_column")
            if partition_col:
                source_details["partition_column"] = partition_col
                source_details["num_partitions"] = str(source.get("num_partitions", "10"))
                source_details["lower_bound"] = str(source.get("lower_bound", "0"))
                source_details["upper_bound"] = str(source.get("upper_bound", "10000000"))
            
            return source_details
        
        elif source_type == "sparksql":
            return {
                "source_format": "delta",
                "source_query": source.get("query", "").strip()
            }
        
        elif source_type == "deltatable":
            return {
                "source_format": "delta",
                "source_table": source.get("name", "")
            }
        
        return {}
    
    def _find_bronze_target(self, stages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Bronze target 찾기"""
        for stage in stages:
            target = stage.get("target", {})
            target_name = target.get("name", "")
            
            if target.get("target_type") == "tempview":
                continue
            
            if "bronze" in target_name.lower() or target.get("target_type") in ["delta", "merge"]:
                return target
        
        return None
    
    def _convert_bronze_target(self, target: Dict[str, Any]) -> Dict[str, Any]:
        """Bronze Target 변환"""
        target_name = target.get("name", "")
        
        # catalog.schema.table 파싱
        parts = target_name.split(".")
        catalog = parts[0] if len(parts) >= 3 else "default_catalog"
        database = parts[1] if len(parts) >= 3 else parts[0] if len(parts) >= 2 else "default_db"
        table = parts[-1]
        
        bronze_config = {
            "bronze_catalog_dev": catalog,
            "bronze_database_dev": database,
            "bronze_table": table,
            "bronze_table_properties": {
                "pipelines.autoOptimize.managed": "true",
                "pipelines.reset.allowed": "false",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "delta.columnMapping.mode": "name"
            }
        }
        
        # Cluster by columns
        cluster_by = target.get("cluster_by") or target.get("clusterby")
        if cluster_by:
            if isinstance(cluster_by, str):
                bronze_config["bronze_cluster_by"] = [cluster_by]
            else:
                bronze_config["bronze_cluster_by"] = cluster_by
        
        # Partition columns
        partition_cols = target.get("partition_columns") or target.get("partitioncolumns")
        if partition_cols:
            bronze_config["bronze_partition_columns"] = partition_cols
        
        # Write mode
        mode = target.get("mode", "append")
        bronze_config["append_flow_type"] = "overwrite" if mode == "overwrite" else "append"
        
        return bronze_config
    
    def _convert_silver_stages(self, stages: List[Dict[str, Any]], pipeline_name: str) -> Dict[str, Any]:
        """Silver Stages 변환"""
        silver_config = {}
        transformations = []
        
        for idx, stage in enumerate(stages):
            source = stage.get("source", {})
            target = stage.get("target", {})
            
            # Source Query를 transformation으로
            if source.get("source_type") == "sparksql":
                query = source.get("query", "")
                if query:
                    mode = "filter" if "WHERE" in query.upper() else "transform"
                    input_table = "bronze" if idx == 0 else "filtered"
                    
                    transformations.append({
                        "sequence": len(transformations) + 1,
                        "mode": mode,
                        "layer": "silver",
                        "table_name": target.get("name", "").split(".")[-1],
                        "input_table": input_table,
                        "query": query.strip()
                    })
            
            # Silver Target 설정
            if target.get("target_type") in ["delta", "merge"]:
                target_name = target.get("name", "")
                parts = target_name.split(".")
                
                silver_config["silver_catalog_dev"] = parts[0] if len(parts) >= 3 else "default_catalog"
                silver_config["silver_database_dev"] = parts[1] if len(parts) >= 3 else parts[0] if len(parts) >= 2 else "default_db"
                silver_config["silver_table"] = parts[-1]
                
                # Cluster by
                cluster_by = target.get("cluster_by") or target.get("clusterby")
                if cluster_by:
                    if isinstance(cluster_by, str):
                        silver_config["silver_cluster_by"] = [cluster_by]
                    else:
                        silver_config["silver_cluster_by"] = cluster_by
                
                # Partition columns
                partition_cols = target.get("partition_columns") or target.get("partitioncolumns")
                if partition_cols:
                    silver_config["silver_partition_columns"] = partition_cols
                
                silver_config["silver_table_properties"] = {
                    "pipelines.autoOptimize.managed": "true",
                    "pipelines.reset.allowed": "false",
                    "delta.autoOptimize.optimizeWrite": "true",
                    "delta.autoOptimize.autoCompact": "true",
                    "delta.columnMapping.mode": "name"
                }
        
        # Transformation을 파일로 저장
        if transformations:
            # 파일명만 지정 (경로는 generate_onboarding.py가 처리)
            transformation_file = f"silver_transformations_{pipeline_name}.json"
            silver_config["silver_transformation_json_dev"] = transformation_file
            
            # Transformation JSON 저장
            self._save_transformations(transformation_file, transformations)
        
        return silver_config
    
    def _save_transformations(self, filename: str, transformations: List[Dict[str, Any]]):
        """Transformation을 별도 JSON 파일로 저장"""
        output = {
            "silver_transformations": transformations
        }
        
        # conf 디렉토리에 저장
        conf_dir = Path(__file__).parent / "conf"
        conf_dir.mkdir(exist_ok=True)
        output_path = conf_dir / filename
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=4, ensure_ascii=False)
        
        print(f"   → Saved transformations: {output_path}")
    
    def close(self):
        """MongoDB 연결 종료"""
        if self.client:
            self.client.close()


def main():
    parser = argparse.ArgumentParser(
        description="Convert MongoDB pipeline metadata to DLT-META format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # MongoDB에서 직접 변환
  python3 mongo_to_dltmeta_converter.py \\
    --mongo-uri "mongodb://user:pass@host:port/pipeline_meta" \\
    --pipeline-name sdppoc_gdevdb02_item_goods_option_bronze

  # JSON 파일에서 변환
  python3 mongo_to_dltmeta_converter.py \\
    --input sample_pipeline.json \\
    --output conf/onboarding_my_pipeline.template.json

  # 환경변수 사용
  export MONGO_URI="mongodb://..."
  export JDBC_SECRET_SCOPE="my-secret-scope"
  python3 mongo_to_dltmeta_converter.py --pipeline-name my_pipeline
        """
    )
    
    parser.add_argument("--mongo-uri", 
                        default=os.getenv("MONGO_URI"),
                        help="MongoDB connection URI (or set MONGO_URI env var)")
    parser.add_argument("--pipeline-name", 
                        help="Pipeline name to convert from MongoDB")
    parser.add_argument("--input", 
                        help="Input JSON file (pipeline_info document)")
    parser.add_argument("--output", 
                        help="Output JSON file path (default: conf/onboarding_{pipeline_name}.template.json)")
    parser.add_argument("--output-dir",
                        default="conf",
                        help="Output directory (default: conf)")
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.pipeline_name and not args.input:
        parser.error("Either --pipeline-name or --input is required")
    
    # Create converter
    converter = MongoToDltMetaConverter(mongo_uri=args.mongo_uri)
    
    try:
        if args.input:
            # JSON 파일에서 읽기
            print(f"📖 Reading from: {args.input}")
            with open(args.input, 'r', encoding='utf-8') as f:
                pipeline_meta = json.load(f)
            
            config = converter.convert_pipeline(pipeline_meta)
            pipeline_name = pipeline_meta.get("pipeline_name", "unknown")
        
        elif args.pipeline_name:
            # MongoDB에서 읽기
            config = converter.convert_from_mongodb(args.pipeline_name)
            pipeline_name = args.pipeline_name
        
        # Output 파일 결정
        if args.output:
            output_file = Path(args.output)
        else:
            output_dir = Path(__file__).parent / args.output_dir
            output_dir.mkdir(exist_ok=True)
            output_file = output_dir / f"onboarding_{pipeline_name}.template.json"
        
        # 저장
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump([config], f, indent=4, ensure_ascii=False)
        
        print(f"\n✅ Successfully converted!")
        print(f"   Output: {output_file}")
        print(f"\n💡 Next steps:")
        print(f"   1. Review and edit: {output_file}")
        print(f"   2. Generate environment-specific file:")
        print(f"      python3 generate_onboarding.py --env dev")
        
        return 0
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        converter.close()


if __name__ == "__main__":
    sys.exit(main())
