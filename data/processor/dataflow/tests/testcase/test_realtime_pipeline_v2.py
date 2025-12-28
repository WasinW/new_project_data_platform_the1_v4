import unittest
import logging
from unittest.mock import patch, MagicMock
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

# Import components ของคุณ
from dataflow_common.orchestrator import Orchestrator
from dataflow_common.config import PipelineConfig
# Import BaseStep เพื่อใช้สร้าง Mock Step
from dataflow_common.core import BaseStep

# --- 1. Helper Class: Mock Source Step ---
# ต้องประกาศ Class นี้ก่อน เพื่อเอาไปใส่ใน Registry
class MockSourceStep(BaseStep):
    """Step ปลอมสำหรับส่งข้อมูล Test เข้าไปใน Pipeline (แทนการอ่านจาก PubSub)"""
    def execute(self, p):
        # ดึงข้อมูลที่เตรียมไว้จาก params
        data = self.spec.get('params', {}).get('data', [])
        # สร้าง PCollection จากข้อมูลนั้น
        return p | f"CreateMockData_{self.step_id}" >> beam.Create(data)

# --- 2. Helper Class: ExistingPipelineContext ---
# ตัวช่วยเพื่อป้องกันไม่ให้ Orchestrator ปิด Pipeline ก่อนเวลาอันควร
class ExistingPipelineContext:
    def __init__(self, p):
        self.p = p

    def __enter__(self):
        return self.p

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Return False เพื่อบอกว่าไม่ได้จัดการ exception (ถ้ามี)
        # Key Point: ไม่เรียก self.p.run() หรือ self.p.__exit__()
        # ปล่อยให้ TestPipeline ข้างนอกสุดเป็นคนจัดการ
        return False

# --- 3. Main Test Class ---
class TestRealtimePipelineFixed(unittest.TestCase):
    
    def setUp(self):
        # Setup Config โดยใช้ Step 'MockSource' ที่เราสร้างขึ้น
        self.test_config = PipelineConfig(
            name="test_realtime_pipeline",
            mode="streaming",
            term="realtime",
            # {'payload':b'{"personaId": "P001", "name": "Somchai", "score": 100}'},
            # {'payload':b'{"personaId": "P002", "name": "Somsri", "score": 200}'},

            plan=[
                # Step 1: Mock Source (ส่งข้อมูลเข้า)
                {
                    "step": "MockSource", 
                    "id": "input", 
                    "params": {
                        "data": [
                            b'{"payload": {"personaId": "P001", "name": "Somchai", "score": 100}}',
                            b'{"payload": {"personaId": "P002", "name": "Somsri", "score": 200}}',
                            # {"invalid": "data"} # ข้อมูลเสีย (ถ้าต้องการเทส case กรองออก)

                        ]
                    }
                },
                # Step 2: Step จริงที่คุณต้องการเทส (สมมติว่าเป็น ExtractPersonas)
                # (คุณอาจต้องเปลี่ยนชื่อ 'ExtractPersonas' ให้ตรงกับใน registry.py ของคุณ)
                {
                    "step": "ExtractPersonas", 
                    "id": "extracted_personas", 
                    "params": {"input": "input"}
                },
            ]
        )

    # Patch Registry ให้รู้จัก 'MockSource'
    @patch.dict('dataflow_common.registry.STEP_REGISTRY', {"MockSource": MockSourceStep})
    def test_pipeline_end_to_end(self):
        print("\n[TEST] Running Pipeline Integration Test...")
        
        # 1. สร้าง TestPipeline ไว้ข้างนอกสุด
        with TestPipeline() as p:
            
            # 2. Patch beam.Pipeline ให้ return Context หลอกของเรา
            # เมื่อ Orchestrator เรียก with beam.Pipeline(): มันจะได้ 'p' ตัวเดิมไปใช้
            with patch('apache_beam.Pipeline', side_effect=lambda *args, **kwargs: ExistingPipelineContext(p)):
                
                # 3. รัน Orchestrator (มันจะสร้าง Graph ลงใน 'p' แต่ยังไม่รันจริง)
                orchestrator = Orchestrator(self.test_config)
                final_state = orchestrator.run()
                
                # 4. ตรวจสอบ Output ใน Graph
                if 'extracted_personas' not in final_state:
                     # Debug: ปริ้นดูว่ามี key อะไรบ้างใน state ถ้าไม่เจอที่ต้องการ
                    print(f"DEBUG: State keys -> {list(final_state.keys())}")
                    self.fail("Step output 'extracted_personas' not found in state")

                actual_pcoll = final_state['extracted_personas']
                
                # 5. กำหนดผลลัพธ์ที่คาดหวัง
                expected_data = [
                    {"personaId": "P001"},
                    {"personaId": "P002"}
                ]
                
                # 6. ใส่ Assertion (ยังไม่ทำงานทันที จะทำงานตอนจบ with TestPipeline)
                assert_that(actual_pcoll, equal_to(expected_data))
        
        print("[SUCCESS] Pipeline test finished successfully")