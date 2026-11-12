"""已有原字段摘要、文件身份与配置边界。"""
import json,tempfile,unittest
from pathlib import Path
from lake_data.io import load,asset,integer,unique
from lake_data.domain import summarize

class MaterialTests(unittest.TestCase):
    def test_normal(self):
        v,digest=load('data/normal.json');value=summarize(v,Path('data'))
        self.assertEqual(len(digest),64);self.assertEqual(value,summarize(json.loads(json.dumps(v)),Path('data')))
        self.assertEqual(value['gap'],10);self.assertEqual(value['kinds']['watermark'],2)
    def test_bad_config(self):
        with self.assertRaises((ValueError,KeyError,TypeError)):summarize(load('data/invalid-config.json')[0],Path('data'))
    def test_source_variants(self):
        for p in Path('data').glob('*.json'):
            if p.name.startswith('invalid-'):continue
            with self.subTest(file=p.name):self.assertIsNotNone(summarize(load(p)[0],p.parent))
    def test_asset_identity(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'raw';p.write_bytes(b'abc')
            with self.assertRaises(ValueError):asset(d,{'path':'raw','bytes':3,'sha256':'bad'})
            with self.assertRaises(ValueError):asset(d,{'path':'../other','bytes':0,'sha256':'none'})
    def test_numeric_and_id(self):
        for x in (True,-1,1.2):
            with self.assertRaises(ValueError):integer(x)
        with self.assertRaises(ValueError):unique([{'id':'x'},{'id':'x'}],'id')
    def test_json_shape(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'资料.json'
            for raw in ('[]','null','{"v":Infinity}'):
                p.write_text(raw,encoding='utf8')
                with self.assertRaises(ValueError):load(p)
