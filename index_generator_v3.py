import math
import pandas as pd

class Index_generator():
    def __init__(self):
        self.index_name = "nutrient_ver_03"
        self.settings ={
            "settings": {
                "number_of_shards": 1,
                "analysis": {
                    "analyzer": {
                        "korean": {
                            "type": "custom",
                            "tokenizer": "nori_tokenizer",
                            "decompound_mode": "mixed"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "code": {"type": "keyword"}, # 식품코드
                    "name": {"type": "text", "analyzer": "korean"}, # 식품명
                    "manufacturer": {"type": "keyword"}, # 제조사명
                    "distributor": {"type": "keyword"}, # 유통업체명
                    "data_classification_code": {"type": "keyword"}, # 데이터구분코드
                    "data_classification_name": {"type": "keyword"}, # 데이터구분명
                    "food_classification_code": {"type": "keyword"}, # 식품대분류코드
                    "food_classification_name": {"type": "keyword"}, # 식품대분류명
                    "food_representative_code": {"type": "keyword"}, # 대표식품코드
                    "food_representative_name": {"type": "keyword"}, # 대표식품명
                    "food_category_code": {"type": "keyword"}, # 식품중분류코드
                    "food_category_name": {"type": "keyword"}, # 식품중분류명
                    "food_subcategory_code": {"type": "keyword"}, # 식품소분류코드
                    "food_subcategory_name": {"type": "keyword"}, # 식품소분류명
                    "weight_original": {"type": "keyword"}, # 식품중량
                    "nutrient_reference_weight_original": {"type": "keyword"}, # 영양성분함량기준량
                    "energy": {"type": "double"}, # 에너지(kcal)
                    "protein": {"type": "double"}, # 단백질(g)
                    "fat": {"type": "double"}, # 지방(g)
                    "carbohydrate": {"type": "double"}, # 탄수화물(g)
                    "sugar": {"type": "double"}, # 당류(g)
                    # 후 가공 데이터
                    "text_for_search": {"type": "text", "analyzer": "korean"},
                    "text_for_client": {"type": "text", "analyzer": "korean"}, # 클라이언트에 보여줄 이름
                    "weight_unit": {"type": "keyword"},
                    "weight": {"type": "double"}, # 식품중량
                    "nutrient_reference_weight": {"type": "double"}, # 영양성분함량기준량
                    "classification_type": {"type": "keyword"}, # 가공 후 데이터구분코드
                }
            }
        }
    
    def remove_suffix(self, value):
        if value == None: return None
        if value.endswith('g'): return float(value[:-1])
        if value.endswith('ml'): return float(value[:-2])
        raise Exception("Cannot remove last suffix from {}".format(value))
    
    def nan2None(self, value):
        if type(value) is str: return value
        if math.isnan(value): return None
        return value

    def parse_data2doc(self, data):
        try:
            unit = ''
            data = list(map(self.nan2None, data))

            if data[0] == "P109-801080100-0814": return None

            if data[14] != None and data[14].endswith('g'): unit = 'g'
            elif data[14] != None and data[14].endswith('ml'): unit = 'ml'
            elif data[15] != None and data[15].endswith('g'): unit = 'g'
            elif data[15] != None and data[15].endswith('ml'): unit = 'ml'
            else: raise Exception("Cannot find unit from {}".format(data[14]))


            return {
                "code": data[0],
                "name": data[1],
                "manufacturer": data[2],
                "distributor": data[3],
                "data_classification_code": data[4],
                "data_classification_name": data[5],
                "food_classification_code": data[6],
                "food_classification_name": data[7],
                "food_representative_code": data[8],
                "food_representative_name": data[9],
                "food_category_code": data[10],
                "food_category_name": data[11],
                "food_subcategory_code": data[12],
                "food_subcategory_name": data[13],
                "weight_original": data[14],
                "nutrient_reference_weight_original": data[15],
                "energy": data[16],
                "protein": data[17],
                "fat": data[18],
                "carbohydrate": data[19],
                "sugar": data[20],
                "text_for_search": "{} / {}".format(data[2] if data[2] != None else '', data[1] if data[1] != None else ''),
                "text_for_client": "",
                "weight_unit": unit,
                "weight": self.remove_suffix(data[14]),
                "nutrient_reference_weight": self.remove_suffix(data[15]),
                "classification_type": "P",
            }
        except Exception as e:
            print(data)
            print(e)
            return None
    
    def get_parsed_docs(self):
        dataset = pd.read_csv('datasets/nutrient_dataset_ver_231004.csv', header=0).values.tolist()
        return list(map(self.parse_data2doc, dataset))
