import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

print(f"start ...")

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .master("local") \
    .enableHiveSupport() \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------- create gdm_m03_item_sku_act ----------------------------------------
data = [
    Row(item_sku_id="100026588873", sku_name="木桐酒庄", barndname_cn="木桐酒庄",item_first_cate_cd="12259",dt="2024-12-26",create_timestamp="1736911578491",ord_tm="2024-01-01 12:10:30.0")
    # Row(item_sku_id="100026588873", sku_name="木桐酒庄111", barndname_cn="木桐酒庄",item_first_cate_cd="12259",dt="2024-12-26",create_timestamp="1736911578493"),
    # Row(item_sku_id="100026588873", sku_name="木桐酒庄", barndname_cn="木桐酒庄",item_first_cate_cd="12259",dt="2024-12-26",create_timestamp="1736911578492"),
    # Row(item_sku_id="100145382842", sku_name="都夏美隆", barndname_cn="都夏美隆",item_first_cate_cd="12259",dt="2024-12-26",create_timestamp="1736911578493"),
    # Row(item_sku_id="100145382842", sku_name="都夏美隆", barndname_cn="都夏美隆",item_first_cate_cd="12259",dt="2024-12-26",create_timestamp="1736911578491"),
    # Row(item_sku_id="100145382842", sku_name="都夏美隆", barndname_cn="都夏美隆",item_first_cate_cd="12259",dt="2024-12-26",create_timestamp="1736911578492")
]
spark.createDataFrame(data).createOrReplaceTempView("gdm_m03_item_sku_act")

# sku_act_sql="""
# select
#     *
# from
#     (select
#         *,
#         row_number() over (partition by item_sku_id,sku_name order by create_timestamp desc) rank
#     from
#         gdm_m03_item_sku_act)
# where
#     rank=1
# """
data=[
    Row(item_sku_id="苹果", sku_name="苹果",category='perfume_flavor',dt="2024-12-26"),
    Row(item_sku_id="草莓", sku_name="草莓",category='perfume_flavor',dt="2024-12-26"),
    Row(item_sku_id="榄香脂", sku_name="榄香脂",category='perfume_flavor',dt="2024-12-26"),
    Row(item_sku_id="薰衣草", sku_name="薰衣草",category='perfume_flavor',dt="2024-12-26")
]
spark.createDataFrame(data).createOrReplaceTempView("ai_product_detail_mapping_rule_v1")

#     to_json(map_from_entries(collect_list(named_struct('key', item_sku_id, 'value', sku_name)))) AS json_result

# sku_act_sql = """
# select
#     to_json(map_from_entries(collect_list(named_struct('key', item_sku_id, 'value', sku_name)))) AS json_result
# FROM
#     ai_product_detail_mapping_rule_v1
# where
#     dt='2024-12-26'
# group by
#     category
# """

sku_act_sql = """
select
    to_json(named_struct('product_sku_tips', '想找到更多适合的酒')) AS data
FROM 
    ai_product_detail_mapping_rule_v1
where
    dt='2024-12-26'
group by
    category
"""

sku_act_df = spark.sql(sku_act_sql)
sku_act_df.printSchema()
sku_act_df.show(truncate=False)

# row_number使用
sku_act_sql="""
select
    collect_list(item_sku_id) AS aggregated_list
from
    (select
        *,
        CAST(row_number() over (order by item_sku_id desc) / 10 AS INT) as group_id
    from
        gdm_m03_item_sku_act)
group by 
    group_id
"""

sku_act_df = spark.sql(sku_act_sql)
sku_act_df.printSchema()
sku_act_df.show(truncate=False)