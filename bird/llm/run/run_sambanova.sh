#!/bin/bash

# 设置环境变量和路径
eval_path='./data/dev.json'
dev_path='./output/'
db_root_path='/Users/changranh/Desktop/bird2/DAMO-ConvAI/bird/data/dev_databases/'
use_knowledge='True'
not_use_knowledge='False'
mode='dev' # 选择dev或dev
cot='True'
no_cot='False'

# 使用SambaNova API密钥
YOUR_API_KEY='6522ed42-dfa0-4f55-8231-5b6f9e2a9174'

# 模型参数
engine='Meta-Llama-3.3-70B-Instruct'  # 使用Llama 3.3模型

# 输出路径
data_output_path='./exp_result/llama3_output/'
data_kg_output_path='./exp_result/llama3_output_kg/'

# 确保输出目录存在
mkdir -p $data_output_path
mkdir -p $data_kg_output_path

# 首先测试一个小样本（例如，只处理几个查询）
echo '生成小样本测试（使用外部知识）'
python3 -u ./src/gpt_request_sambanova.py --db_root_path ${db_root_path} --api_key ${YOUR_API_KEY} --mode ${mode} \
--engine ${engine} --eval_path ${eval_path} --data_output_path ${data_kg_output_path} --use_knowledge ${use_knowledge} \
--chain_of_thought ${no_cot}

# 评估生成的SQL
echo '评估生成的SQL'
python3 -u ./src/evaluation.py \
  --predicted_sql_path ${data_kg_output_path} \
  --ground_truth_path ./data/ \
  --data_mode ${mode} \
  --db_root_path ${db_root_path} \
  --num_cpus 4 \
  --meta_time_out 30.0 \
  --diff_json_path ${eval_path}
