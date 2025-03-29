#!/usr/bin/env python3
import argparse
import fnmatch
import json
import os
import pdb
import pickle
import re
import sqlite3
import sys
from typing import Dict, List, Tuple

import backoff
import openai
import pandas as pd
import sqlparse
from tqdm import tqdm

# 添加父目录到路径，以便导入sambanova_adapter
sys.path.append('/Users/changranh/Desktop/bird2')
from sambanova_adapter import adapt_completion_to_chat

'''openai configure'''
openai.debug=True

def new_directory(path):  
    if not os.path.exists(path):  
        os.makedirs(path)  

def get_db_schemas(bench_root: str, db_name: str) -> Dict[str, str]:
    """
    Read an sqlite file, and return the CREATE commands for each of the tables in the database.
    """
    asdf = 'database' if bench_root == 'spider' else 'databases'
    with sqlite3.connect(f'file:{bench_root}/{asdf}/{db_name}/{db_name}.sqlite?mode=ro', uri=True) as conn:
        # conn.text_factory = bytes
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        schemas = {}
        for table in tables:
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='{}';".format(table[0]))
            schemas[table[0]] = cursor.fetchone()[0]

        return schemas

def nice_look_table(column_names: list, values: list):
    rows = []
    # Determine the maximum width of each column
    widths = [max(len(str(value[i])) for value in values + [column_names]) for i in range(len(column_names))]

    # Print the column names
    header = ''.join(f'{column.rjust(width)} ' for column, width in zip(column_names, widths))
    # print(header)
    # Print the values
    for value in values:
        row = ''.join(f'{str(v).rjust(width)} ' for v, width in zip(value, widths))
        rows.append(row)
    rows = "\n".join(rows)
    final_output = header + '\n' + rows
    return final_output

def generate_schema_prompt(db_path, num_rows=None):
    # extract create ddls
    '''
    :param root_place:
    :param db_name:
    :return:
    '''
    full_schema_prompt_list = []
    conn = sqlite3.connect(db_path)
    # Create a cursor object
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    schemas = {}
    for table in tables:
        if table == 'sqlite_sequence':
            continue
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='{}';".format(table[0]))
        create_prompt = cursor.fetchone()[0]
        schemas[table[0]] = create_prompt
        if num_rows:
            cur_table = table[0]
            if cur_table in ['order', 'by', 'group']:
                cur_table = "`{}`".format(cur_table)

            cursor.execute("SELECT * FROM {} LIMIT {}".format(cur_table, num_rows))
            column_names = [description[0] for description in cursor.description]
            values = cursor.fetchall()
            rows_prompt = nice_look_table(column_names=column_names, values=values)
            verbose_prompt = "/* \n {} example rows: \n SELECT * FROM {} LIMIT {}; \n {} \n */".format(num_rows, cur_table, num_rows, rows_prompt)
            schemas[table[0]] = "{} \n {}".format(create_prompt, verbose_prompt)

    for k, v in schemas.items():
        full_schema_prompt_list.append(v)

    schema_prompt = "\n\n".join(full_schema_prompt_list)

    return schema_prompt

def generate_comment_prompt(question, knowledge=None):
    pattern_prompt_no_kg = "-- Using valid SQLite, answer the following questions for the tables provided above."
    pattern_prompt_kg = "-- Using valid SQLite and understading External Knowledge, answer the following questions for the tables provided above."
    # question_prompt = "-- {}".format(question) + '\n SELECT '
    question_prompt = "-- {}".format(question)
    knowledge_prompt = "-- External Knowledge: {}".format(knowledge)

    if not knowledge_prompt:
        result_prompt = pattern_prompt_no_kg + '\n' + question_prompt
    else:
        result_prompt = knowledge_prompt + '\n' + pattern_prompt_kg + '\n' + question_prompt

    return result_prompt

def cot_wizard():
    cot = "\nGenerate the SQL after thinking step by step: "
    
    return cot

def few_shot():
    ini_table = "CREATE TABLE singer\n(\n    singer_id         TEXT not null\n        primary key,\n    nation       TEXT  not null,\n    sname       TEXT null,\n    dname       TEXT null,\n    cname       TEXT null,\n    age    INTEGER         not null,\n    year  INTEGER          not null,\n    birth_year  INTEGER          null,\n    salary  REAL          null,\n    city TEXT          null,\n    phone_number   INTEGER          null,\n--     tax   REAL      null,\n)"
    ini_prompt = "-- External Knowledge: age = year - birth_year;\n-- Using valid SQLite and understading External Knowledge, answer the following questions for the tables provided above.\n-- How many singers in USA who is older than 27?\nThe final SQL is: Let's think step by step."
    ini_cot_result = "1. referring to external knowledge, we need to filter singers 'by year' - 'birth_year' > 27; 2. we should find out the singers of step 1 in which nation = 'US', 3. use COUNT() to count how many singers. Finally the SQL is: SELECT COUNT(*) FROM singer WHERE year - birth_year > 27;</s>"
    
    one_shot_demo = ini_table + '\n' + ini_prompt + '\n' + ini_cot_result
    
    return one_shot_demo

def few_shot_no_kg():
    ini_table = "CREATE TABLE singer\n(\n    singer_id         TEXT not null\n        primary key,\n    nation       TEXT  not null,\n    sname       TEXT null,\n    dname       TEXT null,\n    cname       TEXT null,\n    age    INTEGER         not null,\n    year  INTEGER          not null,\n    age  INTEGER          null,\n    salary  REAL          null,\n    city TEXT          null,\n    phone_number   INTEGER          null,\n--     tax   REAL      null,\n)"
    ini_prompt = "-- External Knowledge:\n-- Using valid SQLite and understading External Knowledge, answer the following questions for the tables provided above.\n-- How many singers in USA who is older than 27?\nThe final SQL is: Let's think step by step."
    ini_cot_result = "1. 'older than 27' refers to age > 27 in SQL; 2. we should find out the singers of step 1 in which nation = 'US', 3. use COUNT() to count how many singers. Finally the SQL is: SELECT COUNT(*) FROM singer WHERE age > 27;</s>"
    
    one_shot_demo = ini_table + '\n' + ini_prompt + '\n' + ini_cot_result
    
    return one_shot_demo



def generate_combined_prompts_one(db_path, question, knowledge=None):
    schema_prompt = generate_schema_prompt(db_path, num_rows=None) # This is the entry to collect values
    comment_prompt = generate_comment_prompt(question, knowledge)

    combined_prompts = schema_prompt + '\n\n' + comment_prompt + cot_wizard() + '\nSELECT '
    # combined_prompts = few_shot() + '\n\n' + schema_prompt + '\n\n' + comment_prompt

    # print(combined_prompts)

    return combined_prompts

def quota_giveup(e):
    return isinstance(e, openai.error.RateLimitError) and "quota" in str(e)

# 修改后的connect_gpt函数，使用SambaNova API
def connect_gpt(engine, prompt, max_tokens, temperature, stop):
    try:
        # 使用SambaNova适配器
        result = adapt_completion_to_chat(engine, prompt, max_tokens, temperature, stop)
    except Exception as e:
        result = 'error:{}'.format(e)
    return result

def collect_response_from_gpt(db_path_list, question_list, api_key, engine, knowledge_list=None):
    '''
    :param db_path: str
    :param question_list: []
    :return: dict of responses collected from openai
    '''
    responses_dict = {}
    response_list = []
    
    # 设置SambaNova API密钥
    os.environ["SAMBANOVA_API_KEY"] = api_key
    
    for i, question in tqdm(enumerate(question_list)):
        print('--------------------- processing {}th question ---------------------'.format(i))
        print('the question is: {}'.format(question))
        
        if knowledge_list:
            cur_prompt = generate_combined_prompts_one(db_path=db_path_list[i], question=question, knowledge=knowledge_list[i])
        else:
            cur_prompt = generate_combined_prompts_one(db_path=db_path_list[i], question=question)
        
        plain_result = connect_gpt(engine=engine, prompt=cur_prompt, max_tokens=256, temperature=0, stop=['--', '\n\n', ';', '#'])
        
        if type(plain_result) == str:
            sql = plain_result
        else:
            # 直接使用模型的输出，不添加额外的SELECT前缀
            sql = plain_result['choices'][0]['text']
        
        db_id = db_path_list[i].split('/')[-1].split('.sqlite')[0]
        sql = sql + '\t----- bird -----\t' + db_id # to avoid unpredicted \t appearing in codex results
        response_list.append(sql)

    return response_list

def question_package(data_json, knowledge=False):
    all_questions = []
    for _, content in enumerate(data_json):
        # print(content['question'])
        question = content["question"]
        if knowledge:
            if content["evidence"]:
                knowledge = content["evidence"]
                all_questions.append((question, knowledge))
            else:
                all_questions.append((question, ""))
        else:
            all_questions.append(question)
    return all_questions

def knowledge_package(data_json, knowledge=False):
    if not knowledge:
        return None
    all_knowledge = []
    for _, content in enumerate(data_json):
        if content["evidence"]:
            knowledge = content["evidence"]
            all_knowledge.append(knowledge)
        else:
            all_knowledge.append("")
    return all_knowledge

def decouple_question_schema(datasets, db_root_path):
    db_paths = []
    for i, content in enumerate(datasets):
        db_id = content["db_id"]
        db_path = os.path.join(db_root_path, db_id, db_id + '.sqlite')
        db_paths.append(db_path)
    return db_paths

def generate_sql_file(sql_lst, output_path=None):
    result_dict = {}
    for i, one_sql in enumerate(sql_lst):
        result_dict[str(i)] = one_sql
    if output_path:
        with open(os.path.join(output_path, 'predict_dev.json'), 'w') as f:
            json.dump(result_dict, f, indent=4)


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--eval_path', type=str, default='')
    args_parser.add_argument('--mode', type=str, default='dev')
    args_parser.add_argument('--use_knowledge', type=str, default='True')
    args_parser.add_argument('--chain_of_thought', type=str, default='True')
    args_parser.add_argument('--data_output_path', type=str, default='')
    args_parser.add_argument('--db_root_path', type=str, default='')
    args_parser.add_argument('--api_key', type=str, default='')
    args_parser.add_argument('--engine', type=str, default='')
    args = args_parser.parse_args()
    eval_json = args.eval_path
    db_root_path = args.db_root_path
    data_json = json.load(open(eval_json))
    use_knowledge = True if args.use_knowledge == 'True' else False
    is_cot = True if args.chain_of_thought == 'True' else False

    # print('knowledge: {}'.format(use_knowledge))
    data_output_path = args.data_output_path
    print('output path: {}'.format(data_output_path))
    new_directory(data_output_path)
    questions = question_package(data_json, knowledge=use_knowledge)
    knowledges = knowledge_package(data_json, knowledge=use_knowledge)
    db_paths = decouple_question_schema(data_json, db_root_path)
    
    if use_knowledge:
        new_questions = []
        for each in questions:
            new_questions.append(each[0])
        responses = collect_response_from_gpt(db_path_list=db_paths, question_list=new_questions, api_key=args.api_key, engine=args.engine, knowledge_list=knowledges)
    else:
        responses = collect_response_from_gpt(db_path_list=db_paths, question_list=questions, api_key=args.api_key, engine=args.engine, knowledge_list=None)
    generate_sql_file(responses, data_output_path)
