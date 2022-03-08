import yaml
import multiprocessing
import time
import logging

LOG_FILENAME = 'logging_example1.txt'
Format = "%(asctime)s.%(msecs)06d;%(message)s"
logging.basicConfig(
    format=Format,
    filename=LOG_FILENAME,
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

with open("Milestone1A.yaml","r") as stream:
    parsed_yaml = yaml.safe_load(stream)

def timefunc(input_val):
    k = input_val['ExecutionTime']
    time.sleep(int(k))

def task_perform(func,input_val,initial_str):
    if func == 'TimeFunction':
        logging.info(initial_str+" "+"Entry")
        logging.info(initial_str+" "+"Executing"+" "+func+" "+"("+str(input_val['FunctionInput'])+', '+str(input_val['ExecutionTime'])+")")
        timefunc(input_val)
        logging.info(initial_str+" "+"Exit")
    print(func,input_val,initial_str)


def func(dict_obj,initial_string):
    for k in dict_obj.keys():
        if k=='Type':
            if dict_obj[k] =='Flow':
                logging.info(initial_string+" "+"Entry")
                for key in dict_obj['Activities'].keys():
                    func(dict_obj['Activities'][key],initial_string+"."+str(key))
                logging.info(initial_string+" "+"Exit")
            else:
                task_perform(dict_obj['Function'],dict_obj['Inputs'],initial_string)



initial_string = str(list(parsed_yaml.keys())[0])

func(parsed_yaml[initial_string],initial_string)
