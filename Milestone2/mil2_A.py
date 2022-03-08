from audioop import mul
import threading
import yaml
import time
import logging

LOG_FILENAME = 'logging_example12.txt'
Format = "%(asctime)s.%(msecs)06d;%(message)s"
logging.basicConfig(
    format=Format,
    filename=LOG_FILENAME,
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

with open("Milestone1B.yaml","r") as stream:
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
        if dict_obj['Type'] =='Task':
            task_perform(dict_obj['Function'],dict_obj['Inputs'],initial_string)
        elif dict_obj['Type'] =='Flow' and dict_obj['Execution'] =='Sequential':
            logging.info(initial_string+" "+"Entry")
            for key in dict_obj['Activities'].keys():
                func(dict_obj['Activities'][key],initial_string+"."+str(key))
            logging.info(initial_string+" "+"Exit")
        
        elif dict_obj['Type'] =='Flow' and dict_obj['Execution'] =='Concurrent':
            s = len(dict_obj['Activities'].keys())
            m = list(dict_obj['Activities'].keys())
            arr = []
            for i in m:
                arr.append(dict_obj['Activities'][i])
            print(arr)
            print(s)
            join_arr=[]
            logging.info(initial_string+" "+"Entry")
            for i in range(len(arr)):
                x = threading.Thread(target= func, args=(arr[i],initial_string+"."+str(m[i])))
                join_arr.append(x)
                x.start()
            for i in join_arr:
                i.join()

            logging.info(initial_string+" "+"Exit")


initial_string = str(list(parsed_yaml.keys())[0])

func(parsed_yaml[initial_string],initial_string)
