from audioop import mul
import threading
import yaml
import time
import logging
import pandas as pd
compare_val = dict()

LOG_FILENAME = 'logging_example12.txt'
Format = "%(asctime)s.%(msecs)06d;%(message)s"
logging.basicConfig(
    format=Format,
    filename=LOG_FILENAME,
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

with open("Milestone2A.yaml","r") as stream:
    parsed_yaml = yaml.safe_load(stream)

def timefunc_cond(dicti,initial_str):
    traverse = dicti['Condition'].split(" ")
    logging.info(initial_str+" "+"Entry")
    if traverse[0][2:len(traverse[0])-1] in compare_val.keys():
        if traverse[1] == '>':
            print("yessss")
            if compare_val[traverse[0][2:len(traverse[0])-1]] >int(traverse[2]):
                logging.info(initial_str+" "+"Executing"+" "+dicti['Function']+" "+"("+str(dicti['Inputs']['FunctionInput'])+', '+str(dicti['Inputs']['ExecutionTime'])+")")
                k = dicti['Inputs']['ExecutionTime']
                time.sleep(int(k))
                logging.info(initial_str+" "+"Exit")
        elif traverse[1] =='<':
            if compare_val[traverse[0][2:len(traverse[0])-1]] <int(traverse[2]):
                logging.info(initial_str+" "+"Executing"+" "+dicti['Function']+" "+"("+str(dicti['Inputs']['FunctionInput'])+', '+str(dicti['Inputs']['ExecutionTime'])+")")
                k = dicti['Inputs']['ExecutionTime']
                time.sleep(int(k))
                logging.info(initial_str+" "+"Exit")

        


def timefunc(input_val):
    k = input_val['ExecutionTime']
    time.sleep(int(k))
def loader_cond(input,condi,outputs,string,initial_str,dicti):
    traverse = condi.split(" ")
    logging.info(initial_str+" "+"Entry")
    if traverse[0][2:len(traverse[0])-1] in compare_val.keys():
        if traverse[1] == '>':
            print("yessss")
            if compare_val[traverse[0][2:len(traverse[0])-1]] >int(traverse[2]):
                logging.info(initial_str+" "+"Executing"+" "+dicti['Function']+" "+"("+str(dicti['Inputs']['Filename'])+")")
                data_table = pd.read_csv(input['Filename'])
                k = data_table.shape
                defects = k[0]
                print(defects)
                new_string = string+'NoOfDefects'
                compare_val[new_string] = defects
                print(compare_val)
                logging.info(initial_str+" "+"Exit")
        
        elif traverse[1] =='<':
            print("Yes")
            logging.info(initial_str+" "+"Executing"+" "+dicti['Function']+" "+"("+str(dicti['Inputs']['Filename'])+")")
            if compare_val[traverse[0][2:len(traverse[0])-1]] <int(traverse[2]):
                data_table = pd.read_csv(input['Filename'])
                k = data_table.shape
                defects = k[0]
                print(defects)
                new_string = string+'NoOfDefects'
                compare_val[new_string] = defects
                print(compare_val)
                logging.info(initial_str+" "+"Exit")


def loader(input,outputs,strings):
    data_table = pd.read_csv(input['Filename'])
    k = data_table.shape
    defects = k[0]
    print(defects)
    new_string = strings+'NoOfDefects'
    compare_val[new_string] = defects
    print(compare_val)

def task_perform(dict_obj,initial_str):
    if dict_obj['Function'] == 'TimeFunction':
        if 'Condition' in dict_obj.keys():
            timefunc_cond(dict_obj,initial_str)
        else:
            logging.info(initial_str+" "+"Entry")
            logging.info(initial_str+" "+"Executing"+" "+dict_obj['Function']+" "+"("+str(dict_obj['Inputs']['FunctionInput'])+', '+str(dict_obj['Inputs']['ExecutionTime'])+")")
            timefunc(dict_obj['Inputs'])
            logging.info(initial_str+" "+"Exit")

    if dict_obj['Function'] =="DataLoad":
        #logging.info(initial_str+" "+"Executing"+" "+dict_obj['Function']+" "+"("+str(dict_obj['Inputs']['Filename'])+")")
        print(dict_obj.keys())
        if 'Condition' in dict_obj.keys():
            loader_cond(dict_obj['Inputs'],dict_obj['Condition'],dict_obj['Outputs'],initial_str+'.',initial_str,dict_obj)
        else:
            logging.info(initial_str+" "+"Entry")
            logging.info(initial_str+" "+"Executing"+" "+dict_obj['Function']+" "+"("+str(dict_obj['Inputs']['Filename'])+")")
            loader(dict_obj['Inputs'],dict_obj['Outputs'],initial_str+'.')
            logging.info(initial_str+" "+"Exit")

    #print(func,dict_obj['Inputs'],initial_str)


def func(dict_obj,initial_string):
        if dict_obj['Type'] =='Task':
            task_perform(dict_obj,initial_string)
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
            #print(arr)
            #print(s)
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
