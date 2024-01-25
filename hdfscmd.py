import subprocess

def run_cmd(args_list):
    """
    Description: To execute the hadoop hdfs commands using subprocess libraries

    Parameter: It takes args_list as parameter

    Return: It returns s_return, s_output, e_err
    """

    print(f'Running system command: {args_list}')

    proc = subprocess.Popen(args_list,stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    s_output, s_err = proc.communicate()

    s_return = proc.returncode

    return s_return,s_output,s_err

if __name__ == "__main__":

    #to create directory in hdfs

    mkdir = run_cmd(['hadoop','fs','-mkdir','/Kafka Stock_Data-txt'])

    print(mkdir)

    # copy stockdataconsole.txt from local system to hdfs

    copy_file = run_cmd(['hadoop','fs','-copyFromLocal', '/home/lenovo/Desktop/Python_work/hadoop/KAFKA?stockdata/stockdataconsole.txt','/Kafka_Stock_data.txt'])

    print(copy_file)
