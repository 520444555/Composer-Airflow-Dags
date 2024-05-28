#importing the required packages
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStoragePrefixSensor
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
import os
from configparser import ConfigParser
from airflow.models import Variable
 
#setting Dag name
DAG_ID="gcs-to-bq-fdr-event-store"
 
#Getting the env name
env_name = Variable.get("ENV")
#Get config file details
config_object = ConfigParser()
file_name='/home/airflow/gcs/dags/'+DAG_ID+'_'+env_name+'.ini'
config_object.read(file_name)
#Read Config file
bucket=config_object["PARAM"]["bucket"]
file_source=config_object["PARAM"]["file_source"]
processing_location=config_object["PARAM"]["processing_location"]
job_id=config_object["PARAM"]["job_id"]
template_location=config_object["PARAM"]["template_location"]
region=config_object["PARAM"]["region"]
destination_table=config_object["PARAM"]["destination_table"]
control_table_name=config_object["PARAM"]["control_table"]
archive_location=config_object["PARAM"]["archive_location"]
temp_location=config_object["PARAM"]["temp_location"]
file_pattern=config_object["PARAM"]["file_pattern"]
prefix=config_object["PARAM"]["prefix"]
copy_pattern=config_object["PARAM"]["copy_pattern"]
error_location=config_object["PARAM"]["error_location"]
temp_table=config_object["PARAM"]["temp_filename"]
project_name=config_object["PARAM"]["project_name"]
max_rows=config_object["PARAM"]["max_rows"]
#args for Dag
args={
    'start_date':days_ago(0)}
 
#Dag defination
dag=DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval="20 * * * *",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1
    )
 
#Operators Defination:
 
#File Watcher
file_watcher = GoogleCloudStoragePrefixSensor(
    task_id='file_watcher',
    bucket=bucket,
    prefix=prefix,
    google_cloud_conn_id='google_cloud_default',
    mode='reschedule',
    timeout=54*60,
    poke_interval=18*60,
    dag=dag
    )
#Task to move file to processing location
get_file_list = BashOperator(
    task_id='get_file_list',
    params={'temp_location':temp_location,"file_source":file_source,"file_pattern":file_pattern,"temp_table":temp_table,"region":region,"control_table_name":control_table_name,"processing_location":processing_location,"max_rows":max_rows},
    bash_command="""
    gsutil ls {{params.file_source}}/{{params.file_pattern}} > folder_list.txt
    echo "Getting list of files from source bucket : `cat folder_list.txt`"
    bq query --nouse_legacy_sql 'delete from {{params.temp_table}} where 1=1;'
echo "Temp table truncated"
    bq --location={{params.region}} load  {{params.temp_table}} folder_list.txt folder_name:string
    echo "Temp Table loaded with new file names "
    bq query --max_rows={{params.max_rows}} --nouse_legacy_sql --format=csv "select folder_name from {{params.temp_table}} where ARRAY_REVERSE(split(folder_name,'/'))[OFFSET(1)] || '/' || ARRAY_REVERSE(split(folder_name,'/'))[OFFSET(0)] not in (select ARRAY_REVERSE(split(FileName,'/'))[OFFSET(1)] || '/' || ARRAY_REVERSE(split(FileName,'/'))[OFFSET(0)] from {{params.control_table_name}});" > list.txt
    echo "Processing File List: `cat list.txt`"
    processing_file_list=''
    {
    read
    while IFS= read -r line
    do
    
result=$(gsutil cat $line | awk -F'"' 'NF%2==0')
        if [ -n "$result" ]; then
            echo "Line breaker found in $line"
            file_name_last=$(echo "$line" | awk -F'/' '{print $NF}')
            folder_name=$(echo "$line" | awk -F'/' '{print $(NF-1)}')
            new_line={{params.processing_location}}/$folder_name/$file_name_last
            gsutil cat $line | sed -n 'H;g;/^[^"]*"[^"]*\("[^"]*"[^"]*\)*$/d; s/^\\n//; y/\\n/ /; p; s/.*//; h' >> new_file
            gsutil cp new_file $new_line
            rm -rf new_file
            line=$new_line
        else
            echo "No Line breaker found in $line"
        fi
        processing_file_list=$processing_file_list","$line
    done
    } < list.txt
    if [ -z "${processing_file_list:1}" ];then
        exit 1
else
        first_file=`echo ${processing_file_list:1} | cut -d "," -f 1`
        gsutil cat $first_file | head -1 >> /tmp/header.txt
        gsutil cp /tmp/header.txt {{params.temp_location}}/
        rm -rf /tmp/header.txt
        echo "Final list for dataflow job :"
        echo ${processing_file_list:1}
    fi
    """ ,
    do_xcom_push=True,
    dag=dag,
    )
    
#adding the data in control table, if task is successful
control_table_update = BashOperator(#control_table_update
    task_id='add_to_control_table',
    params={'control_table':control_table_name,'job_id':job_id,'processing_location':processing_location,'archive_location':archive_location,'copy_pattern':copy_pattern,'error_location':error_location,'file_pattern':file_pattern,'file_source':file_source},
    bash_command="""
    Jobstatus=`echo "{{ ti.xcom_pull(task_ids="get_job_status") }}"`
    if [ "$Jobstatus" = "Done" ];then
    echo "Job is completed"
    files=`echo "{{ ti.xcom_pull(task_ids="get_file_list") }}"`
    echo "File List processed : $files"
    for file_name in $(echo $files | tr "," "\n")
    do
if [[ $file_name == {{params.processing_location}}* ]]; then
        file_name_last=$(echo "$file_name" | awk -F'/' '{print $NF}')
        folder_name=$(echo "$file_name" | awk -F'/' '{print $(NF-1)}')
        gsutil mv $file_name {{params.archive_location}}/$folder_name/
        file_name={{params.file_source}}/$folder_name/$file_name_last
    fi
    status=`bq query --use_legacy_sql=false "insert into {{params.control_table}} values('{{params.job_id}}','$Jobstatus',current_timestamp(),'$file_name')"`
    echo $status
    done
    else
    for file_name in $(echo $files | tr "," "\n")
    do
    folder_name=$(echo $file_name | cut -d "/" -f 5)
    echo "$folder_name/$file_name is moved in error location."
    gsutil cp $file_name {{params.error_location}}/$folder_name/
    done
    exit 1
    fi
    """,
    dag=dag,
    )
#Task Execution state
file_watcher >> get_file_list >> dataflow_deploy >> get_job_status  >> control_table_update
# file_watcher >> get_file_list >> dataflow_deploys
#comment
