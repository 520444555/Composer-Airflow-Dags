#importing the required packages
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStoragePrefixSensor
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
import os
from configparser import ConfigParser
from airflow.models import Variable
 
#setting Dag name
DAG_ID="gcs-to-bq-fdr-rules-audit"
#Getting the env name
env_name = Variable.get("ENV")
 
#Getting the project name
#project_name = os.popen("gcloud config get project").read().replace("\n", "")
 
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
#copy_pattern=config_object["PARAM"]["copy_pattern"]
error_location=config_object["PARAM"]["error_location"]
temp_table=config_object["PARAM"]["temp_table"]
project_name=config_object["PARAM"]["project_name"]
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
move_to_processing = BashOperator(
    task_id='move_to_processing',
    params={'temp_location':temp_location,"file_source":file_source,"file_pattern":file_pattern,"temp_table":temp_table,"region":region,"control_table_name":control_table_name},
    bash_command="""
    gsutil ls {{params.file_source}}/{{params.file_pattern}} > folder_list.txt
    bq query --nouse_legacy_sql 'delete from {{params.temp_table}} where 1=1;'
    bq --location={{params.region}} load  {{params.temp_table}} folder_list.txt folder_name:string
    bq query --nouse_legacy_sql --format=csv "select folder_name from {{params.temp_table}} where ARRAY_REVERSE(split(folder_name,'/'))[OFFSET(0)] not in (select ARRAY_REVERSE(split(FileName,'/'))[OFFSET(0)] from {{params.control_table_name}});" > list.txt
    if [ -s list.txt ];then
        processing_file_list=''
        {
        read
        while IFS= read -r line
        do
            processing_file_list=$processing_file_list","$line
            
            
#task to run the dataflow job, from the already created template
dataflow_deploy = BashOperator(
    task_id='dataflow_deploy',
    params={'job_id':job_id,'template_location':template_location,'region':region,'processing_location':processing_location,'destination_table':destination_table,'file_pattern':file_pattern,'temp_location':temp_location},
    bash_command="""
    file_name_ref=`echo "{{ ti.xcom_pull(task_ids="move_to_processing") }}"`
    file_name=`echo "$file_name_ref"`
    gcloud dataflow jobs run {{params.job_id}} --gcs-location {{params.template_location}} --region {{params.region}} --staging-location {{params.temp_location}} --parameters ^~^inputfile=$file_name~desttable={{params.destination_table}} | grep -w 'id' | awk -F':' '{print $2}' | tr -d ' '""",
    dag=dag,
    )
#task to get the status of the dataflow job executed in the above step
get_job_status = BashOperator(
    task_id='get_job_status',
    params={'project_name':project_name,'region':region},
    bash_command="""
        job_id_ref=`echo "{{ ti.xcom_pull(task_ids="dataflow_deploy") }}"`
        job_id=`echo "$job_id_ref"`
        counter=1
        while true; do
        if [ $counter = 1 ];then
        sleep 300
        else
        sleep 60
        fi
        Jobstatus=`gcloud dataflow jobs list --project={{params.project_name}} --region={{params.region}} --filter="id=$job_id" --format="get(state)"`
        if [ "$Jobstatus" = "Running" ];then
        counter=counter+1
        continue
        else
        break
        fi
        done
        echo $Jobstatus 
    """ ,
    dag=dag,
    )
    
#adding the data in control table, if task is successful
control_table_update = BashOperator(#control_table_update
    task_id='add_to_control_table',
    #params={'control_table':control_table_name,'job_id':job_id,'processing_location':processing_location,'archive_location':archive_location,'copy_pattern':copy_pattern,'error_location':error_location,'file_pattern':file_pattern},
    params={'control_table':control_table_name,'job_id':job_id,'processing_location':processing_location,'archive_location':archive_location,'error_location':error_location,'file_pattern':file_pattern},
    bash_command="""
    Jobstatus=`echo "{{ ti.xcom_pull(task_ids="get_job_status") }}"`
    if [ "$Jobstatus" = "Done" ];then
    files=`echo "{{ ti.xcom_pull(task_ids="move_to_processing") }}"`
    for file_name in $(echo $files | tr "," "\n")
    do
    status=`bq query --use_legacy_sql=false "insert into {{params.control_table}} values('{{params.job_id}}','$Jobstatus',current_timestamp(),'$file_name')"`
    echo $status
done
    else
    for file_name in $(echo $files | tr "," "\n")
    do
    gsutil cp $file_name {{params.error_location}}/
    done
    exit 1
    fi
    """,
    dag=dag,
    )
 
 
 
#Task Execution statecontrol_table_update
#file_watcher >> move_to_processing >> dataflow_deploy >> get_job_status >> 
file_watcher >> move_to_processing >> short_circuit_task
short_circuit_task >> dataflow_deploy >> get_job_status >> control_table_update
 
# file_watcher >> move_to_processing >> dataflow_deploy
 
#The VM got deleted so we need to push the DAG again
#comment2
