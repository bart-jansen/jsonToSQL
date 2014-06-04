<?php

$link = mysql_connect('localhost', 'root', 'mooie');
$db = mysql_select_db('surflogs', $link);

$dataFolder = "data";

ini_set('max_execution_time', 300); //300 seconds = 5 minutes
ini_set('memory_limit', '-1');
ini_set('mysql.connect_timeout', 300);
ini_set('default_socket_timeout', 300);
	
/* TRUNCATING
TRUNCATE TABLE `jobs`;
TRUNCATE TABLE `counters`;
TRUNCATE TABLE `map_counters`;
TRUNCATE TABLE `reduce_counters`;

*/
$index = 0;
if ($handle = opendir($dataFolder)) {

	//while keep finding new files to process
    while (false !== ($entry = readdir($handle))) {
	
		//check if .json file
		if (strpos($entry,'.json') !== false) {

			//get .json file contents
			$string =  file_get_contents($dataFolder."/".$entry);
			$json = json_decode($string, true); //decode to make the json parsable

			$index++;
			
			//import json into mysql
			importSQL($json, 0, $index);
		}
	}

}

function importSQL($json, $testmode, $index) {

	$rows = ["JOBID", "TASKID","TASK_ATTEMPT_ID", "TASK_TYPE", "TASK_STATUS","START_TIME", "FINISH_TIME", "SORT_FINISHED", "SHUFFLE_FINISHED","HOSTNAME","HTTP_PORT","STATE_STRING","TRACKER_NAME"];
	$counters= ["COMBINE_INPUT_RECORDS","COMBINE_OUTPUT_RECORDS","COMMITTED_HEAP_BYTES","CPU_MILLISECONDS","FILE_BYTES_READ","FILE_BYTES_WRITTEN","HDFS_BYTES_WRITTEN","PHYSICAL_MEMORY_BYTES","REDUCE_INPUT_GROUPS","REDUCE_OUTPUT_GROUPS","REDUCE_SHUFFLE_BYTES","SPILLED_RECORDS","VIRTUAL_MEMORY_BYTES"];
	$possible_counters = [];
	
	$sql_insert = "INSERT INTO reduce_tasks (".implode(',', array_merge($rows, $counters)).") VALUES ";

	print_r($rows);
	$sql_vals = [];

	//foreach job in .json file
	foreach($json['REDUCERS'] as $jobs) {
		if($jobs[$rows[0]]) {
			$job_sql = "(";
			
			foreach($rows as $row) {
				if(!array_key_exists($row, $jobs)) {
					$job_sql .="'0',";
				}
				else {
					$job_sql .="'".$jobs[$row]."',";
				}
			}
			
			foreach($counters as $counter) {
				if(!array_key_exists($counter, $jobs['COUNTERS'])) {
					$job_sql .="'0',";
				}
				else {
					$job_sql .="'".$jobs['COUNTERS'][$counter]."',";
				}
			}
			
			
			$sql_vals[] = rtrim($job_sql,",").")";
		}
	}

	$sql_insert .= implode(",", $sql_vals);
	
	if(!$testmode) {
		//insert job sql
		mysql_query($sql_insert)or die(mysql_error());
		
		echo $sql_insert;
	}
}
?>