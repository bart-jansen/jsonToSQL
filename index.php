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

	$rows = ["JOBID", "JOBNAME", "USER", "SUBMIT_TIME", "JOBCONF","VIEW_JOB","MODIFY_JOB","JOB_QUEUE","JOB_PRIORITY","LAUNCH_TIME","TOTAL_MAPS","TOTAL_REDUCES","JOB_STATUS","FINISH_TIME","FINISHED_MAPS",
	"FINISHED_REDUCES", "FAILED_MAPS", "FAILED_REDUCES"];

	$counters= ["MAP_COUNTERS", "REDUCE_COUNTERS", "COUNTERS"];
	$possible_counters = [];
	
	foreach($counters as $counter) {
		$result = mysql_query("SHOW COLUMNS FROM ".strtolower($counter)."");
		while ($row = mysql_fetch_assoc($result)) {
			$possible_counters[$counter][] = $row['Field'];
		}	
		
		$counterSQL[$counter] = "INSERT INTO ".strtolower($counter)." (".implode(',',$possible_counters[$counter]).") VALUES ";
		
	}
	
	$sql_insert = "INSERT INTO jobs (".implode(',', $rows).") VALUES ";

	$sql_vals = [];

	//foreach job in .json file
	foreach($json['ALLJOBS'] as $jobs) {
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
			
			//for every kind of counter within job (counter/map_counters/reduce_counters)
			foreach($counters as $counter) {
				
				if(array_key_exists($counter, $jobs) && count($jobs[$counter]) >= 1) {
					//echo $counter . " " . count($jobs[$counter]);
					
					$singleCounterSQL = "('','".$jobs[$rows[0]]."',";
					
					foreach($possible_counters[$counter] as $key => $jCounter) {
						if($key > 1) { //omit both ID & JOBID fields
							if(array_key_exists($jCounter, $jobs[$counter])) {
								//echo $jobs[$counter][$jCounter]."<br/><br/>";
								$singleCounterSQL .="'".$jobs[$counter][$jCounter]."',";
							}
							else {
								$singleCounterSQL .="'',";
							}
						}
					}
					
					$counterSQL[$counter] .= rtrim($singleCounterSQL,",")."),";
				}
			}
			
			$sql_vals[] = rtrim($job_sql,",").")";
		}
	}

	$sql_insert .= implode(",", $sql_vals);
	
	if(!$testmode) {
		//insert job sql
		mysql_query($sql_insert)or die(mysql_error());
		
		foreach($counters as $counter) {
			//insert counter sql
			mysql_query(rtrim($counterSQL[$counter],","))or die(mysql_error());
		}
	}
}
?>