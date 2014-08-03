<?php
class PigFilterScriptBuilder extends PigScriptBuilder {
	public function __construct($params) {
		parent::__construct($params);
		if (count($this->parsed_args) < 2) {
			$this->print_usage();
			return;
		}
		$input_path = $this->parsed_args["inputPath"];
		$loadAs = $this->parsed_args["loadAs"];
		$filterBy = $this->parsed_args["filterBy"];
		$output_path = $this->parsed_args["outputPath"];

		if( !(strpos($input_path, "hdfs://") === 0 )) {
			$input_path = "hdfs://". $input_path;
		}
		/*
		if( !(strpos($output_path, "hdfs://") === 0 )) {
			$output_path = "hdfs://". $input_path;
		}
		*/


		$job_name = array_key_exists("jobName", $this->parsed_args) ? $this->parsed_args["jobName"] : "pigjob";

		#$this->check_pig_classpath($this->hadoop_home);
		$filterBy = str_replace("\"", "'", $filterBy);
		$filterBy = str_replace("=", "==", $filterBy);

		$this->cmd = "SET job.name " . $job_name . ";";
		$this->cmd .= "A = load '".$input_path."' using PigStorage('".$this->delimeter."') as ".$loadAs.";";
		$this->cmd .= "B = Filter A BY $filterBy;";
		$this->cmd .= "store B into '".$output_path."' using PigStorage('".$this->delimeter."');";
		
		//print_r( $params );
		$output_script_path = $this->put_script($input_path);
		$this->clear_output_path();
		$this->run_pig_script($output_script_path);
	}
	public function print_usage() {
		$help = "usage:php PigFilterScriptBuilder.php --inputPath [input path] --loadAs [\"columns in input\"] --filterBy [\"where clause\"] ";
		$help .= "--outputPath [output path] --jobName [job name] --hadoopHome [hadoop home] --pigHome [pig home]\n";
		echo $help;
	}
}
class PigScriptBuilder {
	protected $delimeter = ",";
	protected $parsed_args = array();
	protected $cmd = "";
	protected $hadoop_home = "";
	protected $pig_home = "";
	public function parse_argument($params) {
		for ($i = 0; $i < count($params); $i++) {
			if ($params[$i] == "--inputPath" || $params[$i] == "-i") {
				$this->parsed_args["inputPath"] = $params[++$i];
			} else if ($params[$i] == "--outputPath" || $params[$i] == "-o") {
				$this->parsed_args["outputPath"] = $params[++$i];
			} else if ($params[$i] == "--loadAs" || $params[$i] == "-las") {
				$this->parsed_args["loadAs"] = $params[++$i];
			} else if ($params[$i] == "--filterBy" || $params[$i] == "-fby") {
				$this->parsed_args["filterBy"] = $params[++$i];
			} else if ($params[$i] == "--outputScriptPath" || $params[$i] == "-os") {
				$this->parsed_args["outputScriptPath"] = $params[++$i];
			} else if ($params[$i] == "--jobName" || $params[$i] == "-jn") {
				$this->parsed_args["jobName"] = $params[++$i];
			} else if ($params[$i] == "--hadoopHome" || $params[$i] == "-hh") {
				$this->hadoop_home = $params[++$i];
			} else if ($params[$i] == "--pigHome" || $params[$i] == "-ph") {
				$this->pig_home = $params[++$i];
			}

		}
	}
	public function get_argument() {
		return $this->parsed_args;
	}
	
	public function __construct($params) {
		$this->parse_argument($params);
	}

	public function write_pig_script($script_file) {
		$of = fopen($script_file, "w");
		fwrite($of, $this->cmd);
		fclose($of);
	}

	public function clear_output_path() {
		echo exec("$this->hadoop_home/bin/hadoop fs -rmr " . $this->parsed_args["outputPath"]);
	}
	
	public function run_pig_script($script_file) {
		$SET_JAVA_HOME = exec("grep '^export[[:space:]+]JAVA_HOME' $this->hadoop_home/conf/hadoop-env.sh");

		$cmd = "$SET_JAVA_HOME;export PIG_HOME=$this->pig_home;";
		$cmd .="export PIG_CLASSPATH=$this->hadoop_home/conf:$this->hadoop_home/lib/hadoop-lzo-0.4.9.jar;";
		$cmd .="export HADOOP_HOME=$this->hadoop_home;";
		$cmd .= "$this->pig_home/bin/pig -x mapreduce " . $script_file . " > /dev/null &";
		echo exec($cmd);
	}

	public function check_pig_classpath($hadoop_home) {
		if (getenv("PIG_CLASSPATH") == false) {
			putenv("PIG_CLASSPATH=$this->hadoop_home/conf:$this->hadoop_home/lib/hadoop-lzo-0.4.9.jar");
		} 	
	}

	public function put_script($input_path) {
		$dir = dirname($input_path);
		$base_name = basename($input_path);
		//$output_script_path = $dir . "/." . $base_name . "." . date('Ymd_H_i_s') . "." . rand() . ".pig";
		$ip=$_SERVER['REMOTE_ADDR'];
		$output_script_path = $dir . "/." . $base_name . ".$ip.pig";
		exec("$this->hadoop_home/bin/hadoop fs -rm $output_script_path");
		$cmd = "echo \"$this->cmd\" | $this->hadoop_home/bin/hadoop fs -put - ".$output_script_path;
		echo exec($cmd);
		return $output_script_path;
	}
}
//$dummy = array("--inputPath", "hdfs://20.20.20.31:9000/user/doyoung/input/netflix_training_all.dat", 
/*
$dummy = array("--inputPath", "/user/doyoung/input/netflix_training_all.dat", 
"--outputPath", "/user/doyoung/tmp",  "--loadAs", "(u, i, r)", "--filterBy", "(u < 10 or i < 100)",
"--hadoopHome", "/home/hdfs/hadoop", "--pigHome", "/home/hdfs/pig");
*/
#$obj = new PigFilterScriptBuilder(array_slice($argv, 1));
//$obj = new PigFilterScriptBuilder($dummy);
?>
