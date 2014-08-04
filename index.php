<?
ini_set('display_errors',1);
ini_set('display_startup_errors',1);
//error_reporting(-1);
error_reporting(E_ALL & ~E_NOTICE);
$HADOOP_HOME="/home/ubuntu/user/chaehyun/hadoop-1.2.1";
$FILE_LEN=32768;
$dir =  getCurrentPath();

function getCurrentPath() {
	$path= getVar('dir');
	if( $path == null || $path == "") {
		$path = "/";
	}
	return $path;
}

function getVar($key, $default=null) {
    global $_GET;
    $var = trim($_GET[$key]);
    if( strlen($var) > 0 ) { 
        return $var;
    }   
    if($default != null ) { 
        return $default;
    }   
    return null;
}
class Hadoop{
	private $HADOOP_HOME="/home/ubuntu/user/chaehyun/hadoop-1.2.1";

	public function cmd($cmd1, $cmd2, $params){
		if($cmd1 == null || $cmd2 == null)
			return FALSE;
		$method = $cmd1."_$cmd2";
		$params = explode(" ", urldecode($params));

		if(method_exists($this, $method)) {
			print $this->$method($params);
		}
		else{
			print $this->hadoop_cmd($cmd1, $cmd2, $params);
		}
		return TRUE;
	}
	private function hadoop_cmd($cmd1, $cmd2, $params) {
		if(is_array($params)){
			$params = implode(" ", $params);
		}
		return $this->hadoop_exec("hadoop $cmd1 -$cmd2 $params"); 
	}
	private function hadoop_exec($cmd){
		return shell_exec("export HADOOP_CLASSPATH=.;{$this->HADOOP_HOME}/bin/$cmd"); 
	}

	public function fs_text($arr){
		$filePath = $arr[0];
		$offset = getVar('offset');
		$len = getVar('len');
		$r = $this->hadoop_exec("hadoop HdfsFileReader $filePath $offset $len"); 
		echo $r;
	}

	private function parseLS($str) {
		$a = preg_split("/\s+/", $str);
		$r = array();

		$type = "file";
		if( $a[0][0] == 'd') {
			$type = "dir";
		}
		$r['type'] = $type;

		$r['fullpath'] = $a[7];
		$names = explode('/', $r['fullpath']);
		$r['name'] = $names[ count($names) -1 ];
		$r['operation'] = '<img path="'.$r['fullpath'].'"class=operations command=rename src=images/rename_off.png title="rename file">';
		$r['operation'] .= ' <img path="'.$r['fullpath'].'"class=operations command=delete src=images/delete_off.png title="delete file">';

		$r['permission'] = substr($a[0], 1);
		$r['replication'] = $a[1];
		$r['owner'] = $a[2];
		$r['group'] = $a[3];
		$r['size'] = $a[4];
		$r['modified'] = $a[5] . " " . $a[6];
		return $r;
	}
	private function convertToHumanReadableSize($size)
	{
		if($size == "") {
			return $size;
		}
		$filesizename = array(" Bytes", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB");
		return $size ? round($size/pow(1024, ($i = floor(log($size, 1024)))), 2) . $filesizename[$i] : '0 Bytes';
	}

	public function postVar($key) {
		return urldecode($_POST[$key]);
	}

	private function addLink($dir, $name) {
		return "<a href=?dir=$dir>$name</a>";
	}

	private function addFileLink($dir, $name) {
		return "<a href=\"javascript:viewFile('$dir')\">$name</a>";
	}

	public function splitPathAndAddLink($path) {
		$r = $this->addLink("/", "hdfs:///");
		$a = explode("/", $path);
		$path = "";
		$delim = "";
		foreach( $a as $v ) {
			if( $v == "") {
				continue;
			}
			$path .= "/" . $v;
			$r .= $delim . $this->addLink($path, $v);
			$delim = "/";
		}
		return $r;
	}

	public function fs_ls($path) {
		print "<table class=grid>\n";
		print "<tr>
		<th class=type> Type </th>
		<th class=name width=300> Name </th>
		<th class=permissions> Operations </th>
		<th class=permissions> Permissions </th>
		<th class=replications> Replicates </th>
		<th class=owner> Owner </th>
		<th class=group> Group </th>
		<th class=blockSize> Block Size </th>
		<th class=modified> Modified </th>
		</tr>\n";
		$outputStr = $this->hadoop_cmd("fs", "ls", $path); 
		$output = explode("\n", substr($outputStr, 0, -1));

		foreach($output as $val) {
			if( strncmp("Found", $val, 5) == 0 ) {
				continue;
			}
			$r = $this->parseLS($val);
			print "<tr>\n";
			foreach($r as $key => $v) {
				if( $key == "name") {
					if( $r['type'] == "dir") {
						$v = $this->addLink($r['fullpath'], $v);
					}
					else{
						$v = $this->addFileLink($r['fullpath'], $v);
					}
				}
				else if( $key == "fullpath") {
					continue;
				}
				else if( $key == "size") {
					if( $r['type'] == "file") {
						$v = $this->convertToHumanReadableSize($v);
					}
				}
				print "<td> $v </td>\n";
			}
			print "</tr>\n";
		} 
		print "</table>";
	}

}
$hadoop = new Hadoop();
if($hadoop->cmd(getVar('cmd1'), getVar('cmd2'), getVar('params'))){
	return;
}

?>
<html>
<head>
<link rel="stylesheet" href="jquery-ui.css" type="text/css" media="all" />
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js" type="text/javascript"></script>
<script src="https://ajax.googleapis.com/ajax/libs/jqueryui/1.8.16/jquery-ui.min.js" type="text/javascript"></script>
<style>
body, input, textarea, select {
    font-family: Helvetica,sans-serif;
    font-size: 12px;
}
table.grid a {
    color: #1A3448;
    padding: 0 3px;
}
table.grid a:hover, table.grid a.hover {
    background-color: #3169C6;
    color: white;
}
table.grid {
    border-collapse: collapse;
    border-spacing: 0;
    font-size: 12px;
}
table.grid thead {
    background: url("./images/bg_fieldset_header.png") repeat-x scroll left top #ECECEC;
}
table.grid tfoot {
    background: none repeat scroll 0 0 #F8F8F8;
}
table.grid td, table.grid th {
    border-right: 1px solid #CCCCCC;
    padding: 5px 8px;
}
table.grid tr, table.grid td, table.grid th {
    border-bottom: 1px solid #CCCCCC;
    white-space: nowrap;
}
table.grid td.hover, table.grid tr.hover {
    background-color: #FFFFEF;
}
table.grid tr.click, table.grid td.click {
    background-color: #FFFFCC;
}
table.grid tbody td.checkbox {
    background: url("hdfs/grippy.png") no-repeat scroll 5px center transparent;
    cursor: move;
    padding-left: 18px;
    padding-right: 6px;
    width: 5px;
}
table.grid td.type {
    text-align: center;
}
table.grid td.name {
}
table.grid td.size {
    text-align: right;
}
table.grid td.replications {
    text-align: center;
}
table.grid td.blockSize {
    text-align: right;
}
table.grid td.owner {
    text-align: center;
}
table.grid td.group {
    text-align: center;
}
table.grid td.permissions {
    text-align: center;
}
table.grid td.modified {
    text-align: center;
}
table.grid td.empty {
    font-size: 12px;
    padding: 10px;
    text-align: center;
}
textarea{
	width:100%;
	height:100%;
}
* {
    margin: 0;
    padding: 0;
}
</style>
</head>
<body>
<script>
var dir = "<?=$dir?>";
var gfilepath= "";
var goffset = 0;
var glen = <?=$FILE_LEN?>;
$(function() {
	$( "#dialog" ).dialog({ 
		resizable:true, 
		autoOpen:false,
		width:800,
		height:500,
		buttons: { 
			"rawView": function() { viewFile(gfilepath, goffset, glen);},
			"close": function() { $(this).dialog("close") }
		}
	});

	loadFileList(dir);

	setDialog("#deleteDialog", openDeleteDialog, "Delete all items", deleteItem);
	setDialog("#renameDialog", openRenameDialog, "OK", renameItem);
	setDialog("#pigStatusDialog", openPigStatusDialog, "Refresh", openPigStatusDialog);
	$("#pigStatusDialog").dialog("option", "width", 800);
	$("#pigStatusDialog").dialog("option", "height", 400);
	$("#pigStatusDialog").dialog("option", "resizable", true);

	$("button").button().click( function() {
		$( '#' + $(this).attr('id') + 'Dialog').dialog("open"); 
	});

		
});


function setDialog(name, openFunc, buttonName, buttonFunction) {
	var buttons = {};
	buttons[buttonName] = buttonFunction;
	buttons['Cancel'] = function() {$( this ).dialog( "close" );};

	$( name ).dialog({
		resizable: false,
		width:400,
		height:160,
		autoOpen:false,
		modal: true,
		open : openFunc,
		buttons: buttons	
	});
}

function percentConverter(ratio) {
	var percent = ratio *100.0;
	var remain = 100 - percent;
	var html = percent + '%';
	html += '<table width=\"80px\" border = \"1px\">';
	html += '<tr><td class=\"perc_filled\" width=\"'+percent+'%\" cellspacing=0></td>';
	html += '<td class=\"perc_nonfilled\" width=\"'+remain+'%\" cellspacing=0></td>';
	html += '</tr></table>';
	return html;
}


function openPigStatusDialog(event, ui) {
	$( "#hdfswait" ).show();
	$.get('?command=viewPigJob&inputPath='+dir, 
		function (data){
			var statusList = ['RUNNING', 'SUCCEEDED', 'FAILED', 'PREP', 'KILLED'];
			var html  = '<table style=\"font-size:8pt\" align=center border=1><tr align=center><td>Job ID</td>';
			html += '<td>Status</td><td>Output</td><td>Started at</td>';
			html += '<td>Map % Complete</td><td>Reduce % Complete</td></tr>';
			var val = JSON.parse(data);
			for( i = 0; i < val.count; i++ ) {
				var a = new Date(parseFloat(val.data[i][2]));
				var date = a.toLocaleString();

				var jobStatus = statusList[parseInt(val.data[i][1]) - 1];
				var jobID = '<a href=\"'+val.data[i][9]+'\" target=_blank>';
				jobID += val.data[i][0] + '</a>';
				var jobName = val.data[i][8];
				var output = jobName.split("||")[1];

				html += '<tr title=\"' + jobName + '\">';
				html += '<td>' + jobID + '</td>';
				html += '<td>' + jobStatus + '</td>';
				html += '<td><a href="?dir=' + output +'" target=_blank>' + output + '</a></td>';
				html += '<td>' + date + '</td>';
				html += '<td>' + percentConverter(val.data[i][6]) + '</td>';
				html += '<td>' + percentConverter(val.data[i][7]) + '</td>';
				html += '</tr>';
			}
			html += '</table>';
			$( '#pigStatusDialog').html(html);
			$( "#hdfswait" ).hide();
		}
	);
}


function openRenameDialog() {
	var fullpath = $(this).html(); 
	$(this).html('');
	var pathArray = fullpath.split("/");
	var fileName = pathArray[ pathArray.length - 1 ];

	var baseName = "";
	for(var i = 0; i < pathArray.length -1; i++ ) {
		baseName += pathArray[i] + "/";
	}

	$( "#renameDialog").attr('originalFilePath', fullpath);
	$( "#renameDialog").attr('originalBasePath', baseName);
	var str = 'Before : ' + fileName + '<BR>';
	str += 'After &nbsp: <input size=40 id=targetFileName type=text value=\"' + fileName + '\">';
	$( "#renameDialog").html(str);
}

function renameItem() {
	$( this ).dialog( "close" );
	$( "#hdfswait" ).show();
	var baseName = $(this).attr('originalBasePath');
	var originalFilePath  = $(this).attr('originalFilePath');
	var targetFileName = $('#targetFileName').val();
	var targetFilePath = baseName + targetFileName;
	var url = '?cmd1=fs&cmd2=mv&params='+originalFilePath + '%20'+ targetFilePath;
	$.get(url, function(data) {
		loadFileList(dir);
	});
}

function openDeleteDialog() {
	$( "#deleteDialog").html(path);
}

function deleteItem() {
	$( this ).dialog( "close" );
	$( "#hdfswait" ).show();
	var path = $( "#deleteDialog").html();
	$.get('?cmd1=fs&cmd2=rmr&params='+path, function() {
		loadFileList(dir);
	});
}



function loadFileList(dir) {
	url = '?cmd1=fs&cmd2=ls&params='+dir;
	$('#hdfs').load(url, function() {

		//image overlay effect
		$( ".operations").mouseover(function(e) {
			if ( !$(this).hasClass('active') ){
				var image_name = $(this).attr('src').split('_off.')[0];
				var image_type = $(this).attr('src').split('off.')[1];
				$(this).attr('src', image_name + '_on.' + image_type);
			}
		}).mouseout(function(){
			if ( !$(this).hasClass('active') ){
				var image_name = $(this).attr('src').split('_on.')[0];
				var image_type = $(this).attr('src').split('_on.')[1];
				$(this).attr('src', image_name + '_off.' + image_type);
			}
		});

		//file management
		$(".operations").click(function(e) {
			var path = $(this).attr('path');
			var command = $(this).attr('command');
			
			//$( "#deleteDialog").html(path);
			$( "#"+command+"Dialog" ).html(path);
			$( "#"+command+"Dialog" ).dialog('open');
			e.cancelBubble=true;
		});

		$( "#hdfswait" ).hide();
	});
}




function viewFile(filepath, offset, len) {
	url = '?cmd1=fs&cmd2=text&params='+filepath;
	gfilepath = filepath;
	if( offset != null ){ 
		url += '&offset='+offset;
		goffset = offset;
	}
	if( len != null ){
		url += '&len='+len;
		glen = len;
	}

	$( "#wait" ).show();
	$( "#dialog" ).dialog( "open" );
	$('#content').html('<textarea id=rawData readonly></textarea>');
	$('#rawData').load(url, function() {
		$( "#wait" ).hide();
	});
}

</script>
<div id="dialog" title="FileView">
	<img id="wait" src=images/wait.gif style="position:relative;left:50%;top:40%;z-index:100">
	<div id="content" style="width=100%;height=100%">
	<textarea id=rawData readonly></textarea>
	</div>
</div>

<div id="deleteDialog" title="Delete file">
	<p><span class="ui-icon ui-icon-alert" style="float:left; margin:0 7px 20px 0;"></span>
These items will be permanently deleted and cannot be recovered. Are you sure?</p>
</div>

<div id="renameDialog" title="Rename file">
<span id="renameBefore"</span>
<input type=text></input>
</div>



<?
$path = getCurrentPath();
echo "Contents of directory " . $hadoop->splitPathAndAddLink($path);
?>
<BR>
<button id="pigStatus" >pigStatus</button>

<div id=hdfs>
</div>
<div id="pigStatusDialog" title="PigStatus" style='display:none'>
</div>

<img id="hdfswait" src=images/wait.gif style="position:relative;left:40%;top:200;z-index:100">
</body>
</html>
