<?
ini_set('display_errors',1);
ini_set('display_startup_errors',1);
//error_reporting(-1);
error_reporting(E_ALL & ~E_NOTICE);
//$HADOOP_BIN="/usr/bin/hadoop";
$HADOOP_BIN="/home/ubuntu/user/chaehyun/hadoop-1.2.1/bin/hadoop";
$FILE_LEN=32768;
$dir =  getCurrentPath();

$hadoop = new Hadoop($HADOOP_BIN);
if($hadoop->cmd(getVar('cmd1'), getVar('cmd2'), getVar('params'))){
	return;
}

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
	private $HADOOP_BIN="";

    public function Hadoop($bin){
        $this->HADOOP_BIN=$bin;
    }

	public function cmd($cmd1, $cmd2, $params){
		if($cmd1 == null || $cmd2 == null)
			return FALSE;
		$method = $cmd1."_$cmd2";
		$params = explode(" ", $params);

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
		return $this->hadoop_exec("$cmd1 -$cmd2 $params"); 
	}
	private function hadoop_exec($cmd){
		return shell_exec("export HADOOP_CLASSPATH=.;{$this->HADOOP_BIN} $cmd"); 
	}

	public function fs_text($arr){
		$filePath = $arr[0];
		$offset = getVar('offset');
		$len = getVar('len');
		$r = $this->hadoop_exec("HdfsFileReader $filePath $offset $len"); 
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
		$r['permission'] = substr($a[0], 1);
		$r['replication'] = $a[1];
		$r['owner'] = $a[2];
		$r['group'] = $a[3];
		$r['size'] = $a[4];
		$r['modified'] = $a[5] . " " . $a[6];
		return $r;
	}
	public function postVar($key) {
		return urldecode($_POST[$key]);
	}

	public function fs_ls($path) {
		$outputStr = $this->hadoop_cmd("fs", "ls", $path); 
		$output = explode("\n", substr($outputStr, 0, -1));
		$result = array();

		foreach($output as $val) {
			if( strncmp("Found", $val, 5) == 0 ) {
				continue;
			}
			$r = $this->parseLS($val);
			if($r['name'] != '')
				$result[] = $r;
		}
		print json_encode($result);
	}
	public function fs_count($path) {
		$outputStr = $this->hadoop_cmd("fs", "count", $path); 
		$arr = preg_split("/[\s]+/", $outputStr);
		$result = array();
		$result['DIR_COUNT'] = $arr[1];
		$result['FILE_COUNT'] = $arr[2];
		$result['CONTENT_SIZE'] = $arr[3];
		$result['FILE_NAME'] = $arr[4];
		print json_encode($result);
	}
	public function fs_save($path) {
		$txt= $_POST["contents"];
		shell_exec("s='$txt'; echo ".'"$s"'." | {$this->HADOOP_BIN} fs -put - {$path[0]}");
	}

}
?>
<html>
<head>
<link rel="stylesheet" href="jquery-ui.css" type="text/css" media="all" />
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js" type="text/javascript"></script>
<script src="https://ajax.googleapis.com/ajax/libs/jqueryui/1.8.16/jquery-ui.min.js" type="text/javascript"></script>
<style>
html.wait, html.wait * { cursor: wait !important; }

body, input, textarea, select {
	padding: 10px 10px 10px 10px;
    font-family: Helvetica,sans-serif;
    font-size: 12pt;
}
input, textarea{
	padding: 2px 2px 2px 2px;
}
table.grid a, .path_link_file{ 
    color: #1A3448;
    padding: 0 3px;
	text-decoration:underline;
}
table.grid a:hover, table.grid a.hover, .path_link_file:hover {
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
var dir = getValueFromURL('dir');
var gfilepath= "";
var goffset = 0;
var glen = <?=$FILE_LEN?>;
$(function() {
	$( "#dialog" ).dialog({ 
		resizable:true, 
		autoOpen:false,
		width:800,
		height:600,
		buttons: { 
			"rawView": function() { viewFile(gfilepath, goffset, glen);},
			"close": function() { $(this).dialog("close") }
		}
	});

	showCurrentPath(dir);
	loadFileList(dir);

	setDialog("#deleteDialog", openDeleteDialog, "Delete all items", deleteItem);
	setDialog("#renameDialog", openRenameDialog, "OK", renameItem);
	setDialog("#capacityDialog", openCapacityDialog, null, null);
	setDialog("#createFileDialog", openCreateFileDialog, "Save", saveFile);
	setDialog("#pigStatusDialog", openPigStatusDialog, "Refresh", openPigStatusDialog);
	$("#pigStatusDialog").dialog("option", "width", 800);
	$("#pigStatusDialog").dialog("option", "height", 400);
	$("#pigStatusDialog").dialog("option", "resizable", true);

	$("button").button().click( function() {
		$( '#' + $(this).attr('id') + 'Dialog').dialog("open"); 
	});

    $("html").ajaxStart(function () { $(this).addClass("wait"); });
    $("html").ajaxStop(function () { $(this).removeClass("wait"); });
});
function fileSize(a,b,c,d,e){
 return (b=Math,c=b.log,d=1e3,e=c(a)/c(d)|0,a/b.pow(d,e)).toFixed(2)
 +' '+(e?'kMGTPEZY'[--e]+'B':'Bytes')
}

function getValueFromURL( name )
{
	name = name.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");
	var regexS = "[\\?&]"+name+"=([^&#]*)";
	var regex = new RegExp( regexS );
	var results = regex.exec( window.location.href );
	if( results == null )
		return null;
	else
		return results[1];
}

function setDialog(name, openFunc, buttonName, buttonFunction) {
	var buttons = {};
	if(buttonName != null)
		buttons[buttonName] = buttonFunction;
	buttons['Cancel'] = function() {$( this ).dialog( "close" );};

	$( name ).dialog({
		resizable: false,
		width:600,
		height:300,
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


function openCapacityDialog() {
	var fullpath = $(this).html(); 
	$(this).html('<img src=images/wait.gif class=wait_img>waiting..');
	var url = 'index.php';
	$.getJSON(url, {'cmd1':'fs','cmd2':'count', 'params':fullpath},function(json) {
		var r = "directories : " + json.DIR_COUNT;
		r += "<BR>files : " + json.FILE_COUNT;
		r += "<BR>bytes : " + fileSize(json.CONTENT_SIZE);
		$('#capacityDialog').html(r);
		$( ".wait_img" ).hide();
	});
}


function openCreateFileDialog() {
	var dir = getValueFromURL('dir') + '/';
	$('#created_file_dir').text(dir);
	$('#created_file_contents').val('');
	$('#created_file_name').val('');
}

function saveFile(){
	var dialog = $(this);
	var path =  getValueFromURL('dir')+'/'+$('#created_file_name').val();
	var txt = $('#created_file_contents').val();
	$.post('?cmd1=fs&cmd2=save&params='+path, {name:path, contents:txt})
		.done(function(data){
			loadFileList(dir, dialog);
	});
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
	var dialog = $(this);
	$( "#hdfswait" ).show();
	var baseName = $(this).attr('originalBasePath');
	var originalFilePath  = $(this).attr('originalFilePath');
	var targetFileName = $('#targetFileName').val();
	var targetFilePath = baseName + targetFileName;
	var url = '?cmd1=fs&cmd2=mv&params='+originalFilePath + '%20'+ targetFilePath;
	$.get(url, function(data) {
		loadFileList(dir, dialog);
	});
}

function openDeleteDialog() {
	var path = $(this).html(); 
	$( "#deleteDialog").html(path);
}

function deleteItem() {
	var dialog = $(this);
	$( "#hdfswait" ).show();
	var path = $( "#deleteDialog").html();
	$.get('?cmd1=fs&cmd2=rmr&params='+path, function() {
		loadFileList(dir, dialog);
	});
}

function addLink(dir, name) {
	return "<a href='?dir="+dir+"'>"+name+"</a>";
}

function showCurrentPath(dir){
	var r = "Contents of directory : " + addLink("/", "hdfs:///");
	var a = dir.split("/");
	var path = "";
	var delim = "";
	for( var i in a ) {
		var v = a[i];
		if( v == "") {
			continue;
		}
		path += "/" + v;
		r += delim + addLink(path, v);
		delim = "/";
	}
	$('#current_path').html(r);
}

function loadFileList(dir, dialog) {
	$( "#hdfswait" ).show();
	url = '?cmd1=fs&cmd2=ls&params='+dir;
	$.getJSON(url, function(json) {
		var r = "<table class=grid>\n<tr>\
		<th class=type> Type </th>\
		<th class=name width=300> Name </th>\
		<th class=permissions> Operations </th>\
		<th class=permissions> Permissions </th>\
		<th class=replications> Replicates </th>\
		<th class=owner> Owner </th>\
		<th class=group> Group </th>\
		<th class=blockSize> Block Size </th>\
		<th class=modified> Modified </th>\
		</tr>";
	
		for(var i in json){
			var d = json[i];
			r += "<tr>";
			r += "<td>" + d.type + "</td>";
			r += "<td>" + '<p class="path_link_'+d.type+'"'+ ' data-fullpath="'+d.fullpath+'">' + d.name + "</p></td>";
			r += '<td><img path="'+d.fullpath+'" class=operations command=rename src=images/rename_off.png title="rename file"> ';
			r += '<img width=16px path="'+d.fullpath+'" class=operations command=capacity src=images/disksize.png title="check capcity"> ';
			r += '<img path="'+d.fullpath+'" class=operations command=delete src=images/delete_off.png title="rename file"></td>';
			r += "<td>" + d.permission + "</td>";
			r += "<td>" + d.replication + "</td>";
			r += "<td>" + d.owner + "</td>";
			r += "<td>" + d.group + "</td>";
			r += "<td>" + fileSize(d.size) + "</td>";
			r += "<td>" + d.modified + "</td>";
			r += "</tr>";
		}

		r += '</table>';
		$('#hdfs').html(r);

		$( ".path_link_dir").html(function(i, txt){
			href = '?dir='+$(this).attr('data-fullpath');
			return '<a href="' + href + '">'+txt + '</a>';
		});


		$( ".path_link_file").click(function(){
			viewFile($(this).attr('data-fullpath'));
		});


/*
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
		*/

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
		if(dialog != null)
			dialog.dialog("close");
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

<div id="capacityDialog" title="Check Capcity">
</div>

<div id="createFileDialog" title="Create a file">
File Name : <span id=created_file_dir></span><input type=text id=created_file_name></input>
<BR>
Contents
<textarea id=created_file_contents>
</textarea>
</div>

<div id=current_path></div>

<button id="pigStatus" >pigStatus</button>
<button id="createFile" >Create a file</button>
<div id=hdfs></div>
<div id="pigStatusDialog" title="PigStatus" style='display:none'>
</div>

<img id="hdfswait" src=images/wait.gif style="position:relative;left:40%;top:200;z-index:200">
</body>
</html>
