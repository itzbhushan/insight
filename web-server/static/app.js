const socket = io('http://ec2-44-232-15-31.us-west-2.compute.amazonaws.com')

socket.on('connect', function() {
console.log("I'm connected")
});

document.body.onkeyup = function(e){
if(e.keyCode == 32){
	var x = document.getElementById("questions");
	console.log(x.value);
	msg = {"text": x.value, "sequence_id": 0, "site": "stackoverflow", "timestamps": [123]}
	socket.emit('get-suggestions', msg);
}
}

socket.on('suggestions-list', function(msg) {
	console.log("Suggestions list event", msg)
	suggestions = msg["suggestions"]
	for (const [key, value] of Object.entries(suggestions)) {
		title = value["title"]
		console.log(title)
		$('#suggestions').val($('#suggestions').val() + title +'\n');
	}
});
