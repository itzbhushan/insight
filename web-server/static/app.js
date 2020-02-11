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
	txt = ""
	i = 0
	for (const value of suggestions) {
		console.log(value["title"])
		document.getElementById("score-"+i).innerText = value["score"]
		document.getElementById("title-"+i).innerHTML = "<a href='" + value["link"] + "'>" + value["title"] + "</a>"
		i += 1
		if (i >= 10) { break;}
	}
	while ( i < 10 ) {
		document.getElementById("score-"+i).innerText = ""
		document.getElementById("title-"+i).innerText = ""
		i += 1
	}
});
