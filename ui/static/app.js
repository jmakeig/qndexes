$(document).ready(function(evt) {
  $("#search").submit(function(evt) {
    getElementElementFrequencies($("#q").val(), function(data) {
      console.dir(data);
    });
    getElementFrequencies($("#q").val(), function(data) {
      renderElementFrequencies(data, $("#element"));
    });
    evt.preventDefault();
  });
});

function renderElementFrequencies(data, el) {
  // Replace me with the call to protovis
  $(el).html('<pre>' + JSON.stringify(data) + '</pre>');
}

function getElementFrequencies(query, handler/* function(data) */) {
  var xhr = new XMLHttpRequest();
  xhr.onreadystatechange = function() {
    if(this.readyState === 1) { /* Loading */ }
    if(this.readyState === 4) {
      if (this.status >= 200 && this.status < 300) {
        console.log("success");
        handler(JSON.parse(this.responseText));
      } else {
        console.error(this.responseText);
      }
    }
  }
  xhr.open("GET", "/element.xqy?q=" + encodeURIComponent(query), true); // + $.serialize({"q": query}), true);
  xhr.setRequestHeader("Accept", "application/json");
  xhr.send();
}


function getElementElementFrequencies(query, handler/* function(data) */) {
  var xhr = new XMLHttpRequest();
  xhr.onreadystatechange = function() {
    if(this.readyState === 1) { /* Loading */ }
    if(this.readyState === 4) {
      if (this.status >= 200 && this.status < 300) {
        console.log("success");
        handler(JSON.parse(this.responseText));
      } else {
        console.error(this.responseText);
      }
    }
  }
  xhr.open("GET", "/element-element.xqy?q=" + encodeURIComponent(query), true); // + $.serialize({"q": query}), true);
  xhr.setRequestHeader("Accept", "application/json");
  xhr.send();
}


