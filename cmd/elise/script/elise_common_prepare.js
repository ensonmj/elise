var result={}

var elementNames = ["div"]
elementNames.forEach(function(tagName){
  var tags = document.getElementsByTagName(tagName);
  var numTags = tags.length;
  for(var i=0; i<numTags; i++) {
    var tag = tags[i];
    var style = window.getComputedStyle(tag);
    if(style.backgroundImage.indexOf("url") !== -1) {
      var bg = style.backgroundImage.slice(4, -1).replace(/\"/g, "");
      var imgNode = document.createElement("img");
      imgNode.setAttribute("src", bg);
      tag.appendChild(imgNode);
      result['waitTime'] = 1000;
    }
  }
});

return result;