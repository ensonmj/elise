var result={};
result["final_url"] = window.location.href;
result["title"] = document.title;

var elementNames = ["div"]
elementNames.forEach(function(tagName){
  var tags = document.getElementsByTagName(tagName);
  var numTags = tags.length;
  for(var i=0; i<numTags; i++) {
    var tag = tags[i];
    var style = window.getComputedStyle(tag);
    if(style.backgroundImage.indexOf("url") !== -1) {
      var bg = style.backgroundImage.slice(4, -1);
      var imgNode = document.createElement("img");
      imgNode.setAttribute("src", bg);
      tag.appendChild(imgNode);
    }
  }
});

[].forEach.call(document.querySelectorAll("img"), function(imgItem) {
  imgItem.setAttribute("prim-width", imgItem.naturalWidth);
  imgItem.setAttribute("prim-height", imgItem.naturalHeight);

  clientRect = imgItem.getBoundingClientRect();
  imgItem.setAttribute("prim-top", clientRect.top);
  imgItem.setAttribute("prim-left", clientRect.left);
});

return result;
