var result={};
result["final_url"] = window.location.href;
result["title"] = document.title;

var elementNames = ["body", "div"]
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
  clientRect = imgItem.getBoundingClientRect();
  imgItem.setAttribute("prim-top", clientRect.top);
  imgItem.setAttribute("prim-left", clientRect.left);
  imgItem.setAttribute("prim-width", clientRect.width);
  imgItem.setAttribute("prim-height", clientRect.height);
});

return result;
