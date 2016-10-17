var result={};
result["final_url"] = window.location.href;
result["title"] = document.title;

[].forEach.call(document.querySelectorAll("img"), function(imgItem) {
  imgItem.setAttribute("prim-width", imgItem.naturalWidth);
  imgItem.setAttribute("prim-height", imgItem.naturalHeight);

  clientRect = imgItem.getBoundingClientRect();
  imgItem.setAttribute("prim-top", clientRect.top);
  imgItem.setAttribute("prim-left", clientRect.left);
});

return result;
