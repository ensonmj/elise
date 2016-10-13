var result = {'product_name_0': "倩女幽魂"};

var imgs = document.querySelectorAll('.banner_bg');
if (imgs) {
  [].forEach.call(imgs, function(item, indexY) {
	try {
	  var style= window.getComputedStyle(item);
	  result['product_img'+indexY+'_0'] = style.background.split(/\(|\)/)[3];
	} catch (error) {}
  });
}

return result;
