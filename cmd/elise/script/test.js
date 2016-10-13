(function () {
  return (function () {
    var result = {};

    result['product_url_0'] = window.location.href;
    result['product_name_0'] = document.querySelector('head > title').innerText;
    result['product_img_0'] = window.getComputedStyle(document.querySelector('.page1')).backgroundImage.split(/\(|\)/)[1];

    var imgs = document.querySelectorAll('.ts_img > img');
    if (imgs) {
      [].forEach.call(imgs, function(item, indexY) {
        try {
          result['product_img'+indexY+'_0'] = item.src;
        } catch (error) {}
      });
    }

    return result;
})()
