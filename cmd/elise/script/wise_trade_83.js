var result = {};

if(window.location.href.indexOf('itunes.apple.com/cn/app') !== -1){
  result['product_url_0'] = window.location.href;
}

return result;
