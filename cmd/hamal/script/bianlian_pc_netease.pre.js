var result = {'stop': true};

if(window.location.href.indexOf('qnm.163.com/m') !== -1){
  result['product_url_0'] = window.location.href;

  window.location.href = 'http://qnm.163.com/';
  result['waitTime'] = 3000;
  result['stop'] = false;
}

return result;
