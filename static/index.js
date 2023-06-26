document.getElementById('my-button').addEventListener('click', function() {
    fetch('/get-data/')  // 向后端发送请求获取数据
    .then(response => response.json())  // 解析响应为 JSON
    .then(data => {
      // 在页面中显示数据
      const dataContainer = document.getElementById('data-container');
      dataContainer.innerText = data.message;  // 假设数据中有一个名为 "message" 的字段
    })
    .catch(error => {
      console.error('请求出错:', error);
    });
});