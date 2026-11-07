# Olist 电商经营分析看板

基于 Streamlit 和 Plotly 读取 ADS 层 Parquet，展示 Olist 电商离线数仓结果。

## 前置条件

先生成 DWS 和 ADS：

```powershell
Set-Location "D:\pycharm\PythonProject"

& "D:\PythonProjects\PythonProject\.venv\Scripts\python.exe" `
  ".\build_dws.py"

& "D:\PythonProjects\PythonProject\.venv\Scripts\python.exe" `
  ".\build_ads.py"
```

## 启动

```powershell
Set-Location "D:\pycharm\PythonProject"

& "D:\PythonProjects\PythonProject\.venv\Scripts\python.exe" `
  -m streamlit run ".\dashboard\app.py"
```

浏览器访问：

```text
http://localhost:8501
```

如端口被占用：

```powershell
& "D:\PythonProjects\PythonProject\.venv\Scripts\python.exe" `
  -m streamlit run ".\dashboard\app.py" `
  --server.port 8502
```

## 看板内容

- 经营总览：GMV、有效订单、客单价、取消率、复购率、履约指标。
- 销售分析：月度 GMV、订单量与环比。
- 商品与卖家：品类 Top 15、卖家排行。
- 客户运营：RFM 客户数量和金额贡献。
- 履约质量：各州配送时长、延迟率和履约明细。
- 数据口径：数仓链路、指标定义和质量检查结果。

