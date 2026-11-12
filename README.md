# 告警会话窗的晚到合并与撤回拆分

本项目保存湖仓治理的合成资料与原始配置。`lake_data.io`读取JSON清单和资产身份，`lake_data.domain`检查基本字段并给数量概况，`lake_data.cli`输出原资料摘要。字段、数据类型、时间和版本口径见[资料约定](docs/data-contract.md)。数据中的设备、班组、主体和电量均为虚构，不连接电网、生产数仓或人员系统。

Python 3.11及以上，无第三方Python依赖。在项目根目录运行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q lake_data tests
python -m lake_data.cli inspect data/normal.json
```

`inspect`只显示原字段概况，不修改源资料。`invalid-config.json`是基本配置错误，其余JSON为有效清单但可含需要领域操作判断的边界资料。null与缺字段应按各字段约定区分，原文件摘要绑定实际字节。

Dockerfile固定Linux基础镜像。`docker build -t lake-material .`构建，`docker run --rm lake-material`读取正常清单；`docker run --rm lake-material python -m unittest discover -s tests -v`执行已有检查。所有资料本地可读，无需账号和在线来源。
